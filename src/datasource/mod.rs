use async_trait::async_trait;
use core::fmt;
use sqlx::{Pool, Sqlite};
use std::{
    collections::HashMap,
    fmt::Display,
    str::FromStr,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{
    broadcast,
    mpsc::{self, Receiver, Sender},
    Mutex, RwLock, Semaphore,
};

use crate::{
    core::{
        cli::Cli,
        error::{Error, IoError},
        fake::get_random_uuid,
        limit::token::TokenBuketLimiter,
        log::register,
        parse::merge_columns_to_session,
        shutdown::Shutdown,
        traits::{Name, TaskDetailStatic},
    },
    model::{
        column::{parse_json_from_column, DataSourceColumn},
        schema::DataSourceSchema,
    },
    task::Task,
    Json,
};

use self::{csv::CsvArgs, kafka::KafkaArgs, mysql::MysqlArgs};

pub mod clickhouse;
pub mod csv;
pub mod fake;
pub mod kafka;
pub mod mysql;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataSourceEnum {
    // ClickHouse,
    Mysql,
    //
    Kafka,
    //
    //    ElasticSearch,
    //
    Csv,
    Fake, //
          //    SqlServer,
}

impl Display for DataSourceEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataSourceEnum::Mysql => write!(f, "Mysql"),
            DataSourceEnum::Kafka => write!(f, "Kafka"),
            DataSourceEnum::Csv => write!(f, "Csv"),
            DataSourceEnum::Fake => write!(f, "Fake"),
        }
    }
}

impl DataSourceEnum {
    pub fn parse_meta_from_cli(&self, cli: Cli) -> crate::Result<Json> {
        match self {
            DataSourceEnum::Mysql => {
                Ok(serde_json::to_value(TryInto::<MysqlArgs>::try_into(cli)?)
                    .expect("json解析失败"))
            }
            DataSourceEnum::Kafka => {
                Ok(serde_json::to_value(TryInto::<KafkaArgs>::try_into(cli)?)
                    .expect("json解析失败"))
            }
            DataSourceEnum::Csv => {
                Ok(serde_json::to_value(TryInto::<CsvArgs>::try_into(cli)?).expect("json解析失败"))
            }
            _ => {
                return Err(Error::Io(IoError::UnkownSourceError("unkown".to_owned())));
            }
        }
    }
}

impl FromStr for DataSourceEnum {
    type Err = Box<dyn std::error::Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();

        let s = s.as_str();

        match s {
            // "clickhouse" => Ok(SourceEnum::ClickHouse),
            "mysql" => Ok(DataSourceEnum::Mysql),
            "kafka" => Ok(DataSourceEnum::Kafka),
            //            "elasticsearch" => Ok(SourceEnum::ElasticSearch),
            "csv" => Ok(DataSourceEnum::Csv),
            "fake" => Ok(DataSourceEnum::Fake),
            //            "sqlserver" => Ok(SourceEnum::SqlServer),
            _ => Err("不支持该输出源".into()),
        }
    }
}

#[derive(Clone)]
pub struct ChannelContext {
    pub sender: Option<Sender<serde_json::Value>>,

    pub receiver: Option<Arc<Mutex<Receiver<serde_json::Value>>>>,
}

impl ChannelContext {
    pub fn new(
        sender: Option<Sender<serde_json::Value>>,
        receiver: Option<Receiver<serde_json::Value>>,
    ) -> Self {
        Self {
            sender,
            receiver: receiver.map(|receiver| Arc::new(Mutex::new(receiver))),
        }
    }
}

#[derive(Clone)]
pub struct DataSourceContext {
    pub limit: Option<usize>,
    pub skip: bool,
    pub id: String,
}

impl DataSourceContext {
    pub fn new(limit: Option<usize>, skip: bool) -> Self {
        Self {
            limit,
            skip,
            id: get_random_uuid(),
        }
    }
}

#[derive(Debug)]
pub struct DelegatedDataSource {
    id: String,
    datasources: Vec<Box<dyn DataSourceChannel>>,
    name: String,
}

#[derive(Debug)]
pub struct MpscDataSourceChannel {
    pub producer: Option<Vec<Box<dyn DataSourceChannel>>>,

    pub consumer: Option<Box<dyn DataSourceChannel>>,
}

impl MpscDataSourceChannel {
    pub fn new(
        producer: Vec<Box<dyn DataSourceChannel>>,
        consumer: Box<dyn DataSourceChannel>,
    ) -> Self {
        Self {
            producer: Some(producer),
            consumer: Some(consumer),
        }
    }
}

lazy_static! {
    pub static ref DATA_SOURCE_MANAGER: Arc<RwLock<DataSourceManager>> =
        Arc::new(RwLock::new(DataSourceManager::new()));
}

pub struct DataSourceStatistics {
    pub consume_count: AtomicU64,
    pub produce_count: AtomicU64,
    pub start_time: SystemTime,
}

/// 用于在一个传输会话中共享的数据结构
/// 目前共享的有meta, columns
/// sessionId 检索session, 同一个namespace下的channel sessionId相同
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataSourceTransferSession {
    /// 会话id
    pub id: String,
    /// 会话元数据
    pub meta: Option<Json>,
    /// 会话字段定义
    pub columns: Option<(Json, Vec<DataSourceColumn>)>,

    /// 最终状态 name -> status
    pub final_status: HashMap<String, DataSourceChannelStatus>,

    /// 将要变更的状态 name -> status
    pub will_status: HashMap<String, DataSourceChannelStatus>,

    /// 消费者消费的数据源 name -> vec<name>
    pub consumer_sources: HashMap<String, Vec<String>>,
}

impl DataSourceTransferSession {
    pub fn new(
        id: String,
        meta: Option<Json>,
        columns: Option<(Json, Vec<DataSourceColumn>)>,
    ) -> Self {
        Self {
            id,
            meta,
            columns,
            final_status: HashMap::new(),
            will_status: HashMap::new(),
            consumer_sources: HashMap::new(),
        }
    }
}

/// 后续图形化展示数据结构
pub struct DataSourceTreeNode {
    pub name: String,
}

pub struct DataSourceManager {
    pub pool: Option<Pool<Sqlite>>,
    pub schemas: HashMap<String, DataSourceSchema>,
    pub data_statistics_map: HashMap<String, DataSourceStatistics>,
    pub sessions: HashMap<String, DataSourceTransferSession>,
}

impl DataSourceStatistics {
    pub fn new(consume_count: usize, produce_count: usize) -> Self {
        Self {
            consume_count: AtomicU64::new(consume_count as u64),
            produce_count: AtomicU64::new(produce_count as u64),
            start_time: SystemTime::now(),
        }
    }
}

impl DataSourceManager {
    pub fn new() -> Self {
        Self {
            pool: None,
            schemas: HashMap::new(),
            data_statistics_map: HashMap::new(),
            sessions: HashMap::new(),
        }
    }

    pub fn pool(&self) -> Option<&Pool<Sqlite>> {
        return self.pool.as_ref();
    }

    pub fn put_session_source(&mut self, id: &str, name: String, sources: Vec<String>) {
        if let Some(session) = self.sessions.get_mut(id) {
            session.consumer_sources.insert(name, sources);
        }
    }

    pub fn get_all_schema(&self) -> Vec<DataSourceSchema> {
        self.schemas.values().map(|val| val.clone()).collect()
    }
    /// special: true 消费 false 生产
    pub fn increase_by(&mut self, name: &str, count: usize, special: bool) {
        if let Some(data_statistics) = self.data_statistics_map.get(name) {
            if special {
                data_statistics
                    .consume_count
                    .fetch_add(count as u64, Ordering::SeqCst);
            } else {
                data_statistics
                    .produce_count
                    .fetch_add(count as u64, Ordering::SeqCst);
            }
        } else {
            if special {
                self.data_statistics_map
                    .insert(name.to_owned(), DataSourceStatistics::new(count, 0));
            } else {
                self.data_statistics_map
                    .insert(name.to_owned(), DataSourceStatistics::new(0, count));
            }
        }
    }

    pub fn statistics(&self, name: &str) -> Option<&DataSourceStatistics> {
        return self.data_statistics_map.get(name);
    }
    pub fn concurrency(&self, name: &str) -> Option<usize> {
        return match self.schemas.get(name) {
            Some(schema) => {
                if let Some(channel) = &schema.channel {
                    return channel.concurrency;
                }
                None
            }
            None => None,
        };
    }

    pub fn count(&self, name: &str) -> Option<isize> {
        return match self.schemas.get(name) {
            Some(schema) => {
                if let Some(channel) = &schema.channel {
                    return channel.count;
                }
                None
            }
            None => None,
        };
    }

    pub fn batch(&self, name: &str) -> Option<usize> {
        return match self.schemas.get(name) {
            Some(schema) => {
                if let Some(channel) = &schema.channel {
                    return channel.batch;
                }
                None
            }
            None => None,
        };
    }

    pub fn columns_json(&self, session_id: &str) -> Option<&Json> {
        self.sessions
            .get(session_id)
            .map(|session| session.columns.as_ref().map(|columns| &columns.0).unwrap())
    }

    pub fn columns(&self, session_id: &str) -> Option<&Vec<DataSourceColumn>> {
        self.sessions
            .get(session_id)
            .map(|session| session.columns.as_ref().map(|columns| &columns.1).unwrap())
    }

    pub fn put_schema(&mut self, name: &str, schema: DataSourceSchema) -> Option<DataSourceSchema> {
        return self.schemas.insert(name.to_owned(), schema);
    }

    pub fn contains_schema(&self, name: &str) -> bool {
        return self.schemas.contains_key(name);
    }

    pub fn get_schema(&self, name: &str) -> Option<&DataSourceSchema> {
        return self.schemas.get(name);
    }

    pub fn get_schema_mut(&mut self, name: &str) -> Option<&mut DataSourceSchema> {
        return self.schemas.get_mut(name);
    }

    ///更新will数据源状态,返回之前的状态
    pub async fn update_will_status(
        &mut self,
        session_id: &str,
        name: &str,
        status: DataSourceChannelStatus,
    ) {
        let _ = self.sessions.get_mut(session_id).is_some_and(|session| {
            session.will_status.insert(name.to_owned(), status);
            return true;
        });
    }

    ///更新will数据源状态,返回之前的状态, 若存在最终状态则不更新
    pub fn update_final_status(
        &mut self,
        session_id: &str,
        name: &str,
        status: DataSourceChannelStatus,
        overide: bool,
    ) -> Option<DataSourceChannelStatus> {
        match self.sessions.get_mut(session_id) {
            Some(session) => {
                let old_status = session.final_status.get(name);
                match old_status {
                    Some(old_status) => {
                        if overide {
                            return session.final_status.insert(name.to_owned(), status);
                        } else {
                            match old_status {
                                DataSourceChannelStatus::Starting => {
                                    return session.final_status.insert(name.to_owned(), status)
                                }
                                DataSourceChannelStatus::Running => {
                                    return session.final_status.insert(name.to_owned(), status)
                                }
                                DataSourceChannelStatus::Stopped(_) => return None,
                                DataSourceChannelStatus::Terminated(_) => return None,
                                DataSourceChannelStatus::Inited => {
                                    return session.final_status.insert(name.to_owned(), status)
                                }
                                DataSourceChannelStatus::Ended => return None,
                            }
                        }
                    }
                    None => return session.final_status.insert(name.to_owned(), status),
                }
            }
            None => None,
        }
    }

    pub fn is_shutdown(&self, session_id: &str, name: &str) -> bool {
        match self.sessions.get(session_id) {
            Some(session) => match session.will_status.get(name) {
                Some(status) => {
                    return Self::is_shutdown_self(status);
                }
                None => return false,
            },
            None => false,
        }
    }

    pub fn get_final_status(
        &self,
        session_id: &str,
        name: &str,
    ) -> Option<&DataSourceChannelStatus> {
        match self.sessions.get(session_id) {
            Some(session) => return session.final_status.get(name),
            None => None,
        }
    }

    pub fn is_shutdown_self(source: &DataSourceChannelStatus) -> bool {
        match source {
            DataSourceChannelStatus::Stopped(_) => true,
            DataSourceChannelStatus::Ended => true,
            DataSourceChannelStatus::Terminated(_) => true,
            _ => false,
        }
    }

    pub async fn stop_all_task(&mut self) {
        for session in self.sessions.values_mut() {
            for val in session.will_status.values_mut() {
                *val = DataSourceChannelStatus::Stopped("手动关闭任务".to_owned());
            }
        }
    }

    pub async fn stop_all_task_by_session(&mut self, session_id: &str) {
        match self.sessions.get_mut(session_id) {
            Some(session) => {
                for val in session.will_status.values_mut() {
                    *val = DataSourceChannelStatus::Stopped("手动关闭任务".to_owned());
                }
            }
            None => {
                // nothing
            }
        }
    }

    /// 阻塞所有操作
    /// 等待所有任务完成
    pub async fn await_all_done(&self) {
        loop {
            let mut session_num = self.sessions.len();
            for session in self.sessions.values() {
                let mut count = session.final_status.iter().count();

                for val in session.final_status.values() {
                    match val {
                        DataSourceChannelStatus::Running => continue,
                        DataSourceChannelStatus::Stopped(_) => count -= 1,
                        DataSourceChannelStatus::Terminated(_) => count -= 1,
                        DataSourceChannelStatus::Inited => continue,
                        DataSourceChannelStatus::Ended => count -= 1,
                        DataSourceChannelStatus::Starting => continue,
                    }
                }

                if count == 0 {
                    session_num -= 1;
                    continue;
                }
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            if session_num == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await
        }
    }

    pub async fn await_all_done_by_session(&self, session_id: &str) {
        loop {
            match self.sessions.get(session_id) {
                Some(session) => {
                    let mut count = session.final_status.iter().count();

                    for val in session.final_status.values() {
                        match val {
                            DataSourceChannelStatus::Running => continue,
                            DataSourceChannelStatus::Stopped(_) => count -= 1,
                            DataSourceChannelStatus::Terminated(_) => count -= 1,
                            DataSourceChannelStatus::Inited => continue,
                            DataSourceChannelStatus::Ended => count -= 1,
                            DataSourceChannelStatus::Starting => continue,
                        }
                    }

                    if count == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await
                }
                None => {
                    break;
                }
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataSourceChannelStatus {
    Starting,
    Running,
    Stopped(String),
    Terminated(String),
    Inited,
    Ended,
}

impl Name for MpscDataSourceChannel {
    fn name(&self) -> &str {
        "mspc"
    }

    fn id(&self) -> &str {
        "mspc"
    }
}

impl TaskDetailStatic for MpscDataSourceChannel {}

#[async_trait]
impl DataSourceChannel for MpscDataSourceChannel {
    fn need_log(&self) -> bool {
        return false;
    }

    async fn run(
        &mut self,
        context: Arc<RwLock<DataSourceContext>>,
        _chanel: ChannelContext,
    ) -> crate::Result<()> {
        let (tx, rx) = mpsc::channel(10000);
        let producers = self.producer.take();
        if let Some(producers) = producers {
            for mut producer in producers {
                let context = context.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    producer.send(context, tx).await.unwrap();
                });
            }
        }
        if let Some(mut consumer) = self.consumer.take() {
            tokio::spawn(async move {
                consumer.recv(context, rx).await.unwrap();
            });
        }

        Ok(())
    }
}

impl Name for DelegatedDataSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> &str {
        return &self.id;
    }
}

impl TaskDetailStatic for DelegatedDataSource {}

#[async_trait]
impl DataSourceChannel for DelegatedDataSource {
    fn need_log(&self) -> bool {
        return false;
    }

    async fn run(
        &mut self,
        context: Arc<RwLock<DataSourceContext>>,
        _channel: ChannelContext,
    ) -> crate::Result<()> {
        let datasources = &mut self.datasources;
        let channel = ChannelContext::new(None, None);
        for datasource in datasources {
            let context = context.clone();
            let channel = channel.clone();
            datasource.execute(context, channel).await?;
        }
        Ok(())
    }
}
use uuid::Uuid;
impl DelegatedDataSource {
    pub fn new(datasources: Vec<Box<dyn DataSourceChannel>>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            datasources,
            name: "delegate".to_string(),
        }
    }

    /// 注册日志并执行任务
    pub async fn start_output(
        &mut self,
        output: &mut Box<dyn DataSourceChannel>,
        context: Arc<RwLock<DataSourceContext>>,
    ) -> crate::Result<()> {
        output.run(context, ChannelContext::new(None, None)).await
    }
}

#[async_trait]
pub trait DataSourceChannel: Send + Sync + fmt::Debug + TaskDetailStatic + Name {
    fn need_log(&self) -> bool {
        return true;
    }

    /// 通用初始化逻辑
    async fn init(&mut self, _context: Arc<RwLock<DataSourceContext>>, _channel: ChannelContext) {
        //注册日志
        if self.need_log() {
            register(&self.id().clone()).await;
            //更新状态
            self.update_final_status(DataSourceChannelStatus::Inited, false)
                .await;
        }
    }

    async fn before_run(
        &mut self,
        _context: Arc<RwLock<DataSourceContext>>,
        _channel: ChannelContext,
    ) -> crate::Result<()> {
        if self.need_log() {
            {
                self.update_final_status(DataSourceChannelStatus::Starting, false)
                    .await;
            }

            // 更新配置
            let schema = self.schema().await;

            if let Some(schema) = schema {
                // 获取字段定义
                let columns = self.get_columns_define().await?;
                if schema.columns.is_some() || columns.len() > 0 {
                    let schema_columns = DataSourceColumn::get_columns_from_value(
                        &schema.columns.unwrap_or(json!({})),
                    );
                    // 合并两个数组
                    let columns = DataSourceColumn::merge_columns(&columns, &schema_columns);
                    if columns.len() > 0 {
                        let mut data_manager = DATA_SOURCE_MANAGER.write().await;
                        data_manager
                            .sessions
                            .get_mut(self.id())
                            .expect("会话已失效")
                            .columns = Some((json!({}), columns.clone()));

                        match data_manager.get_schema_mut(self.name()) {
                            Some(schema) => {
                                // 更新columns
                                let columns = parse_json_from_column(&columns);
                                schema.columns = Some(columns.clone());
                            }
                            None => {
                                return Err(Error::Io(IoError::ParseSchemaError));
                            }
                        }

                        drop(data_manager);
                        merge_columns_to_session(self.id(), &columns).await?;
                    }
                }
            } else {
                return Err(Error::Io(IoError::SchemaNotFound));
            }
        }

        Ok(())
    }

    async fn after_run(
        &mut self,
        _context: Arc<RwLock<DataSourceContext>>,
        _channel: ChannelContext,
    ) -> crate::Result<()> {
        if self.need_log() {
            //更新自己的状态
            self.update_final_status(DataSourceChannelStatus::Ended, false)
                .await;

            //拿到对应的source
            if let Some(flag) = self.is_producer().await {
                if !flag {
                    let session = DATA_SOURCE_MANAGER
                        .read()
                        .await
                        .sessions
                        .get(self.id())
                        .cloned();

                    if let Some(session) = session {
                        if let Some(sources) = session.consumer_sources.get(self.name()) {
                            for source in sources {
                                DATA_SOURCE_MANAGER
                                    .write()
                                    .await
                                    .update_will_status(
                                        self.id(),
                                        source,
                                        DataSourceChannelStatus::Ended,
                                    )
                                    .await;
                            }
                        }
                    }
                }
            }
            println!("{} Ended", self.name());
        }
        Ok(())
    }

    async fn execute(
        &mut self,
        context: Arc<RwLock<DataSourceContext>>,
        channel: ChannelContext,
    ) -> crate::Result<()> {
        let context_rc = context.clone();
        self.init(context_rc, channel.clone()).await;
        let context_rc = context.clone();
        match self.before_run(context_rc, channel.clone()).await {
            Ok(()) => {
                let context_rc = context.clone();

                match self.run(context_rc, channel.clone()).await {
                    Ok(_) => return self.after_run(context, channel.clone()).await,
                    Err(e) => {
                        self.update_final_status(
                            DataSourceChannelStatus::Terminated(format!("程序异常终止:{}", e)),
                            false,
                        )
                        .await;
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                self.update_final_status(
                    DataSourceChannelStatus::Terminated(format!("程序异常终止:{}", e)),
                    false,
                )
                .await;
                return Err(e);
            }
        };
    }

    async fn send(
        &mut self,
        context: Arc<RwLock<DataSourceContext>>,
        sender: Sender<serde_json::Value>,
    ) -> crate::Result<()> {
        let channel_context = ChannelContext::new(Some(sender), None);

        self.execute(context, channel_context).await
    }

    async fn recv(
        &mut self,
        context: Arc<RwLock<DataSourceContext>>,
        receiver: Receiver<serde_json::Value>,
    ) -> crate::Result<()> {
        let channel_context = ChannelContext::new(None, Some(receiver));

        self.execute(context, channel_context).await
    }

    async fn get_columns_define(&mut self) -> crate::Result<Vec<DataSourceColumn>> {
        return Ok(vec![]);
    }

    async fn get_task(
        &mut self,
        _channel: ChannelContext,
        _shutdown_complete_tx: mpsc::Sender<()>,
        _shutdown: Shutdown,
        _count_rc: Option<Arc<AtomicI64>>,
        _limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> crate::Result<Option<Box<dyn Task>>> {
        return Ok(None);
    }

    ///执行流程
    ///1. 获取字段定义
    ///2. 生成输出任务
    ///3. 等待任务执行完毕
    async fn run(
        &mut self,
        context: Arc<RwLock<DataSourceContext>>,
        channel: ChannelContext,
    ) -> crate::Result<()> {
        {
            self.update_final_status(DataSourceChannelStatus::Running, false)
                .await;
        }

        if context.read().await.skip {
            return Ok(());
        }
        let (notify_shutdown, _) = broadcast::channel::<()>(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(1);
        println!("{} will running...", self.name());
        let limiter = context
            .read()
            .await
            .limit
            .map(|limit| Arc::new(Mutex::new(TokenBuketLimiter::new(limit, limit * 2))));

        let count_rc = self
            .count_inner()
            .await
            .map(|count| Arc::new(AtomicI64::new(count as i64)));

        let concurrency = Arc::new(Semaphore::new(self.concurrency().await));
        while !self.is_shutdown().await {
            // 检查数量
            if let Some(count) = count_rc.as_ref() {
                if count.load(Ordering::SeqCst) <= 0 {
                    break;
                }
            }

            let permit = concurrency.clone().acquire_owned().await.unwrap();

            let shutdown = Shutdown::new(notify_shutdown.subscribe());

            let count_rc = count_rc.clone();
            let limiter = limiter.clone();
            let channel = channel.clone();
            let task = self
                .get_task(
                    channel,
                    shutdown_complete_tx.clone(),
                    shutdown,
                    count_rc,
                    limiter,
                )
                .await?;

            match task {
                Some(mut task) => {
                    tokio::spawn(async move {
                        if let Err(err) = task.run().await {
                            println!("task run error: {}", err);
                        }
                        drop(permit);
                    });
                }
                None => {
                    //nothing
                }
            }
        }

        drop(notify_shutdown);
        drop(shutdown_complete_tx);
        // 等待所有的task执行完毕
        let _ = shutdown_complete_rx.recv().await;

        Ok(())
    }
}
