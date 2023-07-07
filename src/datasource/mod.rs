use async_recursion::async_recursion;
use core::fmt;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;

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
        parse::merge_json,
        shutdown::Shutdown,
        traits::{Name, TaskDetailStatic},
    },
    model::{
        column::{parse_json_from_column, DataSourceColumn},
        schema::{ChannelSchema, DataSourceSchema},
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

/// 后续图形化展示数据结构
pub struct DataSourceTreeNode {
    pub name: String,
}

pub struct DataSourceManager {
    pub final_status: HashMap<String, DataSourceChannelStatus>,
    pub will_status: HashMap<String, DataSourceChannelStatus>,
    pub schemas: HashMap<String, DataSourceSchema>,
    pub columns: HashMap<String, Vec<DataSourceColumn>>,
    pub data_statistics_map: HashMap<String, DataSourceStatistics>,
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
            final_status: HashMap::new(),
            will_status: HashMap::new(),
            schemas: HashMap::new(),
            data_statistics_map: HashMap::new(),
            columns: HashMap::new(),
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

    pub fn count(&self, name: &str) -> Option<usize> {
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

    pub fn columns(&self, name: &str) -> Option<&Vec<DataSourceColumn>> {
        self.columns.get(name)
    }

    pub fn put_schema(&mut self, name: &str, schema: DataSourceSchema) -> Option<DataSourceSchema> {
        return self.schemas.insert(name.to_owned(), schema);
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
        name: &str,
        status: DataSourceChannelStatus,
    ) -> Option<DataSourceChannelStatus> {
        self.will_status.insert(name.to_owned(), status)
    }

    pub async fn update_datasource_columns(&mut self, name: &str, columns: Vec<DataSourceColumn>) {
        self.columns.insert(name.to_owned(), columns);
    }

    #[async_recursion]
    pub async fn notify_update_sources(&mut self, name: &str) {
        if let Some(schema) = self.get_schema(name).cloned() {
            if let Some(sources) = schema.sources.as_ref() {
                let columns =
                    DataSourceColumn::get_columns_from_value(&schema.columns.unwrap_or(json!(0)));
                let meta = &schema.meta.unwrap_or(json!({}));
                let channel = &schema.channel.unwrap_or(ChannelSchema::default());

                for source in sources {
                    let columns = columns.clone();
                    if let Some(mut source_schema) = self.get_schema(source).cloned() {
                        let mut next_columns = Vec::new();
                        if let Some(schema_columns) = &source_schema.columns {
                            let schema_columns =
                                DataSourceColumn::get_columns_from_value(schema_columns);
                            // 合并两个数组
                            next_columns =
                                DataSourceColumn::merge_columns(&columns, &schema_columns);
                        } else {
                            next_columns = columns.clone();
                        }
                        let mut next_meta = json!({});

                        if let Some(schema_meta) = &mut source_schema.meta {
                            merge_json(meta, schema_meta);
                            next_meta = schema_meta.clone();
                        } else {
                            next_meta = meta.clone()
                        }
                        let mut result_channel = ChannelSchema::default();

                        let source_channel = source_schema.channel;
                        if let Some(source_channel) = source_channel {
                            if source_channel.batch.is_none() {
                                result_channel.batch = channel.batch.clone();
                            } else {
                                result_channel.batch = source_channel.batch.clone();
                            }
                            if source_channel.count.is_none() {
                                result_channel.count = channel.count.clone();
                            } else {
                                result_channel.count = source_channel.count.clone();
                            }
                            if source_channel.concurrency.is_none() {
                                result_channel.concurrency = channel.concurrency.clone();
                            } else {
                                result_channel.concurrency = source_channel.concurrency.clone();
                            }
                        } else {
                            result_channel = channel.clone();
                        }

                        if let Some(schema) = DATA_SOURCE_MANAGER
                            .write()
                            .await
                            .get_schema_mut(&source_schema.name)
                        {
                            // 更新columns
                            schema.columns = Some(parse_json_from_column(&next_columns));
                            schema.meta = Some(next_meta);
                            schema.channel = Some(result_channel);
                            self.notify_update_sources(&schema.name).await;
                        }

                        self.columns.insert(source_schema.name.to_owned(), columns);
                    }
                }
            }
        }
    }

    ///更新will数据源状态,返回之前的状态, 若存在最终状态则不更新
    pub fn update_final_status(
        &mut self,
        name: &str,
        status: DataSourceChannelStatus,
        overide: bool,
    ) -> Option<DataSourceChannelStatus> {
        match self.final_status.get(name) {
            Some(old_status) => {
                if overide {
                    return self.final_status.insert(name.to_owned(), status);
                } else {
                    match old_status {
                        DataSourceChannelStatus::Starting => {
                            return self.final_status.insert(name.to_owned(), status)
                        }
                        DataSourceChannelStatus::Running => {
                            return self.final_status.insert(name.to_owned(), status)
                        }
                        DataSourceChannelStatus::Stopped(_) => return None,
                        DataSourceChannelStatus::Terminated(_) => return None,
                        DataSourceChannelStatus::Inited => {
                            return self.final_status.insert(name.to_owned(), status)
                        }
                        DataSourceChannelStatus::Ended => return None,
                    }
                }
            }
            None => return self.final_status.insert(name.to_owned(), status),
        }
    }

    pub fn is_shutdown(&self, name: &str) -> bool {
        match self.will_status.get(name).map(Self::is_shutdown_self) {
            Some(shut) => return shut,
            None => return false,
        }
    }

    pub fn get_final_status(&self, name: &str) -> Option<&DataSourceChannelStatus> {
        self.final_status.get(name)
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
        for val in self.will_status.values_mut() {
            *val = DataSourceChannelStatus::Stopped("手动关闭任务".to_owned());
        }
    }

    /// 等待所有任务完成
    pub async fn await_all_done(&self) {
        loop {
            let mut count = self.final_status.iter().count();

            for val in self.final_status.values() {
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
            tokio::time::sleep(Duration::from_millis(100)).await
        }
    }

    /// 等待所有任务完成
    pub async fn await_all_init(&self, name: &str) {
        loop {
            if let Some(schema) = self.get_schema(name) {
                if let Some(sources) = schema.sources.as_ref() {
                    let mut count = sources.len();
                    for source in sources {
                        if let Some(DataSourceChannelStatus::Inited) = self.get_final_status(source)
                        {
                            count -= 1;
                        }
                    }
                    if count == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
            break;
        }
    }
}

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

impl DelegatedDataSource {
    pub fn new(datasources: Vec<Box<dyn DataSourceChannel>>) -> Self {
        Self {
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
            register(&self.name().clone()).await;
            //更新状态
            DATA_SOURCE_MANAGER.write().await.update_final_status(
                self.name(),
                DataSourceChannelStatus::Inited,
                false,
            );
        }
    }

    async fn before_run(
        &mut self,
        _context: Arc<RwLock<DataSourceContext>>,
        _channel: ChannelContext,
    ) -> crate::Result<()> {
        if self.need_log() {
            DATA_SOURCE_MANAGER.write().await.update_final_status(
                self.name(),
                DataSourceChannelStatus::Starting,
                false,
            );
        }

        Ok(())
    }

    async fn after_run(
        &mut self,
        _context: Arc<RwLock<DataSourceContext>>,
        _channel: ChannelContext,
    ) -> crate::Result<()> {
        if self.need_log() {
            //更新状态
            DATA_SOURCE_MANAGER.write().await.update_final_status(
                self.name(),
                DataSourceChannelStatus::Ended,
                false,
            );
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
                        DATA_SOURCE_MANAGER.write().await.update_final_status(
                            self.name(),
                            DataSourceChannelStatus::Terminated(format!("程序异常终止:{}", e)),
                            false,
                        );
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                DATA_SOURCE_MANAGER.write().await.update_final_status(
                    self.name(),
                    DataSourceChannelStatus::Terminated(format!("程序异常终止:{}", e)),
                    false,
                );
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

    async fn get_columns_define(&mut self) -> Option<Vec<DataSourceColumn>> {
        return None;
    }

    async fn get_task(
        &mut self,
        _channel: ChannelContext,
        _shutdown_complete_tx: mpsc::Sender<()>,
        _shutdown: Shutdown,
        _count_rc: Option<Arc<AtomicI64>>,
        _limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> Option<Box<dyn Task>> {
        return None;
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
        DATA_SOURCE_MANAGER.write().await.update_final_status(
            self.name(),
            DataSourceChannelStatus::Running,
            false,
        );
        // 更新配置
        let schema = DATA_SOURCE_MANAGER
            .read()
            .await
            .get_schema(self.name())
            .cloned();
        if let Some(schema) = schema {
            // 获取字段定义
            let columns = self.get_columns_define().await.unwrap_or(vec![]);
            if let Some(schema_columns) = &schema.columns {
                let schema_columns = DataSourceColumn::get_columns_from_value(schema_columns);
                // 合并两个数组
                let columns = DataSourceColumn::merge_columns(&columns, &schema_columns);

                match DATA_SOURCE_MANAGER
                    .write()
                    .await
                    .get_schema_mut(self.name())
                {
                    Some(schema) => {
                        // 更新columns
                        let columns = parse_json_from_column(&columns);
                        schema.columns = Some(columns.clone());
                        DATA_SOURCE_MANAGER
                            .write()
                            .await
                            .notify_update_sources(self.name())
                            .await;
                    }
                    None => {
                        return Err(Error::Io(IoError::ParseSchemaError));
                    }
                }
            }
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

        let count_rc = DATA_SOURCE_MANAGER
            .read()
            .await
            .count(self.name())
            .map(|count| Arc::new(AtomicI64::new(count as i64)));

        let concurrency = Arc::new(Semaphore::new(
            DATA_SOURCE_MANAGER
                .read()
                .await
                .concurrency(self.name())
                .unwrap_or(1),
        ));
        while !DATA_SOURCE_MANAGER.read().await.is_shutdown(self.name()) {
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
                .await;

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
