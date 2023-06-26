use core::fmt;
use std::{sync::{Arc, atomic::{AtomicI64, Ordering}}, str::FromStr};

use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use serde_json::json;
use tokio::sync::{mpsc::{self, Sender, Receiver}, Mutex, broadcast, Semaphore};

use crate::{core::{log::register, shutdown::Shutdown, limit::token::TokenBuketLimiter, fake::get_random_uuid}, model::{schema::{ChannelSchema, Schema, DataSourceSchema}, column::DataSourceColumn}, task::Task};

pub mod clickhouse;
pub mod csv;
pub mod kafka;
pub mod mysql;
pub mod fake;

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
    Fake
    //
    //    SqlServer,
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
    pub fn new(sender: Option<Sender<serde_json::Value>>, receiver:Option<Receiver<serde_json::Value>>) -> Self {
        Self { sender, receiver: receiver.map(|receiver| Arc::new(Mutex::new(receiver))) }
    }
}

pub struct DataSourceContext {
    pub concurrency: usize,
    pub limit: Option<usize>,
    pub skip: bool,
    pub id: String,
    pub schema: Schema,
}


impl DataSourceContext {
    pub fn new(concurrency: usize, limit: Option<usize>, skip: bool, schema: Schema) -> Self {
        Self {
            concurrency,
            limit,
            skip,
            schema,
            id: get_random_uuid(),
        }
    }
}

#[async_trait]
pub trait Close {
    async fn close(&mut self) -> crate::Result<()>;
}

#[derive(Debug)]
pub struct DelegatedDataSource {
    datasources: Vec<Box<dyn DataSourceChannel>>,
    name: String,
}

#[derive(Debug)]
pub struct MpscDataSourceChannel {
    pub producer: Vec<Box<dyn DataSourceChannel>>,
    
    pub consumer: Box<dyn DataSourceChannel>
}

impl MpscDataSourceChannel {
    pub fn new(producer: Vec<Box<dyn DataSourceChannel>>,consumer: Box<dyn DataSourceChannel>) -> Self {
        Self { producer, consumer }
    }
}

#[async_trait]
impl Close for MpscDataSourceChannel {
    async fn close(&mut self) -> crate::Result<()>{
        for ele in &mut self.producer {
            ele.close().await?;
        }

        self.consumer.close().await?;
        Ok(())
    }

}

#[async_trait]
impl DataSourceChannel for MpscDataSourceChannel {

    fn name(&self) -> &str {
        return "mspc-datasource";
    }

    async fn run(&mut self, context: &mut DataSourceContext, _chanel: ChannelContext) -> crate::Result<()> {
        let (tx, rx) = mpsc::channel(10000);
        
        for producer in &mut self.producer {
            producer.send(context, tx.clone()).await?;
        }
        self.consumer.recv(context, rx).await?;
        Ok(())
    }
}

#[async_trait]
impl Close for DelegatedDataSource {
    async fn close(&mut self) -> crate::Result<()> {
        let datasources = &mut self.datasources;

        for datasource in datasources {
            datasource.close().await?;
        }

        Ok(())
    }
}

#[async_trait]
impl DataSourceChannel for DelegatedDataSource {
    fn need_log(&self) -> bool {
        return false;
    }
    fn name(&self) -> &str {
        return self.name.as_str();
    }

    async fn run(&mut self, context: &mut DataSourceContext, _channel: ChannelContext) -> crate::Result<()> {
        let datasources = &mut self.datasources;
        let channel = ChannelContext::new(None, None);
        for datasource in datasources {
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
        context: &mut DataSourceContext,
        ) -> crate::Result<()> {
        output.run(context, ChannelContext::new(None, None)).await
    }
}


#[async_trait]
pub trait DataSourceChannel: Send + Sync + fmt::Debug + Close{
    
    fn need_log(&self) -> bool {
        return true;
    }
    
    /// 通用初始化逻辑
    async fn init(&mut self, _context: &mut DataSourceContext , _channel: ChannelContext) {
        //注册日志
        if self.need_log() {
            register(&self.name().clone()).await;
        }
    }

    async fn before_run(&mut self, _context: &mut DataSourceContext, _channel: ChannelContext) -> crate::Result<()> {

        Ok(())
    }

    async fn after_run(&mut self, _context: &mut DataSourceContext, _channel: ChannelContext) -> crate::Result<()> {
        Ok(())
    }

    async fn execute(&mut self, context: &mut DataSourceContext, channel: ChannelContext) -> crate::Result<()> {
        self.init(context, channel.clone()).await;
        self.before_run(context, channel.clone()).await?;
        self.run(context, channel.clone()).await?;
        self.after_run(context, channel.clone()).await
    }
    
    async fn send(&mut self, context: &mut DataSourceContext, sender: Sender<serde_json::Value>) -> crate::Result<()> {
        let channel_context = ChannelContext::new(Some(sender), None);
        
        self.execute(context, channel_context).await
    }
    
    async fn recv(&mut self, context: &mut DataSourceContext, receiver: Receiver<serde_json::Value>) -> crate::Result<()> {
        let channel_context = ChannelContext::new(None, Some(receiver));

        self.execute(context, channel_context).await
    }

    fn sources(&self) -> Option<&Vec<String>> {
        return None;
    }

    fn source_type(&self) -> Option<DataSourceEnum> {
        return None;
    }

    fn batch(&self) -> Option<usize> {
        return None;
    }

    fn meta(&self) -> Option<serde_json::Value> {
        return None;
    }

    /// 每个通道的参数
    fn channel_schema(&self) -> Option<ChannelSchema> {
        return None;
    }

    fn transfer_to_schema(&self) -> Option<DataSourceSchema> {
        match self.channel_schema() {
            Some(channel_schema) => Some(DataSourceSchema {
                name: self.name().to_owned(),
                source: match self.source_type() {
                    Some(source_type) => source_type,
                    None => return None,
                },
                sources: Vec::new(),
                meta: match self.meta() {
                    Some(meta) => meta,
                    None => return None,
                },
                columns: match self.columns() {
                    Some(columns) => {
                        let mut res = json!({});
                        for column in columns {
                            res[&column.name] = json!(column.data_type().to_string());
                        }
                        res
                    }
                    None => return None,
                },
                channel: channel_schema,
            }),
            None => None,
        }
    }

    fn count(&self) -> Option<usize> {
        return None;
    }

    fn concurrency(&self) -> usize {
        return 1;
    }

    fn name(&self) -> &str;

    fn columns(&self) -> Option<&Vec<DataSourceColumn>> {
        return None;
    }

    fn columns_mut(&mut self, _columns: Vec<DataSourceColumn>) {}


    async fn get_columns_define(&mut self) -> Option<Vec<DataSourceColumn>> {
        return None;
    }

    fn get_task(
        &mut self,
        _channel: ChannelContext,
        _columns: Vec<DataSourceColumn>,
        _shutdown_complete_tx: mpsc::Sender<()>,
        _shutdown: Shutdown,
        _count_rc: Option<Arc<AtomicI64>>,
        _limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
        ) -> Option<Box<dyn Task>> {
        return None;
    }

    fn is_shutdown(&self) -> bool {
        return false;
    }

    ///执行流程
    ///1. 获取字段定义
    ///2. 生成输出任务
    ///3. 等待任务执行完毕
    async fn run(&mut self, context: &mut DataSourceContext, channel: ChannelContext) -> crate::Result<()> {
        // 获取字段定义
        let columns = self.get_columns_define().await.unwrap_or(vec![]);

        let default_columns = Vec::new();

        let schema_columns = self.columns().unwrap_or(&default_columns);

        // 合并两个数组
        let columns = DataSourceColumn::merge_columns(&columns, &schema_columns);
        self.columns_mut(columns.clone());

        match self.transfer_to_schema() {
            Some(schema) => {
                context
                    .schema
                    .sources
                    .push(schema);
            }
            None => {
                println!("不能解析output的schema");
            }
        }

        if context.skip {
            return Ok(());
        }
        let (notify_shutdown, _) = broadcast::channel::<()>(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(1);
        println!("{} will running...", self.name());
        let limiter = context
            .limit
            .map(|limit| Arc::new(Mutex::new(TokenBuketLimiter::new(limit, limit * 2))));

        let count_rc = self
            .count()
            .map(|count| Arc::new(AtomicI64::new(count as i64)));

        let concurrency = Arc::new(Semaphore::new(self.concurrency()));
        while !self.is_shutdown() {
            // 检查数量
            if let Some(count) = count_rc.as_ref() {
                if count.load(Ordering::SeqCst) <= 0 {
                    break;
                }
            }

            let permit = concurrency.clone().acquire_owned().await.unwrap();

            let columns = columns.clone();
            let shutdown = Shutdown::new(notify_shutdown.subscribe());

            let count_rc = count_rc.clone();
            let limiter = limiter.clone();
            let channel = channel.clone();
            let task = self.get_task(
                channel,
                columns,
                shutdown_complete_tx.clone(),
                shutdown,
                count_rc,
                limiter,
            );

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