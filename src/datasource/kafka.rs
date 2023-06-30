use std::{
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

use super::{ChannelContext, Close, DataSourceChannel, DataSourceEnum};
use crate::{
    core::{
        cli::Cli,
        error::{Error, IoError, Result},
        limit::token::TokenBuketLimiter,
        shutdown::Shutdown,
    },
    model::{
        column::DataSourceColumn,
        schema::{ChannelSchema, DataSourceSchema},
    },
    task::{kafka::KafkaTask, Task},
    Json,
};
use async_trait::async_trait;
use rdkafka::{
    consumer::{CommitMode, Consumer, DefaultConsumerContext, StreamConsumer},
    producer::FutureProducer,
    ClientConfig, Message,
};
use serde::{Serialize, Deserialize};
use serde_json::json;
use tokio::{
    sync::{mpsc, Mutex},
    time::timeout,
};

#[derive(Debug)]
pub struct KafkaDataSource {
    pub name: String,
    pub args: KafkaArgs,
    pub shutdown: AtomicBool,
    pub columns: Vec<DataSourceColumn>,
    pub sources: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct KafkaArgs {
    pub host: String,

    pub port: u16,

    pub topic: String,

    pub concurrency: usize,

    pub batch: usize,

    pub count: usize,
}

impl KafkaArgs {
    pub fn from_value(meta: Option<Json>, channel: Option<ChannelSchema>) -> Result<KafkaArgs> {
        let meta = meta.unwrap_or(json!({
        "host":"127.0.0.1",
        "port": 9092
        }));

        let channel = channel.unwrap_or(ChannelSchema::default());

        Ok(KafkaArgs {
            host: meta["host"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("host".to_string())))?
                .to_string(),
            port: meta["port"].as_u64().unwrap_or(3306) as u16,
            batch: channel.batch.unwrap_or(1000),
            count: channel.count.unwrap_or(usize::MAX),
            concurrency: channel.concurrency.unwrap_or(usize::MAX),
            topic: meta["topic"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("topic".to_string())))?
                .to_string(),
        })
    }
}

pub static DEFAULT_GROUP: &'static str = "vFBiQ6aasB";

impl KafkaDataSource {
    pub fn get_provider(&self) -> Result<FutureProducer> {
        let host = format!("{}:{}", self.args.host, self.args.port);
        return ClientConfig::new()
            .set("bootstrap.servers", host)
            .set("message.timeout.ms", "10000")
            .set("acks", "0")
            .set("compression.codec", "lz4")
            .set("socket.send.buffer.bytes", "67108864")
            .set("socket.keepalive.enable", "true")
            .set("batch.num.messages", "32768")
            .set("linger.ms", "0")
            .set("request.timeout.ms", "100")
            .set("max.in.flight.requests.per.connection", "100")
            .create()
            .map_err(From::from);
    }

    /// 尝试获取字段定义
    pub async fn get_columns_define(
        consumer: StreamConsumer<DefaultConsumerContext>,
    ) -> Vec<DataSourceColumn> {
        match timeout(Duration::from_secs(5), consumer.recv()).await {
            Ok(result) => match result {
                Ok(message) => {
                    let payload = match message.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(_e)) => "",
                    };

                    if payload.len() == 0 {
                        return vec![];
                    }
                    let val: serde_json::Value = serde_json::from_str(payload).unwrap();

                    let columns = DataSourceColumn::get_columns_from_value(&val);
                    consumer
                        .commit_message(&message, CommitMode::Async)
                        .unwrap();
                    return columns;
                }
                Err(_) => vec![],
            },
            Err(_) => vec![],
        }
    }

    pub fn get_consumer(
        &self,
        group: String,
        topics: &[&str],
    ) -> Result<StreamConsumer<DefaultConsumerContext>> {
        let host = format!("{}:{}", self.args.host, self.args.port);
        let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
            .set("group.id", group)
            .set("bootstrap.servers", host)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            //.set("statistics.interval.ms", "30000")
            .set("auto.offset.reset", "earliest")
            .create_with_context(DefaultConsumerContext::default())
            .map_err(|e| Error::Other(Box::new(e)))?;
        consumer
            .subscribe(&topics.to_vec())
            .expect("Can't subscribe to specified topics");

        return Ok(consumer);
    }

    pub(crate) fn from_cli(cli: Cli) -> Result<Box<dyn DataSourceChannel>> {
        let res = KafkaDataSource {
            name: "kafka".into(),
            args: cli.try_into()?,
            shutdown: AtomicBool::new(false),
            columns: vec![],
            sources: vec!["fake_data_source".to_owned()],
        };

        Ok(Box::new(res))
    }
}

#[async_trait]
impl DataSourceChannel for KafkaDataSource {
    fn sources(&self) -> Option<&Vec<String>> {
        return Some(&self.sources);
    }

    fn columns_mut(&mut self, columns: Vec<DataSourceColumn>) {
        self.columns = columns;
    }

    fn source_type(&self) -> Option<DataSourceEnum> {
        return Some(DataSourceEnum::Kafka);
    }

    fn batch(&self) -> Option<usize> {
        return Some(self.args.batch);
    }

    fn meta(&self) -> Option<serde_json::Value> {
        return Some(json!({
            "host": self.args.host,
            "port": self.args.port,
            "topic": self.args.topic
        }));
    }

    fn channel_schema(&self) -> Option<ChannelSchema> {
        return Some(ChannelSchema {
            batch: Some(self.args.batch),
            concurrency: Some(self.args.concurrency),
            count: Some(self.args.count),
        });
    }

    fn columns(&self) -> Option<&Vec<DataSourceColumn>> {
        return Some(&self.columns);
    }

    fn concurrency(&self) -> usize {
        return self.args.concurrency;
    }

    fn name(&self) -> &str {
        return &self.name;
    }

    async fn get_columns_define(&mut self) -> Option<Vec<DataSourceColumn>> {
        let topic = self.args.topic.as_str();
        let ref topics = vec![topic];
        match self.get_consumer(DEFAULT_GROUP.to_string(), topics) {
            Ok(consumer) => {
                // 获取字段定义
                let columns = KafkaDataSource::get_columns_define(consumer).await;
                return Some(columns);
            }
            Err(_) => {
                return None;
            }
        }
    }

    fn get_task(
        &mut self,
        _channel: ChannelContext,
        columns: Vec<DataSourceColumn>,
        shutdown_complete_tx: mpsc::Sender<()>,
        shutdown: Shutdown,
        count_rc: Option<Arc<AtomicI64>>,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> Option<Box<dyn Task>> {
        // 获取生产者
        match self.get_provider() {
            Ok(producer) => {
                let task = KafkaTask::from_args(
                    self.name.clone(),
                    &self.args,
                    producer,
                    columns,
                    shutdown_complete_tx,
                    shutdown,
                    count_rc,
                    limiter,
                );
                return Some(Box::new(task));
            }
            Err(_) => {
                return None;
            }
        }
    }

    fn is_shutdown(&self) -> bool {
        return self.shutdown.load(Ordering::SeqCst);
    }
}

impl TryFrom<Cli> for Box<KafkaDataSource> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: Cli) -> std::result::Result<Self, Self::Error> {
        let res = KafkaDataSource {
            name: "kafka".into(),
            args: value.try_into()?,
            shutdown: AtomicBool::new(false),
            columns: vec![],
            sources: vec!["fake_data_source".to_owned()],
        };

        Ok(Box::new(res))
    }
}

impl TryInto<KafkaArgs> for Cli {
    type Error = Error;

    fn try_into(self) -> std::result::Result<KafkaArgs, Self::Error> {
        Ok(KafkaArgs {
            host: self.host,
            port: self.port.unwrap_or(9092),
            batch: self.batch.unwrap_or(5000),
            count: self.count.unwrap_or(isize::max_value() as usize),
            concurrency: self.concurrency.unwrap_or(1),
            topic: self
                .topic
                .ok_or(Error::Io(IoError::ArgNotFound("topic".to_owned())))?,
        })
    }
}

impl TryFrom<DataSourceSchema> for KafkaDataSource {
    type Error = Error;

    fn try_from(value: DataSourceSchema) -> std::result::Result<Self, Self::Error> {
        Ok(KafkaDataSource {
            name: value.name,
            args: KafkaArgs::from_value(value.meta, value.channel)?,
            shutdown: AtomicBool::new(false),
            columns: DataSourceColumn::get_columns_from_schema(&value.columns.unwrap_or(json!(0))),
            sources: value.sources.unwrap_or(vec![]),
        })
    }
}

#[async_trait]
impl Close for KafkaDataSource {
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
