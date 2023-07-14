use std::{
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

use super::{ChannelContext, DataSourceChannel};
use crate::{
    core::{
        cli::Cli,
        error::{Error, IoError, Result},
        limit::token::TokenBuketLimiter,
        shutdown::Shutdown,
        traits::{Name, TaskDetailStatic},
    },
    model::{
        column::DataSourceColumn,
        schema::{ChannelSchema, DataSourceSchema},
    },
    task::{kafka::KafkaTask, Task},
    Json,
};
use async_trait::async_trait;
use mysql_common::frunk::labelled::chars::E;
use rdkafka::{
    consumer::{CommitMode, Consumer, DefaultConsumerContext, StreamConsumer},
    producer::FutureProducer,
    ClientConfig, Message,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    sync::{mpsc, Mutex},
    time::timeout,
};
use uuid::Uuid;
#[derive(Debug)]
pub struct KafkaDataSource {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct KafkaArgs {
    pub host: String,

    pub port: u16,

    pub topic: String,

    pub concurrency: usize,

    pub batch: usize,

    pub count: isize,
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
                .ok_or(Error::Io(IoError::ArgNotFound("host")))?
                .to_string(),
            port: meta["port"].as_u64().unwrap_or(3306) as u16,
            batch: channel.batch.unwrap_or(1000),
            count: channel.count.unwrap_or(isize::MAX),
            concurrency: channel.concurrency.unwrap_or(usize::MAX),
            topic: meta["topic"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("topic")))?
                .to_string(),
        })
    }
}

pub static DEFAULT_GROUP: &'static str = "vFBiQ6aasB";

impl KafkaDataSource {
    pub fn new(schema: DataSourceSchema, id: &str) -> Self {
        Self {
            id: id.to_owned(),
            name: schema.name,
        }
    }

    pub async fn get_provider(&self) -> Result<FutureProducer> {
        let host = format!("{}:{}", self.host().await, self.port().await);
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

    pub async fn host(&self) -> String {
        self.meta("host")
            .await
            .unwrap_or(json!("localhost"))
            .as_str()
            .unwrap_or("localhost")
            .to_owned()
    }

    pub async fn port(&self) -> u64 {
        self.meta("port")
            .await
            .unwrap_or(json!(9092))
            .as_u64()
            .unwrap_or(9092)
    }

    pub async fn get_consumer(
        &self,
        group: String,
        topics: &[&str],
    ) -> Result<StreamConsumer<DefaultConsumerContext>> {
        let host = format!("{}:{}", self.host().await, self.port().await);
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

    #[deprecated(since = "0.1.0", note = "Please use the parse schema function instead")]
    pub(crate) fn from_cli(_cli: Cli) -> Result<Box<dyn DataSourceChannel>> {
        let res = KafkaDataSource {
            id: Uuid::new_v4().to_string(),
            name: "kafka".into(),
        };

        Ok(Box::new(res))
    }
}

impl Name for KafkaDataSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl TaskDetailStatic for KafkaDataSource {}

#[async_trait]
impl DataSourceChannel for KafkaDataSource {
    async fn get_columns_define(&mut self) -> crate::Result<Vec<DataSourceColumn>> {
        if let Some(_schema) = self.schema().await {
            if let Some(topic) = self.meta("topic").await.unwrap().as_str() {
                let ref topics = vec![topic];
                let consumer = self.get_consumer(DEFAULT_GROUP.to_string(), topics).await?;
                // 获取字段定义
                let columns = KafkaDataSource::get_columns_define(consumer).await;
                return Ok(columns);
            } else {
                Err(Error::Io(IoError::ArgNotFound("topic")))
            }
        } else {
            Err(Error::Io(IoError::SchemaNotFound))
        }
    }

    async fn get_task(
        &mut self,
        _channel: ChannelContext,
        shutdown_complete_tx: mpsc::Sender<()>,
        shutdown: Shutdown,
        count_rc: Option<Arc<AtomicI64>>,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> crate::Result<Option<Box<dyn Task>>> {
        // 获取生产者
        let producer = self.get_provider().await?;
        let task = KafkaTask::from_args(
            self.id(),
            self.name(),
            producer,
            shutdown_complete_tx,
            shutdown,
            count_rc,
            limiter,
        );
        return Ok(Some(Box::new(task)));
    }
}

impl TryFrom<Cli> for Box<KafkaDataSource> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(_value: Cli) -> std::result::Result<Self, Self::Error> {
        let res = KafkaDataSource {
            id: Uuid::new_v4().to_string(),
            name: "kafka".into(),
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
            count: self.count.unwrap_or(isize::max_value()),
            concurrency: self.concurrency.unwrap_or(1),
            topic: self.topic.ok_or(Error::Io(IoError::ArgNotFound("topic")))?,
        })
    }
}

impl TryFrom<DataSourceSchema> for KafkaDataSource {
    type Error = Error;

    fn try_from(value: DataSourceSchema) -> std::result::Result<Self, Self::Error> {
        Ok(KafkaDataSource {
            id: Uuid::new_v4().to_string(),
            name: value.name,
        })
    }
}
