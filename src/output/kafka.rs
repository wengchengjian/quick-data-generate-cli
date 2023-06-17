use std::{
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

use super::{Close, Output};
use crate::{
    core::{
        cli::Cli,
        error::{Error, IoError, Result},
        limit::token::TokenBuketLimiter,
        shutdown::Shutdown,
    },
    model::{
        column::OutputColumn,
        schema::{ChannelSchema, OutputSchema},
    },
    task::{kafka::KafkaTask, Task},
};
use async_trait::async_trait;
use rdkafka::{
    consumer::{CommitMode, Consumer, DefaultConsumerContext, StreamConsumer},
    producer::FutureProducer,
    ClientConfig, Message,
};
use tokio::{
    sync::{mpsc, Mutex},
    time::timeout,
};

#[derive(Debug)]
pub struct KafkaOutput {
    pub name: String,
    pub args: KafkaArgs,
    pub shutdown: AtomicBool,
    pub columns: Vec<OutputColumn>,
}

#[derive(Debug)]
pub struct KafkaArgs {
    pub host: String,

    pub port: u16,

    pub topic: String,

    pub concurrency: usize,

    pub batch: usize,

    pub count: usize,
}

impl KafkaArgs {
    pub fn from_value(meta: serde_json::Value, channel: ChannelSchema) -> Result<KafkaArgs> {
        Ok(KafkaArgs {
            host: meta["host"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("host".to_string())))?
                .to_string(),
            port: meta["port"].as_u64().unwrap_or(3306) as u16,
            batch: channel.batch,
            count: channel.count,
            concurrency: channel.concurrency,
            topic: meta["topic"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("topic".to_string())))?
                .to_string(),
        })
    }
}

pub static DEFAULT_GROUP: &'static str = "vFBiQ6aasB";

impl KafkaOutput {
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
    ) -> Vec<OutputColumn> {
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

                    let columns = OutputColumn::get_columns_from_value(&val);
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

    pub(crate) fn from_cli(cli: Cli) -> Result<Box<dyn Output>> {
        let res = KafkaOutput {
            name: "kafka".into(),
            args: cli.try_into()?,
            shutdown: AtomicBool::new(false),
            columns: vec![],
        };

        Ok(Box::new(res))
    }
}

#[async_trait]
impl Output for KafkaOutput {
    fn columns(&self) -> Option<&Vec<OutputColumn>> {
        return Some(&self.columns);
    }

    fn concurrency(&self) -> usize {
        return self.args.concurrency;
    }

    fn name(&self) -> &str {
        return &self.name;
    }

    async fn get_columns_define(&mut self) -> Option<Vec<OutputColumn>> {
        let topic = self.args.topic.as_str();
        let ref topics = vec![topic];
        match self.get_consumer(DEFAULT_GROUP.to_string(), topics) {
            Ok(consumer) => {
                // 获取字段定义
                let columns = KafkaOutput::get_columns_define(consumer).await;
                return Some(columns);
            }
            Err(_) => {
                return None;
            }
        }
    }

    fn get_output_task(
        &mut self,
        columns: Vec<OutputColumn>,
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

impl TryFrom<Cli> for Box<KafkaOutput> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: Cli) -> std::result::Result<Self, Self::Error> {
        let res = KafkaOutput {
            name: "kafka".into(),
            args: value.try_into()?,
            shutdown: AtomicBool::new(false),
            columns: vec![],
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

impl TryFrom<OutputSchema> for KafkaOutput {
    type Error = Error;

    fn try_from(value: OutputSchema) -> std::result::Result<Self, Self::Error> {
        Ok(KafkaOutput {
            name: "默认kafka输出".to_string(),
            args: KafkaArgs::from_value(value.meta, value.channel)?,
            shutdown: AtomicBool::new(false),
            columns: OutputColumn::get_columns_from_schema(&value.columns),
        })
    }
}

#[async_trait]
impl Close for KafkaOutput {
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
