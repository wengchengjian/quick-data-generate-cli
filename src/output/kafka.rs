use std::time::Duration;

use super::{Close, Output, OutputContext};
use crate::{core::error::{Result, Error}, model::column::OutputColumn};
use async_trait::async_trait;
use rdkafka::{
    consumer::{CommitMode, Consumer, DefaultConsumerContext, StreamConsumer},
    producer::FutureProducer,
    ClientConfig, Message,
};
use tokio::time::{timeout, Timeout};

#[derive(Debug)]
pub struct KafkaOutput {
    pub name: String,
    pub args: KafkaArgs,
    pub columns: Vec<OutputColumn>,
}

#[derive(Debug)]
pub struct KafkaArgs {
    pub host: String,

    pub port: u16,

    pub topic: String,

    pub partitions: Option<Vec<i32>>,
}

pub static DEFAULT_GROUP: &'static str = "vFBiQ6aasB";

impl KafkaOutput {
    pub fn get_provider(&self) -> Result<FutureProducer> {
        let host = format!("{}:{}", self.args.host, self.args.port);
        return ClientConfig::new()
            .set("bootstrap.servers", host)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(From::from);
    }

    /// 尝试获取字段定义
    pub async fn get_columns_define(
        consumer: StreamConsumer<DefaultConsumerContext>,
    ) -> Vec<OutputColumn> {
        match timeout(Duration::from_millis(5), consumer.recv()).await {
            Ok(result) => match result {
                Ok(message) => {
                    let payload = match message.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(e)) => "",
                    };

                    if payload.len() == 0 {
                        return vec![];
                    }
                    let val: serde_json::Value = serde_json::from_str(payload);

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
}

#[async_trait]
impl Output for KafkaOutput {
    fn get_columns(&self) -> Option<&Vec<crate::model::column::OutputColumn>> {
        return Some(&self.columns);
    }

    fn name(&self) -> &str {
        return &self.name;
    }

    async fn run(&mut self, _context: &mut OutputContext) -> Result<()> {
        // 获取生产者
        let producer = self.get_provider()?;
        // 获取生产者
        let topic = self.args.topic.as_str();
        let ref topics = vec![topic];
        let consumer = self.get_consumer(DEFAULT_GROUP.to_string(), topics)?;
        // 获取字段定义
        let columns = KafkaOutput::get_columns_define(consumer).await;

        let schema_columns = &self.columns;

        // 合并两个数组
        let columns = OutputColumn::merge_columns(schema_columns, &columns);

        let (notify_shutdown, _) = broadcast::channel::<()>(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(1);

        println!("{} will running...", self.name);

        let concurrency = Arc::new(Semaphore::new(self.args.concurrency));
        while !self.shutdown.load(Ordering::SeqCst) {
            let permit = concurrency.clone().acquire_owned().await.unwrap();

            let conn = pool
                .get_conn()
                .await
                .map_err(|err| Error::Other(err.into()))?;
            let columns = columns.clone();

            let shutdown = Shutdown::new(notify_shutdown.subscribe());
            let mut task = MysqlTask::from_args(
                self.name.clone(),
                &self.args,
                conn,
                columns,
                shutdown_complete_tx.clone(),
                shutdown,
            );

            tokio::spawn(async move {
                if let Err(err) = task.run().await {
                    println!("task run error: {}", err);
                }
                drop(permit);
            });
        }

        // When `notify_shutd4own` is dropped, all tasks which have `subscribe`d will
        // receive the shutdown signal and can exit
        drop(notify_shutdown);
        // Drop final `Sender` so the `Receiver` below can complete
        drop(shutdown_complete_tx);
        // 等待所有的future执行完毕
        // futures::future::join_all(futures).await;
        let _ = shutdown_complete_rx.recv().await;

        Ok(())
    }
}

#[async_trait]
impl Close for KafkaOutput {
    async fn close(&mut self) -> Result<()> {
        todo!()
    }
}
