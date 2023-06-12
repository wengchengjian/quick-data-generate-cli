use std::time::Duration;

use async_trait::async_trait;
use mysql_async::{
    prelude::{BatchQuery, WithParams},
    Conn,
};
use rdkafka::{
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
};
use tokio::sync::mpsc;

use crate::{
    core::{fake::{get_fake_data, get_random_string}, log::incr_log, shutdown::Shutdown, error::Error},
    model::column::OutputColumn,
    output::{kafka::KafkaArgs, mysql::MysqlArgs, Close},
};

pub struct KafkaTask {
    pub name: String,
    pub batch: usize,
    pub count: usize,
    pub shutdown_sender: mpsc::Sender<()>,
    pub shutdown: Shutdown,
    pub columns: Vec<OutputColumn>,
    pub executor: KafkaTaskExecutor,
}
#[async_trait]
impl Close for KafkaTask {
    async fn close(&mut self) -> crate::Result<()> {
        // 判断任务是否完成
        Ok(())
    }
}
impl KafkaTask {
    pub fn new(
        producer: FutureProducer,
        name: String,
        batch: usize,
        count: usize,
        columns: Vec<OutputColumn>,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        topic: String,
    ) -> KafkaTask {
        let columns2 = columns.clone();
        let name2 = name.clone();
        KafkaTask {
            name,
            batch,
            count,
            shutdown_sender,
            shutdown,
            columns,
            executor: KafkaTaskExecutor {
                topic,
                producer,
                batch: batch,
                count: count,
                columns: columns2,
                task_name: name2,
            },
        }
    }

    pub fn from_args(
        name: String,
        args: &KafkaArgs,
        producer: FutureProducer,
        columns: Vec<OutputColumn>,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
    ) -> Self {
        let columns2 = columns.clone();
        let name2 = name.clone();
        Self {
            name,
            batch: args.batch,
            count: args.count,
            shutdown_sender,
            shutdown,
            columns,
            executor: KafkaTaskExecutor {
                batch: args.batch,
                count: args.count,
                columns: columns2,
                task_name: name2,
                producer,
                topic: args.topic.clone()
            },
        }
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() {
            tokio::select! {
                res = self.executor.add_batch() => {
                    if let Err(e) = res {
                        println!("{:?}", e);
                        break;
                    }
                },
                _ = self.shutdown.recv() => {
                    continue;
                }
            };
        }
        Ok(())
    }
}

pub struct KafkaTaskExecutor {
    pub batch: usize,
    pub count: usize,
    pub columns: Vec<OutputColumn>,
    pub task_name: String,
    pub producer: FutureProducer,
    pub topic: String,
}

impl KafkaTaskExecutor {
    pub fn new(
        producer: FutureProducer,
        batch: usize,
        count: usize,
        columns: Vec<OutputColumn>,
        task_name: String,
        topic: String,
    ) -> Self {
        Self {
            producer,
            batch,
            columns,
            count,
            task_name,
            topic,
        }
    }

    pub async fn add_batch(&mut self) -> crate::core::error::Result<()> {
        for _i in 0..self.batch {
            let data = get_fake_data(&self.columns);
            let key = get_random_string();
            
            let data = serde_json::to_string(&data).unwrap();
            self.producer
                .send(
                    FutureRecord::to(self.topic.as_str()).key(&key).payload(&data),
                    Timeout::After(Duration::from_secs(0)),
                )
                .await.map_err(|(e,_m)| Error::from(e))?;
        }

        incr_log(&self.task_name, self.batch, 1).await;
        
        Ok(())
    }
}
