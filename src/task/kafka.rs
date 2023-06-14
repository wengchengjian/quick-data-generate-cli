use std::{
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use tokio::sync::mpsc;

use crate::{
    core::{
        error::Error,
        fake::{get_fake_data, get_random_string},
        log::incr_log,
        shutdown::Shutdown,
    },
    model::column::OutputColumn,
    output::{kafka::KafkaArgs, Close},
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
    pub fn from_args(
        name: String,
        args: &KafkaArgs,
        producer: FutureProducer,
        columns: Vec<OutputColumn>,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        count_rc: Arc<AtomicI64>,
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
                count: count_rc,
                columns: columns2,
                task_name: name2,
                producer,
                topic: args.topic.clone(),
            },
        }
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() {
            tokio::select! {
                res = self.executor.add_batch() => {
                    match res {
                        Err(e) => {
                            println!("{:?}", e);
                            break;
                        },
                        Ok(completed) => {
                            if completed {
                                break;
                            }
                        }
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
    pub count: Arc<AtomicI64>,
    pub columns: Vec<OutputColumn>,
    pub task_name: String,
    pub producer: FutureProducer,
    pub topic: String,
}

impl KafkaTaskExecutor {
    pub async fn add_batch(&mut self) -> crate::core::error::Result<bool> {
        let mut num = 0;
        for _i in 0..self.batch {
            if self.count.load(Ordering::SeqCst) <= 0 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                break;
            }
            let data = get_fake_data(&self.columns);
            let key = get_random_string();

            let data = serde_json::to_string(&data).unwrap();
            if data.len() == 0 {
                continue;
            }
            self.producer
                .send(
                    FutureRecord::to(self.topic.as_str())
                        .key(&key)
                        .payload(&data),
                    Timeout::Never,
                )
                .await
                .map_err(|(e, _m)| Error::from(e))?;
            num += 1;
            self.count.fetch_sub(1, Ordering::SeqCst);
            if num % 100 == 0 {
                incr_log(&self.task_name, num, 1).await;
                num = 0;
            }
        }
        if num != 0 {
            incr_log(&self.task_name, num, 1).await;
        }
        match self.count.load(Ordering::SeqCst) {
            x if x <= 0 => Ok(true),
            _ => Ok(false),
        }
    }
}
