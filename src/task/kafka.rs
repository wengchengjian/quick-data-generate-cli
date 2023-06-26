use std::sync::{atomic::AtomicI64, Arc};

use async_trait::async_trait;
use rdkafka::producer::FutureProducer;
use tokio::sync::{mpsc, Mutex};

use crate::{
    core::{limit::token::TokenBuketLimiter, shutdown::Shutdown},
    exec::{kafka::KafkaTaskExecutor, Exector},
    model::column::{ DataSourceColumn}
, datasource::{kafka::KafkaArgs, Close},
};

use super::Task;

pub struct KafkaTask {
    pub name: String,
    pub batch: usize,
    pub count: usize,
    pub shutdown_sender: mpsc::Sender<()>,
    pub shutdown: Shutdown,
    pub columns: Vec<DataSourceColumn>,
    pub executor: KafkaTaskExecutor,
}
#[async_trait]
impl Close for KafkaTask {
    async fn close(&mut self) -> crate::Result<()> {
        // 判断任务是否完成
        Ok(())
    }
}

#[async_trait]
impl Task for KafkaTask {
    fn shutdown(&mut self) -> &mut Shutdown {
        return &mut self.shutdown;
    }

    fn executor(&self) -> Box<dyn Exector> {
        return Box::new(self.executor.clone());
    }
}

impl KafkaTask {
    pub fn from_args(
        name: String,
        args: &KafkaArgs,
        producer: FutureProducer,
        columns: Vec<DataSourceColumn>,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        count_rc: Option<Arc<AtomicI64>>,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
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
                limiter,
                producer,
                topic: args.topic.clone(),
            },
        }
    }
}
