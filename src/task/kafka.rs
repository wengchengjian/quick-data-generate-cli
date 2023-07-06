use std::sync::{atomic::AtomicI64, Arc};

use async_trait::async_trait;
use rdkafka::producer::FutureProducer;
use tokio::sync::{mpsc, Mutex};

use crate::{
    core::{
        limit::token::TokenBuketLimiter,
        shutdown::Shutdown,
        traits::{Name, TaskDetailStatic},
    },
    datasource::{kafka::KafkaArgs},
    exec::{kafka::KafkaTaskExecutor, Exector},
    model::column::DataSourceColumn,
};

use super::Task;

pub struct KafkaTask {
    pub name: String,
    pub shutdown_sender: mpsc::Sender<()>,
    pub shutdown: Shutdown,
    pub executor: KafkaTaskExecutor,
}
impl Name for KafkaTask {
    fn name(&self) -> &str {
        &self.name
    }
}

impl TaskDetailStatic for KafkaTask {}

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
        producer: FutureProducer,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        count_rc: Option<Arc<AtomicI64>>,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> Self {
        let name2 = name.clone();
        Self {
            name,
            shutdown_sender,
            shutdown,
            executor: KafkaTaskExecutor {
                task_name: name2,
                limiter,
                producer,
                count_rc,
            },
        }
    }
}
