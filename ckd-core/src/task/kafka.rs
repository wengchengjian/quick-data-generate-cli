use std::sync::{atomic::AtomicI64, Arc};

use async_trait::async_trait;
use rdkafka::producer::FutureProducer;
use tokio::sync::{mpsc, Mutex};
use crate::core::limit::token::TokenBuketLimiter;
use crate::core::shutdown::Shutdown;
use crate::core::traits::{Name, TaskDetailStatic};
use crate::exec::Executor;
use crate::exec::kafka::KafkaTaskExecutor;

use super::Task;

pub struct KafkaTask {
    pub id: String,
    pub name: String,
    pub shutdown_sender: mpsc::Sender<()>,
    pub shutdown: Shutdown,
    pub executor: KafkaTaskExecutor,
}
impl Name for KafkaTask {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> &str {
        todo!()
    }
}

impl TaskDetailStatic for KafkaTask {}

#[async_trait]
impl Task for KafkaTask {
    fn shutdown(&mut self) -> &mut Shutdown {
        return &mut self.shutdown;
    }

    fn executor(&self) -> Box<dyn Executor> {
        return Box::new(self.executor.clone());
    }
}

impl KafkaTask {
    pub fn from_args(
        pid: &str,
        name: &str,
        producer: FutureProducer,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        count_rc: Option<Arc<AtomicI64>>,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> Self {
        Self {
            id: pid.to_owned(),
            name: name.to_owned(),
            shutdown_sender,
            shutdown,
            executor: KafkaTaskExecutor {
                id: pid.to_owned(),
                task_name: name.to_owned(),
                limiter,
                producer,
                count_rc,
            },
        }
    }
}
