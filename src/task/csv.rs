use std::sync::{atomic::AtomicI64, Arc};

use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex};

use crate::{
    core::{
        limit::token::TokenBuketLimiter,
        shutdown::Shutdown,
        traits::{Name, TaskDetailStatic},
    },
    exec::{csv::CsvTaskExecutor, Exector},
};

use super::Task;

#[derive(Debug)]
pub struct CsvTask {
    pub id: String,
    pub name: String,
    pub shutdown_sender: mpsc::Sender<()>,
    pub shutdown: Shutdown,
    pub executor: CsvTaskExecutor,
}

impl Name for CsvTask {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl TaskDetailStatic for CsvTask {}

#[async_trait]
impl Task for CsvTask {
    fn shutdown(&mut self) -> &mut Shutdown {
        return &mut self.shutdown;
    }

    fn executor(&self) -> Box<dyn Exector> {
        return Box::new(self.executor.clone());
    }
}

impl CsvTask {
    pub fn from_args(
        pid: &str,
        name: &str,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
        count_rc: Option<Arc<AtomicI64>>,
    ) -> Self {
        Self {
            id: pid.to_owned(),
            name: name.to_owned(),
            shutdown_sender,
            shutdown,
            executor: CsvTaskExecutor::new(pid.to_owned(), count_rc, name.to_owned(), limiter),
        }
    }
}
