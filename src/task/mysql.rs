use std::sync::{atomic::AtomicI64, Arc};

use async_trait::async_trait;
use mysql_async::Pool;

use tokio::sync::{mpsc, Mutex};

use crate::{
    core::{limit::token::TokenBuketLimiter, shutdown::Shutdown, traits::{Name, TaskDetailStatic}},
    datasource::{ChannelContext},
    exec::{mysql::MysqlTaskExecutor, Exector},
};

use super::Task;

#[derive(Debug)]
pub struct MysqlTask {
    pub name: String,
    pub shutdown_sender: mpsc::Sender<()>,
    pub shutdown: Shutdown,
    pub executor: MysqlTaskExecutor,
}
impl Name for MysqlTask {
    fn name(&self) -> &str {
        &self.name
    }
}

impl TaskDetailStatic for MysqlTask {}

#[async_trait]
impl Task for MysqlTask {
    fn shutdown(&mut self) -> &mut Shutdown {
        return &mut self.shutdown;
    }

    fn executor(&self) -> Box<dyn Exector> {
        return Box::new(self.executor.clone());
    }
}

impl MysqlTask {
    pub fn from_args(
        name: String,
        pool: Pool,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
        count_rc: Option<Arc<AtomicI64>>,
        channel: ChannelContext,
    ) -> Self {
        let name2 = name.clone();
        Self {
            name,
            shutdown_sender,
            shutdown,
            executor: MysqlTaskExecutor::new(
                pool,
                count_rc,
                name2,
                limiter,
                channel.receiver,
                channel.sender,
            ),
        }
    }
}
