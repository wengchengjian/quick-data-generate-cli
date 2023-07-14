use std::sync::{Arc, atomic::AtomicI64};

use async_trait::async_trait;
use mysql_async::Pool;

use tokio::sync::{mpsc, Mutex};
use crate::core::limit::token::TokenBuketLimiter;
use crate::core::shutdown::Shutdown;
use crate::core::traits::{Name, TaskDetailStatic};
use crate::datasource::ChannelContext;
use crate::exec::Executor;
use crate::exec::mysql::MysqlTaskExecutor;

use super::Task;

#[derive(Debug)]
pub struct MysqlTask {
    pub id: String,
    pub name: String,
    pub shutdown_sender: mpsc::Sender<()>,
    pub shutdown: Shutdown,
    pub executor: MysqlTaskExecutor,
}
impl Name for MysqlTask {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> &str {
        return &self.id;
    }
}

impl TaskDetailStatic for MysqlTask {}

#[async_trait]
impl Task for MysqlTask {
    fn shutdown(&mut self) -> &mut Shutdown {
        return &mut self.shutdown;
    }

    fn executor(&self) -> Box<dyn Executor> {
        return Box::new(self.executor.clone());
    }
}

impl MysqlTask {
    pub fn from_args(
        pid: &str,
        name: &str,
        pool: Pool,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
        count_rc: Option<Arc<AtomicI64>>,
        channel: ChannelContext,
    ) -> Self {
        Self {
            id: pid.to_owned(),
            name: name.to_owned(),
            shutdown_sender,
            shutdown,
            executor: MysqlTaskExecutor::new(
                pid.to_owned(),
                pool,
                count_rc,
                name.to_owned(),
                limiter,
                channel.receiver,
                channel.sender,
            ),
        }
    }
}