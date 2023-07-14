use async_trait::async_trait;

use tokio::sync::mpsc;
use crate::core::shutdown::Shutdown;
use crate::core::traits::{Name, TaskDetailStatic};
use crate::datasource::ChannelContext;
use crate::exec::Executor;
use crate::exec::fake::FakeTaskExecutor;

use super::Task;

impl Name for FakeTask {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl TaskDetailStatic for FakeTask {}

#[derive(Debug)]
pub struct FakeTask {
    pub id: String,
    pub name: String,
    pub shutdown_sender: mpsc::Sender<()>,
    pub shutdown: Shutdown,
    pub executor: FakeTaskExecutor,
}

#[async_trait]
impl Task for FakeTask {
    fn shutdown(&mut self) -> &mut Shutdown {
        return &mut self.shutdown;
    }

    fn executor(&self) -> Box<dyn Executor> {
        return Box::new(self.executor.clone());
    }
}

impl FakeTask {
    pub fn from_args(
        pid: &str,
        name: &str,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        channel: ChannelContext,
    ) -> Self {
        Self {
            id: pid.to_owned(),
            name: name.to_owned(),
            shutdown_sender,
            shutdown,
            executor: FakeTaskExecutor::new(pid.to_owned(), name.to_owned(), channel.sender),
        }
    }
}
