use async_trait::async_trait;

use tokio::sync::mpsc;

use crate::{
    core::{
        shutdown::Shutdown,
        traits::{Name, TaskDetailStatic},
    },
    datasource::{ChannelContext},
    exec::{fake::FakeTaskExecutor, Exector},
};

use super::Task;

impl Name for FakeTask {
    fn name(&self) -> &str {
        &self.name
    }
}

impl TaskDetailStatic for FakeTask {}

#[derive(Debug)]
pub struct FakeTask {
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

    fn executor(&self) -> Box<dyn Exector> {
        return Box::new(self.executor.clone());
    }
}

impl FakeTask {
    pub fn from_args(
        name: String,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        channel: ChannelContext,
    ) -> Self {
        let name2 = name.clone();
        Self {
            name,
            shutdown_sender,
            shutdown,
            executor: FakeTaskExecutor::new(name2, channel.sender),
        }
    }
}
