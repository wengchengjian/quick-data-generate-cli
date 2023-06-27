use async_trait::async_trait;

use tokio::sync::mpsc;

use crate::{
    core::shutdown::Shutdown,
    datasource::{fake::FakeArgs, ChannelContext, Close},
    exec::{fake::FakeTaskExecutor, Exector},
    model::column::DataSourceColumn,
};

use super::Task;

#[derive(Debug)]
pub struct FakeTask {
    pub name: String,
    pub batch: usize,
    pub shutdown_sender: mpsc::Sender<()>,
    pub shutdown: Shutdown,
    pub columns: Vec<DataSourceColumn>,
    pub executor: FakeTaskExecutor,
}
#[async_trait]
impl Close for FakeTask {
    async fn close(&mut self) -> crate::Result<()> {
        // 判断任务是否完成
        Ok(())
    }
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
        args: &FakeArgs,
        columns: Vec<DataSourceColumn>,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        channel: ChannelContext,
    ) -> Self {
        let columns2 = columns.clone();
        let name2 = name.clone();
        Self {
            name,
            batch: args.batch,
            shutdown_sender,
            shutdown,
            columns,
            executor: FakeTaskExecutor::new(args.batch, columns2, name2, channel.sender),
        }
    }
}
