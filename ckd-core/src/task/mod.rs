use async_trait::async_trait;
use crate::core::shutdown::Shutdown;
use crate::core::traits::{Name, TaskDetailStatic};
use crate::exec::Executor;

pub mod clickhouse;
pub mod csv;
pub mod fake;
pub mod kafka;
pub mod mysql;

#[async_trait]
pub trait Task: Send + Sync + Name + TaskDetailStatic {
    fn shutdown(&mut self) -> &mut Shutdown;

    fn executor(&self) -> Box<dyn Executor>;

    async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown().is_shutdown {
            let mut exector = self.executor();
            let completed = exector.execute().await?;
            if completed {
                break;
            }
            self.shutdown().try_recv();
        }
        Ok(())
    }
}