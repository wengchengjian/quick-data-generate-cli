use async_trait::async_trait;

use crate::{core::shutdown::Shutdown, exec::Exector};

pub mod clickhouse;
pub mod csv;
pub mod fake;
pub mod kafka;
pub mod mysql;

#[async_trait]
pub trait Task: Send + Sync {
    fn shutdown(&mut self) -> &mut Shutdown;

    fn executor(&self) -> Box<dyn Exector>;

    async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown().is_shutdown {
            let mut exector = self.executor();
            match exector.execute().await {
                Err(e) => {
                    println!("{:?}", e);
                    break;
                }
                Ok(completed) => {
                    if completed {
                        break;
                    }
                }
            }
            self.shutdown().try_recv();
        }
        Ok(())
    }
}
