use async_trait::async_trait;

pub mod clickhouse;
pub mod kafka;
pub mod mysql;

#[async_trait]
pub trait Task: Send + Sync {
    async fn run(&mut self) -> crate::Result<()>;
}
