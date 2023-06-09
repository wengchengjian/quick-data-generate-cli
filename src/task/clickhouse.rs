use async_trait::async_trait;
use clickhouse::{inserter::Inserter, Client};
use serde_json::Value;

use crate::{core::log::StaticsLogger, output::Close};

#[derive(Debug)]
pub struct ClickHouseTask {
    pub name: String,
}
#[async_trait]
impl Close for ClickHouseTask {
    async fn close(&mut self) -> crate::Result<()> {
        Ok(())
    }
}

impl ClickHouseTask {
    pub fn new(name: String, client: Client, batch: usize, count: usize) -> ClickHouseTask {
        ClickHouseTask { name }
    }

    pub async fn run(&mut self, logger: &mut StaticsLogger) -> crate::Result<()> {
        Ok(())
    }

    pub async fn add_batch(inserter: &mut Inserter<Value>, batch: usize) -> crate::Result<()> {
        //        for i in 1..batch {
        //            let src_ip = generate_ip();
        //            let dst_ip = generate_ip();
        //
        //            let date = get_current_date().try_into()?;
        //            let data = generate_data(src_ip, dst_ip, i, date);
        //            inserter.write(&data).await?;
        //        }
        //        inserter.commit().await?;

        Ok(())
    }
}
