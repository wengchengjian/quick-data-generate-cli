use async_trait::async_trait;
use clickhouse::{inserter::Inserter, Client};
use serde_json::Value;


#[derive(Debug)]
pub struct ClickHouseTask {
    pub name: String,
}


impl ClickHouseTask {
    pub fn new(name: String, _client: Client, _batch: usize, _count: usize) -> ClickHouseTask {
        ClickHouseTask { name }
    }

    pub async fn add_batch(_inserter: &mut Inserter<Value>, _batch: usize) -> crate::Result<()> {
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
