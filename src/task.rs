use std::time::Duration;

use clickhouse::{
    inserter::{self, Inserter},
    Client,
};
use tokio::sync::mpsc;

use crate::{
    model::IpSessionRow,
    output::STATICS,
    shutdown::Shutdown,
    util::{generate_ip, get_current_date},
};

pub struct ClickHouseTask {
    pub shutdown: Shutdown,

    pub shutdown_complete_tx: mpsc::Sender<()>,

    pub client: Client,
    pub batch: usize,
    pub count: usize,
}

impl ClickHouseTask {
    pub fn new(
        shutdown: Shutdown,
        shutdown_complete_tx: mpsc::Sender<()>,
        client: Client,
        batch: usize,
        count: usize,
    ) -> ClickHouseTask {
        ClickHouseTask {
            shutdown,
            shutdown_complete_tx,
            client,
            batch,
            count,
        }
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        let batch = self.batch;
        while !self.shutdown.is_shutdown() {
            let mut inserter = self
                .client
                .inserter::<IpSessionRow>("dwd_standard_ip_session_all")?
                .with_timeouts(Some(Duration::from_secs(5)), Some(Duration::from_secs(20)))
                .with_max_entries(750_000)
                .with_period(Some(Duration::from_secs(15)));
            tokio::select! {
                _ = Self::add_batch(&mut inserter, batch) => {
                }
                _ = self.shutdown.recv() =>{
                    break;
                }
            }
            inserter.end().await?;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    pub async fn add_batch(
        inserter: &mut Inserter<IpSessionRow>,
        batch: usize,
    ) -> crate::Result<()> {
        for i in 1..batch {
            let src_ip = generate_ip();
            let dst_ip = generate_ip();

            let date = get_current_date().try_into()?;
            let data = generate_data(src_ip, dst_ip, i, date);
            inserter.write(&data).await?;
        }
        inserter.commit().await?;
        STATICS.add_total(batch as u64);
        STATICS.add_commit(1);
        Ok(())
    }
}

pub fn generate_data(src_ip: String, dst_ip: String, i: usize, date: u32) -> IpSessionRow {
    IpSessionRow {
        src_ip,
        dst_ip,
        device_id: 303510315976552,
        customer_id: 300810286461824,
        source_id: String::from("303510315976552_2"),
        protocol: 3342,
        protocol_ip: 700,
        src_port: vec![80],
        dst_port: vec![752],
        send_traffic: 35672,
        recv_traffic: i as u64,
        end_time: date,
        request_time: date,
        src_ip_type: String::from("inside"),
        dst_ip_type: String::from("inside"),
        src_inner_network_ip: 1,
        src_country_name: String::from("局域网"),
        src_province_name: String::new(),
        src_city_name: String::new(),
        src_ip_longitude_latitude: vec![0.0, 0.0],
        dst_inner_network_ip: 1,
        dst_country_name: String::from("局域网"),
        dst_province_name: String::new(),
        dst_city_name: String::new(),
        dst_ip_longitude_latitude: vec![0.0, 0.0],
        create_time: date,
    }
}
