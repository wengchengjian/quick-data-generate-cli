use std::sync::Arc;

use clickhouse::Client;
use tokio::sync::{broadcast, mpsc, Semaphore};

use crate::{cli::Cli, shutdown::Shutdown, task::ClickHouseTask};

impl ClickHouseOutput {
    pub fn new(
        cli: Cli,

        concurrency: Arc<Semaphore>,

        notify_shutdown: broadcast::Sender<()>,

        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            args: cli.into(),
            concurrency,
            notify_shutdown,
            shutdown_complete_tx,
        }
    }

    pub fn connect(&self) -> Client {
        let url = format!("http://{}:{}", self.args.host, self.args.port);
        Client::default()
            .with_url(url)
            .with_user(self.args.user.clone())
            .with_password(self.args.password.clone())
            .with_database("ws_dwd")
    }
}

impl Into<ClickHouseArgs> for Cli {
    fn into(self) -> ClickHouseArgs {
        ClickHouseArgs {
            host: self.host.unwrap_or("192.168.180.217".to_string()),
            port: self.port.unwrap_or(8123),
            user: self.user.unwrap_or("default".to_string()),
            password: self.password.unwrap_or("!default@123".to_string()),
            database: self.database.unwrap_or("ws_dwd".to_string()),

            table: self
                .table
                .unwrap_or("dwd_standard_ip_session_all".to_string()),
            batch: self.batch.unwrap_or(50000),
            count: self.count.unwrap_or(0),
        }
    }
}

use async_trait::async_trait;

#[async_trait]
impl super::Output for ClickHouseOutput {
    fn name(&self) -> String {
        todo!()
    }

    fn interval(&self) -> usize {
        todo!()
    }

    async fn run(&mut self) -> crate::Result<()> {
        let client = self.connect();

        let batch = self.args.batch;
        let count = self.args.count;

        loop {
            let semaphore: Arc<Semaphore> = self.concurrency.clone();
            let permit = semaphore.acquire_owned().await.unwrap();

            let client = client.clone();

            let shutdown = Shutdown::new(self.notify_shutdown.subscribe());
            let shutdown_complete_tx = self.shutdown_complete_tx.clone();

            let mut task =
                ClickHouseTask::new(shutdown, shutdown_complete_tx, client, batch, count);

            tokio::spawn(async move {
                if let Err(err) = task.run().await {
                    println!("task run error: {}", err);
                }

                drop(permit)
            });
        }
    }
}

#[derive(Debug)]
pub struct ClickHouseArgs {
    pub host: String,

    pub port: u16,

    pub user: String,

    pub password: String,

    pub database: String,

    pub table: String,

    pub batch: usize,

    pub count: usize,
}

#[derive(Debug)]
pub struct ClickHouseOutput {
    pub concurrency: Arc<Semaphore>,

    pub notify_shutdown: broadcast::Sender<()>,

    pub shutdown_complete_tx: mpsc::Sender<()>,

    pub args: ClickHouseArgs,
}
