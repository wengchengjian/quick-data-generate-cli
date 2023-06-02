use std::{
    error::Error,
    future::Future,
    io::Write,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use clickhouse::Client;
use structopt::lazy_static::lazy::Lazy;
use tokio::{
    io::AsyncWriteExt,
    sync::{broadcast, mpsc, Semaphore},
};
// use tracing::{error, info};

use crate::{
    cli::Cli,
    model::IpSessionRow,
    shutdown,
    task::ClickHouseTask,
    util::{generate_ip, get_current_date, get_system_usage},
};

pub trait Output {}

#[derive(Debug)]
pub enum OutputEnum {
    ClickHouse,

    Mysql,

    Kafka,

    ElasticSearch,

    CSV,

    SqlServer,
}

impl FromStr for OutputEnum {
    type Err = Box<dyn std::error::Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "clickhouse" => Ok(OutputEnum::ClickHouse),
            "mysql" => Ok(OutputEnum::Mysql),
            "kafka" => Ok(OutputEnum::Kafka),
            "elasticsearch" => Ok(OutputEnum::ElasticSearch),
            "csv" => Ok(OutputEnum::CSV),
            "sqlserver" => Ok(OutputEnum::SqlServer),
            _ => Err("不支持该输出源".into()),
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

pub struct ClickHouseOutput {
    pub concurrency: Arc<Semaphore>,

    pub notify_shutdown: broadcast::Sender<()>,

    pub shutdown_complete_tx: mpsc::Sender<()>,

    pub args: ClickHouseArgs,
}
use lazy_static::lazy_static;

lazy_static! {
    pub static ref STATICS: OutputStatics = OutputStatics::new();
}

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

    pub async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        let client = self.connect();

        let batch = self.args.batch;
        let count = self.args.count;

        loop {
            let semaphore: Arc<Semaphore> = self.concurrency.clone();
            let permit = semaphore.acquire_owned().await.unwrap();

            let client = client.clone();

            let shutdown = shutdown::Shutdown::new(self.notify_shutdown.subscribe());
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

impl Output for ClickHouseOutput {}

#[derive(Debug)]
pub struct OutputStatics {
    total: AtomicU64,

    commit: AtomicU64,

    start: AtomicU64,

    print: usize,
}

impl OutputStatics {
    pub fn new() -> Self {
        Self {
            total: AtomicU64::new(0),
            commit: AtomicU64::new(0),
            start: AtomicU64::new(get_current_date()),
            print: 5,
        }
    }

    pub async fn print_log(&self) {
        let print = self.print;
        loop {
            tokio::spawn(async move {
                let total = STATICS.total();
                let commit = STATICS.commit();
                let start = STATICS.start();
                let now = get_current_date();
                let time = now - start;
                let tps = match total {
                    0 => 0,
                    _ => total / time,
                };

                let (memory_usage, cpu_percent) = get_system_usage().await;

                print!(
                    "\rTotal: {}, Commit: {}, TPS: {}, Memory Usage: {:.2} KB, Cpu Usage: {:.2}%",
                    total, commit, tps, memory_usage, cpu_percent
                );
                std::io::stdout().flush().unwrap();
                tokio::time::sleep(Duration::from_secs(print as u64)).await;
            })
            .await
            .unwrap();
        }
    }

    pub fn total(&self) -> u64 {
        self.total.load(Ordering::SeqCst)
    }

    pub fn commit(&self) -> u64 {
        self.commit.load(Ordering::SeqCst)
    }

    pub fn start(&self) -> u64 {
        self.start.load(Ordering::SeqCst)
    }

    pub fn add_total(&self, val: u64) {
        self.total.fetch_add(val, Ordering::SeqCst);
    }

    pub fn add_commit(&self, val: u64) {
        self.commit.fetch_add(val, Ordering::SeqCst);
    }
}
