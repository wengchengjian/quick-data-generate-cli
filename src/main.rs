use cli::Cli;
use clickhouse::Client;
use model::IpSessionRow;
use output::Output;
use std::{
    cell::RefCell,
    error::Error,
    io::Write,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
    vec,
};
use structopt::StructOpt;
use tokio::{
    signal,
    sync::{broadcast, mpsc, Semaphore},
};

use crate::output::{ClickHouseOutput, STATICS};
// use tracing::{error, info, Level};
// use tracing_subscriber::FmtSubscriber;
pub mod cli;
pub mod model;
pub mod output;
pub mod shutdown;
pub mod task;
pub mod util;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    // let subscriber = FmtSubscriber::builder()
    // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
    // will be written to stdout.
    // .with_max_level(Level::TRACE)
    // builds the subscriber.
    // .finish();

    // tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let num_threads = num_cpus::get();

    let cli = Cli::from_args();
    let threats = cli.threats.unwrap_or(num_threads);

    if threats > num_threads {
        println!("threats must be less than or equal to the number of cores");
        return Ok(());
    }

    println!("pid {}, process starting... ", std::process::id());

    execute(cli, threats).await.unwrap();

    Ok(())
}

pub async fn execute(cli: Cli, concurrency: usize) -> Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);

    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let concurrency = Arc::new(Semaphore::new(concurrency));

    let mut output = ClickHouseOutput::new(
        cli,
        concurrency.clone(),
        notify_shutdown.clone(),
        shutdown_complete_tx,
    );

    tokio::select! {
        _ = STATICS.print_log() => {
            println!("\nckd exiting...");
        }
        _ = output.run() => {
            println!("\nckd exiting...");
        }
        _ = signal::ctrl_c() => {
            println!("\nctrl-c received, exiting...");
        }
    }

    let ClickHouseOutput {
        notify_shutdown,
        shutdown_complete_tx,
        ..
    } = output;

    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // 等待所有的活跃连接完成处理
    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}
