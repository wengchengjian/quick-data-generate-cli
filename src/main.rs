use cli::Cli;
use std::{error::Error, sync::Arc};
use structopt::StructOpt;
use tokio::{
    signal,
    sync::{broadcast, mpsc, Semaphore},
};
use output::Output;
use crate::{
    log::{StaticsLogFactory, StaticsLogger},
    output::clickhouse::ClickHouseOutput,
};
// use tracing::{error, info, Level};
// use tracing_subscriber::FmtSubscriber;
pub mod check;
pub mod cli;
pub mod log;
pub mod model;
pub mod output;
pub mod shutdown;
pub mod task;
pub mod util;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::from_args();

    check::check_args(&mut cli);

    println!("pid {}, process starting... ", std::process::id());

    execute(cli).await.unwrap();

    Ok(())
}

pub fn parse_output(cli: Cli) -> Vec<Box<dyn output::Output>> {
    let output_enum = cli.output;

    let (notify_shutdown, _) = broadcast::channel(1);

    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let concurrency = cli.threads.unwrap();

    let concurrency = Arc::new(Semaphore::new(concurrency));

    let output = match output_enum {
        output::OutputEnum::ClickHouse => {
            // 初始化输出源
            let mut output = ClickHouseOutput::new(
                cli,
                concurrency.clone(),
                notify_shutdown.clone(),
                shutdown_complete_tx,
            );
            output
        }
        //        output::OutputEnum::Mysql => todo!(),
        //        output::OutputEnum::Kafka => todo!(),
        //        output::OutputEnum::ElasticSearch => todo!(),
        //        output::OutputEnum::CSV => todo!(),
        //        output::OutputEnum::SqlServer => todo!(),
    };

    let res = Box::new(output);
    vec![res]
}
pub async fn execute(cli: Cli) -> Result<()> {
    let cli_args = cli.clone();
    // 初始化日志
    let log_factory = StaticsLogFactory::new();
    let interval = cli.interval.unwrap_or(1);

    let logger = StaticsLogger::new(log_factory, interval);
    // 获取所有输出任务
    let outputs: Vec<Box<dyn output::Output>> = parse_output(cli_args);

    // 创建代理输出任务
    let output = output::DelegatedOutput::new(outputs, logger);

    tokio::select! {
        _ = output.execute() => {
            println!("\nquick-data-generator is exiting...");
        }
        _ = signal::ctrl_c() => {
            println!("\nreceived stop signal, exiting...");
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
