use crate::{
    log::{StaticsLogFactory, StaticsLogger},
    output::{clickhouse::ClickHouseOutput, mysql::MysqlOutput},
};
use cli::Cli;
use log::STATICS_LOGGER;
use output::Output;
use std::error::Error;
use structopt::StructOpt;
use tokio::signal;
// use tracing::{error, info, Level};
// use tracing_subscriber::FmtSubscriber;
pub mod check;
pub mod cli;
pub mod column;
pub mod fake;
pub mod log;
pub mod model;
pub mod output;
pub mod shutdown;
pub mod task;
pub mod util;

#[macro_use]
extern crate lazy_static;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    let mut cli = Cli::from_args();

    check::check_args(&mut cli);

    println!("pid {}, process starting... ", std::process::id());

    execute(cli).await.unwrap();

    Ok(())
}

pub fn parse_output(cli: Cli) -> Vec<Box<dyn output::Output>> {
    let output_enum = cli.output;

    let output = match output_enum {
        // output::OutputEnum::ClickHouse => {
        //     // 初始化输出源
        //     let mut output = ClickHouseOutput::new(cli);
        //     output
        // }
        output::OutputEnum::Mysql => MysqlOutput::new(cli),
        //        output::OutputEnum::Kafka => todo!(),
        //        output::OutputEnum::ElasticSearch => todo!(),
        //        output::OutputEnum::CSV => todo!(),
        //        output::OutputEnum::SqlServer => todo!(),
    };

    let res = Box::new(output);
    vec![res]
}

use output::{Close, DelegatedOutput, OutputContext};

pub fn create_context(cli: &Cli) -> OutputContext {
    let concurrency = cli.concurrency.unwrap();

    return OutputContext::new(concurrency);
}

/// 创建代理输出任务
pub async fn create_delegate_output(cli: Cli) -> (DelegatedOutput, OutputContext) {
    let cli_args = cli.clone();
    // 初始化日志
    let interval = cli.interval.unwrap_or(5);

    STATICS_LOGGER.lock().await.interval(interval);
    // 获取所有输出任务
    let outputs: Vec<Box<dyn output::Output>> = parse_output(cli_args);

    let context = create_context(&cli);

    let output = DelegatedOutput::new(outputs, interval);

    return (output, context);
}

pub async fn execute(cli: Cli) -> Result<()> {
    let (mut output, mut context) = create_delegate_output(cli).await;
    tokio::select! {
        _ = output.execute(&mut context) => {
            println!("\nquick-data-generator is exiting...");
        }
        _ = StaticsLogger::log() => {
            println!("\nlogger is exiting...");
        }
        _ = signal::ctrl_c() => {
            println!("\nreceived stop signal, exiting...");
        }
    }
    // 关闭任务
    output.close().await?;

    Ok(())
}
