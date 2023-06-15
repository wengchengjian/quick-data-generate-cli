use crate::core::error::{Error, IoError};

use crate::core::cli::Cli;
use crate::core::error::Result;
use crate::core::log::{StaticsLogger, STATICS_LOGGER};
use crate::core::parse::parse_output;
use output::Output;
use structopt::StructOpt;
use tokio::signal;
// use tracing::{error, info, Level};
// use tracing_subscriber::FmtSubscriber;

pub mod core;
pub mod macros;
pub mod model;
pub mod output;
pub mod task;
pub mod util;
#[macro_use]
extern crate lazy_static;

#[tokio::main]
async fn main() -> Result<()> {
    let mut cli = core::cli::Cli::from_args();

    core::check::check_args(&mut cli);

    println!("pid {}, process starting... ", std::process::id());

    if let Err(e) = execute(cli).await {
        println!("execute error: {}", e);
    }

    Ok(())
}

use output::{Close, DelegatedOutput, OutputContext};

pub fn create_context(concurrency: usize, limit: Option<usize>, skip: bool) -> OutputContext {
    return OutputContext::new(concurrency, limit, skip);
}

/// 创建代理输出任务
pub async fn create_delegate_output(cli: Cli) -> Result<(DelegatedOutput, OutputContext)> {
    let cli_args = cli.clone();
    // 获取所有输出任务
    let (outputs, interval, context) = parse_output(cli_args).expect("解析输出任务失败");

    if outputs.len() == 0 {
        return Err(Error::Io(IoError::ArgNotFound("output".to_owned())));
    }

    STATICS_LOGGER.lock().await.interval(interval);

    let output = DelegatedOutput::new(outputs);

    return Ok((output, context));
}

pub async fn execute(cli: Cli) -> Result<()> {
    let (mut output, mut context) = create_delegate_output(cli).await?;
    tokio::select! {
        res = output.execute(&mut context) => {
            if let Err(err) = res {
                println!("execute error: {}", err);
            } else {
                println!("\nquick-data-generator is exiting...");
            }
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
