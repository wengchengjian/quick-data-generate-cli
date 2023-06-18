use std::path::PathBuf;

use crate::core::error::{Error, IoError};

use crate::core::cli::Cli;
use crate::core::error::Result;
use crate::core::log::{StaticsLogger, STATICS_LOGGER};
use crate::core::parse::parse_output;
use model::schema::Schema;
use output::Output;
use structopt::StructOpt;
use tokio::signal;
// use tracing::{error, info, Level};
// use tracing_subscriber::FmtSubscriber;

pub mod core;
pub mod exec;
pub mod macros;
pub mod model;
pub mod output;
pub mod task;
pub mod util;
#[macro_use]
extern crate lazy_static;

#[tokio::main(flavor = "multi_thread")]
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

pub fn create_context(
    concurrency: usize,
    limit: Option<usize>,
    skip: bool,
    schema: Schema,
) -> OutputContext {
    return OutputContext::new(concurrency, limit, skip, schema);
}

/// 创建代理输出任务
pub async fn create_delegate_output(cli: Cli) -> Result<(DelegatedOutput, OutputContext)> {
    let cli_args = cli.clone();
    // 获取所有输出任务
    let (outputs, interval, context) = parse_output(cli_args).expect("解析输出任务失败");

    if outputs.len() == 0 {
        return Err(Error::Io(IoError::ArgNotFound("output".to_owned())));
    }

    unsafe {
        STATICS_LOGGER.as_mut().unwrap().interval(interval);
    }

    let output = DelegatedOutput::new(outputs);

    return Ok((output, context));
}

pub async fn execute(cli: Cli) -> Result<()> {
    unsafe {
        STATICS_LOGGER = Some(StaticsLogger::new(0));
    }

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
    //输出schema,以便修正或重复利用
    let path = output_schema_to_dir(&context.id, &context.schema).await;
    println!("schema文件输出至: {:?}", path);
    // 关闭任务
    output.close().await?;

    Ok(())
}

async fn output_schema_to_dir(id: &str, schema: &Schema) -> PathBuf {

    let filename = format!("{}.{}", id, "json");

    let mut path = home::home_dir().unwrap_or(PathBuf::from("./"));


    path.push(filename);

    let ab_path = tokio::fs::canonicalize(&path).await.unwrap();

    match serde_json::to_string_pretty(schema) {
        Ok(content) => {
            match tokio::fs::write(path, content).await {
                Ok(_) => {
                    // nothing
                }
                Err(e) => {
                    println!("写入schema文件失败:{}", e);
                }
            };
        }
        Err(e) => {
            println!("写入schema文件失败:{}", e);
        }
    };
    return ab_path;
}