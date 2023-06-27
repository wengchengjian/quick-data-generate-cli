use crate::core::parse::parse_datasource;

use crate::core::parse::parse_mpsc_from_schema;
use std::path::PathBuf;

use crate::core::error::{Error, IoError};

use crate::core::cli::Cli;
use crate::core::error::Result;
use crate::core::log::{StaticsLogger, STATICS_LOGGER};
use crate::datasource::{ChannelContext, Close, DataSourceChannel};
use datasource::{DataSourceContext, DelegatedDataSource};
use model::schema::Schema;
use structopt::StructOpt;

use tokio::signal;
// use tracing::{error, info, Level};
// use tracing_subscriber::FmtSubscriber;
pub mod core;
pub mod datasource;
pub mod exec;
pub mod macros;
pub mod model;
pub mod task;
pub mod util;
#[macro_use]
extern crate lazy_static;
pub type Json = serde_json::Value;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut cli = core::cli::Cli::from_args();

    core::check::check_args(&mut cli);

    let pid: u32 = std::process::id();

    println!("pid {}, process starting... ", pid);

    if let Err(e) = execute(cli).await {
        println!("execute error: {}", e);
    }

    Ok(())
}

pub fn create_context(limit: Option<usize>, skip: bool, schema: Schema) -> DataSourceContext {
    return DataSourceContext::new(limit, skip, schema);
}

/// 创建代理输出任务
pub async fn create_delegate_output(cli: Cli) -> Result<(DelegatedDataSource, DataSourceContext)> {
    let cli_args = cli.clone();
    // 获取所有数据源
    let (datasources, interval, context) = parse_datasource(cli_args).expect("解析数据源失败");

    if datasources.len() == 0 {
        return Err(Error::Io(IoError::ParseSchemaError));
    }

    unsafe {
        STATICS_LOGGER.as_mut().unwrap().interval(interval);
    }

    let datasource = DelegatedDataSource::new(datasources);

    return Ok((datasource, context));
}

pub async fn execute(cli: Cli) -> Result<()> {
    unsafe {
        STATICS_LOGGER = Some(StaticsLogger::new(0));
    }

    let (mut datasource, mut context) = create_delegate_output(cli).await?;
    let channel = ChannelContext::new(None, None);
    tokio::select! {
        result = datasource.execute(&mut context, channel) => {
            if let Err(err) = result {
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
    datasource.close().await?;

    Ok(())
}

async fn output_schema_to_dir(id: &str, schema: &Schema) -> PathBuf {
    let filename = format!("/{}/{}.{}", "schema", id, "json");

    let mut path = home::home_dir().unwrap_or(PathBuf::from("./"));

    path.push(filename);

    let as_path = path.clone();

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
    let ab_path = tokio::fs::canonicalize(&as_path).await.unwrap();

    return ab_path;
}
