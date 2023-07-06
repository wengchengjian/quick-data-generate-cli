use crate::core::parse::parse_datasource;

use crate::datasource::DATA_SOURCE_MANAGER;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::core::error::{Error, IoError};

use crate::core::cli::Cli;
use crate::core::error::Result;
use crate::core::log::{StaticsLogger, STATICS_LOGGER};
use crate::datasource::{ChannelContext, DataSourceChannel};
use datasource::{DataSourceContext, DelegatedDataSource};
use model::schema::Schema;
use structopt::StructOpt;

use tokio::signal;
use tokio::sync::RwLock;
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

pub fn create_context(limit: Option<usize>, skip: bool) -> DataSourceContext {
    return DataSourceContext::new(limit, skip);
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

    let (mut datasource, context) = create_delegate_output(cli).await?;
    let channel = ChannelContext::new(None, None);
    let context = Arc::new(RwLock::new(context));
    let context_rc = context.clone();
    datasource.execute(context_rc, channel).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut done = false;
    tokio::select! {
        _ = await_all_done() => {
            done = true;
        }
        _ = StaticsLogger::log() => {
            println!("\nlogger is exiting...");
        }
        _ = signal::ctrl_c() => {
            println!("\nreceived stop signal, exiting...");
        }
    }
    let context = context.read().await;
    let schema = Schema::new(Some(5), DATA_SOURCE_MANAGER.get_all_schema());
    //输出schema,以便修正或重复利用
    let path = output_schema_to_dir(&context.id, &schema).await;
    println!("schema文件输出至: {:?}", path);
    if !done {
        // 关闭所有任务
        DATA_SOURCE_MANAGER.stop_all_task();
        // 等待所有任务关闭
        DATA_SOURCE_MANAGER.await_all_done().await;
    }
    Ok(())
}

async fn await_all_done() {
    DATA_SOURCE_MANAGER.await_all_done().await
}

#[cfg(target_os = "windows")]
fn get_schema_file_name(id: &str) -> String {
    return format!("{}\\{}.{}", "schema", id, "json");
}

#[cfg(target_os = "linux")]
fn get_schema_file_name(id: &str) -> String {
    return format!("{}/{}.{}", "schema", id, "json");
}

async fn output_schema_to_dir(id: &str, schema: &Schema) -> PathBuf {
    let filename = get_schema_file_name(id);

    let mut path = home::home_dir().unwrap_or(PathBuf::from("./"));
    path.push(filename);

    if let Some(parent) = path.parent() {
        if !parent.exists() {
            tokio::fs::create_dir(parent).await.unwrap();
        }
    }
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
