use crate::core::cli::Cli;
use crate::core::error::{Error, IoError, Result};
use crate::core::limit::token::TokenBuketLimiter;
use crate::core::log::register;
use crate::core::shutdown::Shutdown;
use crate::model::column::{DataTypeEnum, OutputColumn};
use crate::model::schema::{ChannelSchema, OutputSchema};
use crate::task::csv::CsvTask;
use crate::task::Task;
use bytes::Buf;
use serde_json::json;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Arc;
use std::vec;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::{BufReader, BufWriter};
use tokio::sync::{mpsc, Mutex};

impl CsvOutput {
    pub(crate) fn from_cli(cli: Cli) -> Result<Box<dyn Output>> {
        let res = CsvOutput {
            name: "Csv".into(),
            args: cli.try_into()?,
            shutdown: AtomicBool::new(false),
            columns: vec![],
        };

        Ok(Box::new(res))
    }

    pub fn get_columns_names(&self) -> String {
        let mut columns_name = String::new();
        for column in &self.columns {
            columns_name.push_str(&column.name());
            columns_name.push_str(",");
        }
        columns_name.pop();
        columns_name
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CsvColumnDefine {
    pub field: String,
    pub cype: String,
    pub null: String,
    pub key: String,
    pub default: String,
    pub extra: String,
}

impl CsvArgs {
    pub fn from_value(meta: serde_json::Value, channel: ChannelSchema) -> Result<CsvArgs> {
        Ok(CsvArgs {
            filename: meta["filename"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("database".to_string())))?
                .to_string(),

            batch: channel.batch,
            count: channel.count,
            concurrency: channel.concurrency,
        })
    }
}

impl TryInto<CsvArgs> for Cli {
    type Error = Error;

    fn try_into(self) -> std::result::Result<CsvArgs, Self::Error> {
        Ok(CsvArgs {
            filename: self.filename.unwrap_or("default.csv".to_owned()),
            batch: self.batch.unwrap_or(1000),
            count: self.count.unwrap_or(0),
            concurrency: self.concurrency.unwrap_or(1),
        })
    }
}

impl TryFrom<OutputSchema> for CsvOutput {
    type Error = Error;

    fn try_from(value: OutputSchema) -> std::result::Result<Self, Self::Error> {
        Ok(CsvOutput {
            name: "默认Csv输出".to_string(),
            args: CsvArgs::from_value(value.meta, value.channel)?,
            shutdown: AtomicBool::new(false),
            columns: OutputColumn::get_columns_from_schema(&value.columns),
        })
    }
}

use async_trait::async_trait;

#[async_trait]
impl super::Output for CsvOutput {
    async fn before_run(&mut self, _context: &mut OutputContext) -> crate::Result<()> {
        //注册日志
        register(&self.name().clone()).await;
        // 创建csv文件
        let path = PathBuf::from(&self.args.filename);

        match path.parent() {
            Some(parent) => {
                if !parent.exists() {
                    tokio::fs::create_dir_all(parent).await?;
                    tokio::fs::File::create(&path).await?;
                }

                let file = File::create(path).await?;
                let mut writer = BufWriter::new(file);
                let header = self.get_columns_names();
                let mut buffer = Cursor::new(header.as_str());
                while buffer.has_remaining() {
                    writer.write_buf(&mut buffer).await?;
                }
                writer.write_u8(b'\n').await?;
                writer.flush().await?;
            }
            None => {}
        }
        Ok(())
    }

    fn columns_mut(&mut self, columns: Vec<OutputColumn>) {
        self.columns = columns;
    }

    fn output_type(&self) -> Option<OutputEnum> {
        return Some(OutputEnum::Csv);
    }

    fn batch(&self) -> Option<usize> {
        return Some(self.args.batch);
    }

    fn meta(&self) -> Option<serde_json::Value> {
        return Some(json!({
            "filename": self.args.filename
        }));
    }

    fn channel_schema(&self) -> Option<ChannelSchema> {
        return Some(ChannelSchema {
            batch: self.args.batch,
            concurrency: self.args.concurrency,
            count: self.args.count,
        });
    }

    fn columns(&self) -> Option<&Vec<OutputColumn>> {
        return Some(&self.columns);
    }

    fn concurrency(&self) -> usize {
        return self.args.concurrency;
    }

    fn name(&self) -> &str {
        return &self.name;
    }

    fn count(&self) -> Option<usize> {
        match self.args.count {
            0 => None,
            x => Some(x),
        }
    }

    async fn get_columns_define(&mut self) -> Option<Vec<OutputColumn>> {
        return None;
    }

    fn get_output_task(
        &mut self,
        columns: Vec<OutputColumn>,
        shutdown_complete_tx: mpsc::Sender<()>,
        shutdown: Shutdown,
        count_rc: Option<Arc<AtomicI64>>,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> Option<Box<dyn Task>> {
        let task = CsvTask::from_args(
            self.name.clone(),
            &self.args,
            columns,
            shutdown_complete_tx,
            shutdown,
            limiter,
            count_rc,
        );
        return Some(Box::new(task));
    }

    fn is_shutdown(&self) -> bool {
        return self.shutdown.load(Ordering::SeqCst);
    }
}

#[async_trait]
impl Close for CsvOutput {
    async fn close(&mut self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct CsvArgs {
    pub filename: String,

    pub batch: usize,

    pub count: usize,

    pub concurrency: usize,
}

use super::{Close, Output, OutputContext, OutputEnum};

#[derive(Debug)]
pub struct CsvOutput {
    pub name: String,

    pub args: CsvArgs,

    pub columns: Vec<OutputColumn>,

    pub shutdown: AtomicBool,
}

impl TryFrom<Cli> for Box<CsvOutput> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: Cli) -> std::result::Result<Self, Self::Error> {
        let res = CsvOutput {
            name: "Csv".into(),
            args: value.try_into()?,
            shutdown: AtomicBool::new(false),
            columns: vec![],
        };

        Ok(Box::new(res))
    }
}
