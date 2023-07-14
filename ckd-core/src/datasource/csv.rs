use bytes::Buf;
use serde::{Deserialize, Serialize};
use serde_json::json;

use std::io::Cursor;
use std::path::PathBuf;

use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use tokio::fs::File;

use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::sync::{mpsc, Mutex, RwLock};

impl CsvDataSource {
    pub fn new(schema: DataSourceSchema, session_id: &str) -> CsvDataSource {
        let _batch = schema.channel.as_ref().unwrap().batch.unwrap_or(1000);
        let _concurrency = schema.channel.unwrap().concurrency.unwrap_or(1);
        CsvDataSource {
            id: session_id.to_owned(),
            name: schema.name,
        }
    }

    pub async fn get_columns_names(&self) -> crate::Result<String> {
        let mut columns_name = String::new();
        for column in &self.columns().await? {
            columns_name.push_str(&column.name());
            columns_name.push_str(",");
        }
        columns_name.pop();
        Ok(columns_name)
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
    pub fn from_value(
        meta: Option<Json>,
        channel: Option<ChannelSchema>,
    ) -> crate::Result<CsvArgs> {
        let meta = meta.unwrap_or(json!({}));

        let channel = channel.unwrap_or(ChannelSchema::default());

        Ok(CsvArgs {
            filename: meta["filename"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("database")))?
                .to_string(),

            batch: channel.batch.unwrap_or(1000),
            count: channel.count.unwrap_or(isize::MAX),
            concurrency: channel.concurrency.unwrap_or(1),
        })
    }
}

impl TryFrom<DataSourceSchema> for CsvDataSource {
    type Error = Error;

    fn try_from(value: DataSourceSchema) -> std::result::Result<Self, Self::Error> {
        Ok(CsvDataSource {
            id: Uuid::new_v4().to_string(),
            name: value.name,
        })
    }
}

use async_trait::async_trait;

impl Name for CsvDataSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl TaskDetailStatic for CsvDataSource {}

#[async_trait]
impl super::DataSourceChannel for CsvDataSource {
    async fn before_run(
        &mut self,
        _context: Arc<RwLock<DataSourceContext>>,
        _channel: ChannelContext,
    ) -> crate::Result<()> {
        //注册日志
        register(&self.id().clone()).await;
        // 创建csv文件
        let path = PathBuf::from(&self.meta("filename").await?.as_str().unwrap());

        match path.parent() {
            Some(parent) => {
                if !parent.exists() {
                    tokio::fs::create_dir_all(parent).await?;
                    tokio::fs::File::create(&path).await?;
                }

                let file = File::create(path).await?;
                let mut writer = BufWriter::new(file);
                let header = self.get_columns_names().await?;
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

    async fn get_task(
        &mut self,
        _channel: ChannelContext,
        shutdown_complete_tx: mpsc::Sender<()>,
        shutdown: Shutdown,
        count_rc: Option<Arc<AtomicI64>>,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> crate::Result<Option<Box<dyn Task>>> {
        let task = CsvTask::from_args(
            self.id(),
            self.name(),
            shutdown_complete_tx,
            shutdown,
            limiter,
            count_rc,
        );
        return Ok(Some(Box::new(task)));
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CsvArgs {
    pub filename: String,

    pub batch: usize,

    pub count: isize,

    pub concurrency: usize,
}
use super::{ChannelContext, DataSourceContext};
use crate::core::error::{Error, IoError};
use crate::core::limit::token::TokenBuketLimiter;
use crate::core::log::register;
use crate::core::shutdown::Shutdown;
use crate::core::traits::{Name, TaskDetailStatic};
use crate::model::schema::{ChannelSchema, DataSourceSchema};
use crate::task::csv::CsvTask;
use crate::task::Task;
use crate::Json;
use uuid::Uuid;

#[derive(Debug)]
pub struct CsvDataSource {
    pub id: String,

    pub name: String,
}
