use serde_json::json;

use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};


impl FakeDataSource {
    pub fn new(schema: DataSourceSchema, session_id: &str) -> FakeDataSource {
        let batch = schema.channel.as_ref().unwrap().batch.unwrap_or(1000);
        let concurrency = schema.channel.unwrap().concurrency.unwrap_or(1);
        FakeDataSource {
            id: session_id.to_owned(),
            name: schema.name,
            shutdown: AtomicBool::new(false),
            columns: DataSourceColumn::get_columns_from_schema(&schema.columns.unwrap_or(json!(0))),
            args: FakeArgs::new(batch, concurrency),
        }
    }
}
impl Name for FakeDataSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl TaskDetailStatic for FakeDataSource {}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FakeColumnDefine {
    pub field: String,
    pub cype: String,
    pub null: String,
    pub key: String,
    pub default: String,
    pub extra: String,
}

impl FakeArgs {
    pub fn new(batch: usize, concurrency: usize) -> FakeArgs {
        FakeArgs { batch, concurrency }
    }
}

use async_trait::async_trait;
use crate::core::limit::token::TokenBuketLimiter;
use crate::core::shutdown::Shutdown;
use crate::core::traits::{Name, TaskDetailStatic};
use crate::model::column::DataSourceColumn;
use crate::model::schema::DataSourceSchema;
use crate::task::fake::FakeTask;
use crate::task::Task;

#[async_trait]
impl super::DataSourceChannel for FakeDataSource {
    async fn get_task(
        &mut self,
        channel: ChannelContext,
        shutdown_complete_tx: mpsc::Sender<()>,
        shutdown: Shutdown,
        _count_rc: Option<Arc<AtomicI64>>,
        _limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> crate::Result<Option<Box<dyn Task>>> {
        let task = FakeTask::from_args(
            self.id(),
            self.name(),
            shutdown_complete_tx,
            shutdown,
            channel,
        );
        return Ok(Some(Box::new(task)));
    }
}

#[derive(Debug, Default)]
pub struct FakeArgs {
    pub batch: usize,

    pub concurrency: usize,
}

use super::ChannelContext;

#[derive(Debug)]
pub struct FakeDataSource {
    pub id: String,

    pub name: String,

    pub columns: Vec<DataSourceColumn>,

    pub shutdown: AtomicBool,

    pub args: FakeArgs,
}
