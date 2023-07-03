use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use crate::core::error::Result;
use crate::core::limit::token::TokenBuketLimiter;
use crate::core::parse::DEFAULT_FAKE_DATASOURCE;
use crate::core::shutdown::Shutdown;
use crate::model::column::DataSourceColumn;
use crate::model::schema::{ChannelSchema, DataSourceSchema};
use crate::task::fake::FakeTask;
use crate::task::Task;

impl FakeDataSource {
    pub fn new(schema: DataSourceSchema) -> FakeDataSource {
        let batch = schema.channel.as_ref().unwrap().batch.unwrap_or(1000);
        let concurrency = schema.channel.unwrap().concurrency.unwrap_or(1);
        FakeDataSource {
            name: DEFAULT_FAKE_DATASOURCE.to_owned(),
            shutdown: AtomicBool::new(false),
            columns: DataSourceColumn::get_columns_from_schema(&schema.columns.unwrap_or(json!(0))),
            args: FakeArgs::new(batch, concurrency),
        }
    }
}

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

#[async_trait]
impl super::DataSourceChannel for FakeDataSource {
    fn source_type(&self) -> Option<DataSourceEnum> {
        return Some(DataSourceEnum::Fake);
    }

    fn batch(&self) -> Option<usize> {
        return Some(self.args.batch);
    }

    fn channel_schema(&self) -> Option<ChannelSchema> {
        return Some(ChannelSchema {
            batch: Some(self.args.batch),
            concurrency: Some(self.args.concurrency),
            count: Some(usize::MAX),
        });
    }

    fn columns(&self) -> Option<&Vec<DataSourceColumn>> {
        return Some(&self.columns);
    }

    fn concurrency(&self) -> usize {
        return self.args.concurrency;
    }

    fn name(&self) -> &str {
        return &self.name;
    }

    fn get_task(
        &mut self,
        channel: ChannelContext,
        columns: Vec<DataSourceColumn>,
        shutdown_complete_tx: mpsc::Sender<()>,
        shutdown: Shutdown,
        _count_rc: Option<Arc<AtomicI64>>,
        _limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> Option<Box<dyn Task>> {
        let task = FakeTask::from_args(
            self.name.clone(),
            &self.args,
            columns,
            shutdown_complete_tx,
            shutdown,
            channel,
        );
        return Some(Box::new(task));
    }
}

#[async_trait]
impl Close for FakeDataSource {
    async fn close(&mut self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct FakeArgs {
    pub batch: usize,

    pub concurrency: usize,
}

use super::{ChannelContext, Close, DataSourceEnum};

#[derive(Debug)]
pub struct FakeDataSource {
    pub name: String,

    pub columns: Vec<DataSourceColumn>,

    pub shutdown: AtomicBool,

    pub args: FakeArgs,
}
