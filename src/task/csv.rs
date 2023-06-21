use std::sync::{atomic::AtomicI64, Arc};

use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex};

use crate::{
    core::{limit::token::TokenBuketLimiter, shutdown::Shutdown},
    exec::{csv::CsvTaskExecutor, Exector},
    model::column::OutputColumn,
    output::{csv::CsvArgs, Close},
};

use super::Task;

#[derive(Debug)]
pub struct CsvTask {
    pub name: String,
    pub batch: usize,
    pub count: usize,
    pub shutdown_sender: mpsc::Sender<()>,
    pub shutdown: Shutdown,
    pub columns: Vec<OutputColumn>,
    pub executor: CsvTaskExecutor,
}
#[async_trait]
impl Close for CsvTask {
    async fn close(&mut self) -> crate::Result<()> {
        // 判断任务是否完成
        Ok(())
    }
}

#[async_trait]
impl Task for CsvTask {
    fn shutdown(&mut self) -> &mut Shutdown {
        return &mut self.shutdown;
    }

    fn executor(&self) -> Box<dyn Exector> {
        return Box::new(self.executor.clone());
    }
}

impl CsvTask {
    pub fn from_args(
        name: String,
        args: &CsvArgs,
        columns: Vec<OutputColumn>,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
        count_rc: Option<Arc<AtomicI64>>,
    ) -> Self {
        let columns2 = columns.clone();
        let name2 = name.clone();
        Self {
            name,
            batch: args.batch,
            count: args.count,
            shutdown_sender,
            shutdown,
            columns,
            executor: CsvTaskExecutor::new(
                args.filename.clone(),
                args.batch,
                count_rc,
                columns2,
                name2,
                limiter,
            ),
        }
    }
}
