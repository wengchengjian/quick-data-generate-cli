use std::sync::{atomic::AtomicI64, Arc};

use async_trait::async_trait;
use mysql_async::Pool;

use tokio::sync::{mpsc, Mutex};

use crate::{
    core::{limit::token::TokenBuketLimiter, shutdown::Shutdown},
    exec::{mysql::MysqlTaskExecutor, Exector},
    model::column::OutputColumn,
    output::{mysql::MysqlArgs, Close},
};

use super::Task;

#[derive(Debug)]
pub struct MysqlTask {
    pub name: String,
    pub database: String,
    pub table: String,
    pub batch: usize,
    pub count: usize,
    pub shutdown_sender: mpsc::Sender<()>,
    pub shutdown: Shutdown,
    pub columns: Vec<OutputColumn>,
    pub executor: MysqlTaskExecutor,
}
#[async_trait]
impl Close for MysqlTask {
    async fn close(&mut self) -> crate::Result<()> {
        // 判断任务是否完成
        Ok(())
    }
}

#[async_trait]
impl Task for MysqlTask {
    fn shutdown(&mut self) -> &mut Shutdown {
        return &mut self.shutdown;
    }

    fn executor(&self) -> Box<dyn Exector> {
        return Box::new(self.executor.clone());
    }
}

impl MysqlTask {
    pub fn from_args(
        name: String,
        args: &MysqlArgs,
        pool: Pool,
        columns: Vec<OutputColumn>,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
        count_rc: Option<Arc<AtomicI64>>,
    ) -> Self {
        let data2 = args.database.clone();
        let table2: String = args.table.clone();
        let columns2 = columns.clone();
        let name2 = name.clone();
        Self {
            name,
            database: args.database.clone(),
            table: args.table.clone(),
            batch: args.batch,
            count: args.count,
            shutdown_sender,
            shutdown,
            columns,
            executor: MysqlTaskExecutor::new(
                pool, args.batch, count_rc, data2, table2, columns2, name2, limiter,
            ),
        }
    }
}
