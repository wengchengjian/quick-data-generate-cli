use async_trait::async_trait;
use mysql_async::{
    prelude::{BatchQuery, WithParams},
    Pool,
};

use tokio::sync::mpsc;

use crate::{
    core::{error::Error, fake::get_fake_data_mysql, log::incr_log, shutdown::Shutdown},
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
    async fn run(&mut self) -> crate::Result<()> {
        let (columns_name, columns_name_val) = self.get_columns_name();

        while !self.shutdown.is_shutdown() {
            tokio::select! {
                _ = self.executor.add_batch(columns_name.clone(), columns_name_val.clone()) => {

                },
                _ = self.shutdown.recv() => {
                    continue;
                }
            };
        }
        Ok(())
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
                pool, args.batch, args.count, data2, table2, columns2, name2,
            ),
        }
    }

    pub fn get_columns_name(&self) -> (String, String) {
        let mut columns_name = String::new();
        let mut columns_name_val = String::new();
        for column in &self.columns {
            columns_name.push_str(&column.name());
            columns_name.push_str(",");

            columns_name_val.push_str(format!(":{}", column.name()).as_str());
            columns_name_val.push_str(",");
        }
        columns_name.pop();
        columns_name_val.pop();
        (columns_name, columns_name_val)
    }
}

#[derive(Debug)]
pub struct MysqlTaskExecutor {
    pub database: String,
    pub table: String,
    pub pool: Pool,
    pub batch: usize,
    pub count: usize,
    pub columns: Vec<OutputColumn>,
    pub task_name: String,
}

impl MysqlTaskExecutor {
    pub fn new(
        pool: Pool,
        batch: usize,
        count: usize,
        database: String,
        table: String,
        columns: Vec<OutputColumn>,
        task_name: String,
    ) -> Self {
        Self {
            pool,
            batch,
            columns,
            count,
            database,
            table,
            task_name,
        }
    }

    pub async fn add_batch(&mut self, columns_name: String, columns_name_val: String) {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|err| Error::Other(err.into()))
            .unwrap();

        let mut params = vec![];

        for _i in 0..self.batch {
            let data = get_fake_data_mysql(&self.columns);
            params.push(data);
        }

        // let txOpts = TxOpts::default();
        // self.conn.start_transaction(txOpts).await?;
        let insert_header = format!(
            "INSERT INTO {}.{} ({}) VALUES ({})",
            self.database, self.table, columns_name, columns_name_val
        );

        if let Err(err) = insert_header.with(params).batch(&mut conn).await {
            println!("insert error: {:?}", err);
        }

        incr_log(&self.task_name, self.batch, 1).await;
    }
}
