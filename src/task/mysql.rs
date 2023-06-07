use async_trait::async_trait;
use mysql_async::{prelude::*, Conn, Params, Value};
use std::{collections::HashMap, time::Duration};
use tokio::sync::{mpsc, Mutex};

use crate::{
    column::OutputColumn,
    fake::{get_fake_data, get_fake_data_mysql},
    log::{incr_log, register},
    output::Close,
    shutdown::Shutdown,
};

#[derive(Debug)]
pub struct MysqlTask {
    pub name: String,
    pub database: String,
    pub table: String,
    pub batch: usize,
    pub count: usize,
    pub completed: Mutex<()>,
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
impl MysqlTask {
    pub fn new(
        name: String,
        conn: Conn,
        batch: usize,
        count: usize,
        database: String,
        table: String,
        columns: Vec<OutputColumn>,
        shutdown_sender: mpsc::Sender<()>,
        shutdown: Shutdown,
    ) -> MysqlTask {
        let data2 = database.clone();
        let table2: String = table.clone();
        let columns2 = columns.clone();
        let name2 = name.clone();
        MysqlTask {
            name,
            batch: 2000,
            count,
            shutdown_sender,
            completed: Mutex::new(()),
            shutdown,
            columns,
            table,
            database,
            executor: MysqlTaskExecutor::new(conn, 10, count, data2, table2, columns2, name2),
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

    pub async fn run(&mut self) -> crate::Result<()> {
        register(&self.name.clone()).await;
        println!("{} will running...", self.name);
        let (columns_name, columns_name_val) = self.get_columns_name();

        while !self.shutdown.is_shutdown() {
            tokio::select! {
                _ = self.executor.add_batch(columns_name.clone(), columns_name_val.clone()) => {

                },
                _ = self.shutdown.recv() => {
                    continue;
                }
            };

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MysqlTaskExecutor {
    pub database: String,
    pub table: String,
    pub conn: Conn,
    pub batch: usize,
    pub count: usize,
    pub columns: Vec<OutputColumn>,
    pub task_name: String,
}

impl MysqlTaskExecutor {
    pub fn new(
        conn: Conn,
        batch: usize,
        count: usize,
        database: String,
        table: String,
        columns: Vec<OutputColumn>,
        task_name: String,
    ) -> Self {
        Self {
            conn,
            batch,
            columns,
            count,
            database,
            table,
            task_name,
        }
    }

    pub async fn add_batch(&mut self, columns_name: String, columns_name_val: String) {
        let mut params = vec![];

        for i in 0..self.batch {
            let data = get_fake_data_mysql(&self.columns);
            params.push(data);
        }

        // let txOpts = TxOpts::default();
        // self.conn.start_transaction(txOpts).await?;
        let insert_header = format!(
            "INSERT INTO {}.{} ({}) VALUES ({})",
            self.database, self.table, columns_name, columns_name_val
        );
        
        if let Err(err) = insert_header
        .with(params)
        .batch(&mut self.conn)
        .await  {
            println!("insert error: {:?}", err);
        }

        incr_log(&self.task_name, self.batch, 1).await;
    }
}
