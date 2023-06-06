use async_trait::async_trait;
use mysql_async::{prelude::*, Conn, Params, TxOpts};
use serde_json::{json, Value};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::sync::{mpsc, Mutex};

use crate::{
    column::{DataTypeEnum, OutputColumn},
    fake::get_fake_data,
    log::{ChannelStaticsLog, StaticsLogger},
    output::Close,
    shutdown::{self, Shutdown},
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
    pub sender: mpsc::Sender<ChannelStaticsLog>,
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
        sender: mpsc::Sender<ChannelStaticsLog>,
        shutdown: Shutdown,
    ) -> MysqlTask {
        let data2 = database.clone();
        let table2: String = table.clone();
        let columns2 = columns.clone();
        MysqlTask {
            name,
            batch,
            count,
            shutdown_sender,
            completed: Mutex::new(()),
            shutdown,
            columns,
            table,
            database,
            sender,
            executor: MysqlTaskExecutor::new(conn, batch, count, data2, table2, columns2),
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
        let (columns_name, columns_name_val) = self.get_columns_name();

        while !self.shutdown.is_shutdown() {
            let log = tokio::select! {
                log = self.executor.add_batch(columns_name.clone(), columns_name_val.clone()) => log,
                _ = self.shutdown.recv() => {
                    break;
                }
            };

            self.sender.send(log).await?;

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
}

impl MysqlTaskExecutor {
    pub fn new(
        conn: Conn,
        batch: usize,
        count: usize,
        database: String,
        table: String,
        columns: Vec<OutputColumn>,
    ) -> Self {
        Self {
            conn,
            batch,
            columns,
            count,
            database,
            table,
        }
    }

    pub async fn add_batch(
        &mut self,
        columns_name: String,
        columns_name_val: String,
    ) -> ChannelStaticsLog {
        let mut log = ChannelStaticsLog::new(0, 0);

        let mut params = vec![];

        for i in 0..self.batch {
            let data = get_fake_data(&self.columns);
            params.push(data);
        }

        let params = Params::from(params);

        // let txOpts = TxOpts::default();
        // self.conn.start_transaction(txOpts).await?;
        let insert_header = format!(
            "INSERT INTO {}.{} ({}) VALUES ({})",
            self.database, self.table, columns_name, columns_name_val
        );
        insert_header.with(params).run(&mut self.conn).await;
        log.total = self.batch;
        log.commit = 1;

        log
    }
}
