use async_trait::async_trait;
use bloom::BloomFilter;
use core::fmt::Debug;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};
use tokio::sync::{broadcast, mpsc, Mutex, Semaphore};

use crate::{
    core::{
        fake::get_random_uuid, limit::token::TokenBuketLimiter, log::register, shutdown::Shutdown,
    },
    model::{
        column::OutputColumn,
        schema::{ChannelSchema, OutputSchema, Schema},
    },
    task::Task,
};

pub mod clickhouse;
pub mod csv;
pub mod elasticsearch;
pub mod kafka;
pub mod mysql;



#[async_trait]
pub trait Output: Send + Close + Sync + Debug {
    /// 通用初始化逻辑
    fn init(&mut self, _context: &mut OutputContext) {}

    async fn before_run(&mut self, _context: &mut OutputContext) -> crate::Result<()> {
        //注册日志
        if !self.name().eq("delegate") {
            register(&self.name().clone()).await;
        }
        Ok(())
    }

    async fn after_run(&mut self, _context: &mut OutputContext) -> crate::Result<()> {
        Ok(())
    }

    async fn execute(&mut self, context: &mut OutputContext) -> crate::Result<()> {
        self.before_run(context).await?;
        self.init(context);
        self.run(context).await?;
        self.after_run(context).await
    }

    fn output_type(&self) -> Option<SourceEnum> {
        return None;
    }

    fn batch(&self) -> Option<usize> {
        return None;
    }

    fn meta(&self) -> Option<serde_json::Value> {
        return None;
    }

    fn channel_schema(&self) -> Option<ChannelSchema> {
        return None;
    }

    fn transfer_to_schema(&self) -> Option<OutputSchema> {
        match self.channel_schema() {
            Some(channel_schema) => Some(OutputSchema {
                output: match self.output_type() {
                    Some(output_type) => output_type,
                    None => return None,
                },
                meta: match self.meta() {
                    Some(meta) => meta,
                    None => return None,
                },
                columns: match self.columns() {
                    Some(columns) => {
                        let mut res = json!({});
                        for column in columns {
                            res[&column.name] = json!(column.data_type().to_string());
                        }
                        res
                    }
                    None => return None,
                },
                channel: channel_schema,
            }),
            None => None,
        }
    }

    fn count(&self) -> Option<usize> {
        return None;
    }

    fn concurrency(&self) -> usize {
        return 1;
    }

    fn name(&self) -> &str;

    fn columns(&self) -> Option<&Vec<OutputColumn>> {
        return None;
    }

    fn columns_mut(&mut self, _columns: Vec<OutputColumn>) {}


    async fn get_columns_define(&mut self) -> Option<Vec<OutputColumn>> {
        return None;
    }

    fn get_output_task(
        &mut self,
        _columns: Vec<OutputColumn>,
        _shutdown_complete_tx: mpsc::Sender<()>,
        _shutdown: Shutdown,
        _count_rc: Option<Arc<AtomicI64>>,
        _limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> Option<Box<dyn Task>> {
        return None;
    }

    fn is_shutdown(&self) -> bool {
        return false;
    }

    ///执行流程
    ///1. 获取字段定义
    ///2. 生成输出任务
    ///3. 等待任务执行完毕
    async fn run(&mut self, context: &mut OutputContext) -> crate::Result<()> {
        // 获取字段定义
        let columns = self.get_columns_define().await.unwrap_or(vec![]);

        let default_columns = Vec::new();

        let schema_columns = self.columns().unwrap_or(&default_columns);

        // 合并两个数组
        let columns = OutputColumn::merge_columns(&columns, &schema_columns);
        self.columns_mut(columns.clone());

        match self.transfer_to_schema() {
            Some(schema) => {
                context
                    .schema
                    .outputs
                    .insert(self.name().to_owned(), schema);
            }
            None => {
                println!("不能解析output的schema");
            }
        }

        if context.skip {
            return Ok(());
        }
        let (notify_shutdown, _) = broadcast::channel::<()>(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(1);
        println!("{} will running...", self.name());
        let limiter = context
            .limit
            .map(|limit| Arc::new(Mutex::new(TokenBuketLimiter::new(limit, limit * 2))));

        let count_rc = self
            .count()
            .map(|count| Arc::new(AtomicI64::new(count as i64)));

        let concurrency = Arc::new(Semaphore::new(self.concurrency()));
        while !self.is_shutdown() {
            // 检查数量
            if let Some(count) = count_rc.as_ref() {
                if count.load(Ordering::SeqCst) <= 0 {
                    break;
                }
            }

            let permit = concurrency.clone().acquire_owned().await.unwrap();

            let columns = columns.clone();
            let shutdown = Shutdown::new(notify_shutdown.subscribe());

            let count_rc = count_rc.clone();
            let limiter = limiter.clone();

            let task = self.get_output_task(
                columns,
                shutdown_complete_tx.clone(),
                shutdown,
                count_rc,
                limiter,
            );

            match task {
                Some(mut task) => {
                    tokio::spawn(async move {
                        if let Err(err) = task.run().await {
                            println!("task run error: {}", err);
                        }
                        drop(permit);
                    });
                }
                None => {
                    //nothing
                }
            }
        }

        // When `notify_shutd4own` is dropped, all tasks which have `subscribe`d will
        // receive the shutdown signal and can exit
        drop(notify_shutdown);
        // Drop final `Sender` so the `Receiver` below can complete
        drop(shutdown_complete_tx);
        // 等待所有的future执行完毕
        // futures::future::join_all(futures).await;
        let _ = shutdown_complete_rx.recv().await;

        Ok(())
    }
}





