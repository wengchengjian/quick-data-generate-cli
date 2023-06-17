use std::{
    sync::{atomic::AtomicI64, Arc},
};

use async_trait::async_trait;
use mysql_async::{prelude::Query, Pool};
use tokio::sync::Mutex;

use crate::{
    core::{error::Error, limit::token::TokenBuketLimiter, watch::StopWatch},
    model::column::OutputColumn,
};

use super::Exector;

#[derive(Debug, Clone)]
pub struct MysqlTaskExecutor {
    pub database: String,
    pub table: String,
    pub pool: Pool,
    pub batch: usize,
    pub limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    pub count: Option<Arc<AtomicI64>>,
    pub columns: Vec<OutputColumn>,
    pub task_name: String,
}

impl MysqlTaskExecutor {
    pub fn new(
        pool: Pool,
        batch: usize,
        count: Option<Arc<AtomicI64>>,
        database: String,
        table: String,
        columns: Vec<OutputColumn>,
        task_name: String,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> Self {
        Self {
            pool,
            batch,
            columns,
            count,
            database,
            table,
            task_name,
            limiter,
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

    fn replace_val(&self, header: String, key: &str, val: &serde_json::Value) -> String {
        if val.is_number() {
            if val.is_i64() || val.is_u64() || val.is_f64() {
                return header.replace(format!(":{}", key).as_str(), val.to_string().as_str());
            }
        }
        return header.replace(
            format!(":{}", key).as_str(),
            format!("'{}'", val.as_str().unwrap()).as_str(),
        );
    }
}

#[async_trait]
impl Exector for MysqlTaskExecutor {
    fn batch(&self) -> usize {
        return self.batch;
    }
    fn columns(&self) -> &Vec<OutputColumn> {
        return &self.columns;
    }

    fn limiter(&mut self) -> Option<&mut Arc<Mutex<TokenBuketLimiter>>> {
        return self.limiter.as_mut();
    }

    fn count(&mut self) -> Option<&Arc<AtomicI64>> {
        return self.count.as_ref();
    }

    fn name(&self) -> &str {
        return &self.task_name;
    }

    fn is_multi_handle(&self) -> bool {
        return true;
    }

    async fn handle_batch(&mut self, vals: Vec<serde_json::Value>) -> crate::Result<()> {
        let (column_names, column_name_vals) = self.get_columns_name();

        let mut insert_header = format!(
            "INSERT DELAYED  INTO {}.{} ({}) VALUES ",
            self.database, self.table, column_names
        );
        let mut watch = StopWatch::new();
        watch.start("组装sql");

        for val in vals {
            let mut name_vals = format!("({})", column_name_vals);

            let fake_data = val.as_object().expect("错误的数据类型");
            for (key, val) in fake_data {
                name_vals = self.replace_val(name_vals, key, &val);
            }
            insert_header.push_str(&name_vals);
            insert_header.push(',');
        }
        watch.stop();
        insert_header.pop();
        match self.pool.get_conn().await {
            Ok(mut conn) => {
                watch.start("执行sql");
                if let Err(err) = insert_header.run(&mut conn).await {
                    println!("insert error: {:?}", err);
                }
                watch.stop();
                watch.print_all_task_mils();
                return Ok(());
            }
            Err(e) => {
                return Err(Error::Other(Box::new(e)));
            }
        };
    }
}
