use std::sync::{atomic::AtomicI64, Arc};

use async_trait::async_trait;
use mysql_async::{
    prelude::{Query, WithParams},
    Pool,
};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex,
};

use crate::{
    core::{
        error::{Error, IoError},
        limit::token::TokenBuketLimiter,
        traits::{Name, TaskDetailStatic},
    },
    datasource::DATA_SOURCE_MANAGER,
};

use super::Exector;

#[derive(Debug, Clone)]
pub struct MysqlTaskExecutor {
    pub pool: Pool,
    pub limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    pub count: Option<Arc<AtomicI64>>,
    pub task_name: String,
    pub receiver: Option<Arc<Mutex<Receiver<serde_json::Value>>>>,
    pub sender: Option<Sender<serde_json::Value>>,
    pub next: usize,
}
impl Name for MysqlTaskExecutor {
    fn name(&self) -> &str {
        &self.task_name
    }
}

impl TaskDetailStatic for MysqlTaskExecutor {}

impl MysqlTaskExecutor {
    pub fn new(
        pool: Pool,
        count: Option<Arc<AtomicI64>>,
        task_name: String,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
        receiver: Option<Arc<Mutex<Receiver<serde_json::Value>>>>,
        sender: Option<Sender<serde_json::Value>>,
    ) -> Self {
        Self {
            pool,
            count,
            task_name,
            limiter,
            receiver,
            sender,
            next: 0,
        }
    }

    pub async fn database(&self) -> crate::Result<String> {
        self.meta()
            .await
            .ok_or(Error::Io(IoError::ArgNotFound("meta")))?["database"]
            .as_str()
            .map(|database| database.to_owned())
            .ok_or(Error::Io(IoError::ArgNotFound("database")))
    }

    pub async fn table(&self) -> crate::Result<String> {
        self.meta()
            .await
            .ok_or(Error::Io(IoError::ArgNotFound("meta")))?["table"]
            .as_str()
            .map(|table| table.to_owned())
            .ok_or(Error::Io(IoError::ArgNotFound("table")))
    }

    pub async fn get_columns_name(&self) -> crate::Result<(String, String)> {
        let mut columns_name = String::new();
        let mut columns_name_val = String::new();
        if let Some(columns) = self.columns().await {
            for column in columns {
                columns_name.push_str(&column.name());
                columns_name.push_str(",");

                columns_name_val.push_str(format!(":{}", column.name()).as_str());
                columns_name_val.push_str(",");
            }
            columns_name.pop();
            columns_name_val.pop();
            return Ok((columns_name, columns_name_val));
        }
        Err(Error::Io(IoError::ArgNotFound("columns")))
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
    fn limiter(&mut self) -> Option<&mut Arc<Mutex<TokenBuketLimiter>>> {
        return self.limiter.as_mut();
    }

    fn count_rc(&self) -> Option<Arc<AtomicI64>> {
        return self.count.clone();
    }

    fn is_multi_handle(&self) -> bool {
        return true;
    }

    fn receiver(&mut self) -> Option<&mut Arc<Mutex<Receiver<serde_json::Value>>>> {
        return self.receiver.as_mut();
    }

    fn sender(&mut self) -> Option<&mut Sender<serde_json::Value>> {
        return self.sender.as_mut();
    }

    async fn handle_fetch(&mut self) -> crate::Result<Vec<serde_json::Value>> {
        let query_sql = format!(
            "select * from {}.{} limit {}, {}",
            self.database().await?,
            self.table().await?,
            self.next,
            self.batch().await
        );

        match self.pool.get_conn().await {
            Ok(mut conn) => {
                let data = query_sql.with(()).fetch(&mut conn).await?;
                // 更新next
                self.next = self.next + self.batch().await;
                return Ok(data);
            }
            Err(e) => {
                return Err(Error::Other(Box::new(e)));
            }
        };
    }

    async fn handle_batch(&mut self, vals: Vec<serde_json::Value>) -> crate::Result<()> {
        let (column_names, column_name_vals) = self.get_columns_name().await?;
        let _schema = DATA_SOURCE_MANAGER
            .read()
            .await
            .get_schema(self.name())
            .ok_or(Error::Io(IoError::SchemaNotFound))?;
        let mut insert_header = format!(
            "INSERT DELAYED  INTO {}.{} ({}) VALUES ",
            self.database().await?,
            self.table().await?,
            column_names
        );
        //        let mut watch = StopWatch::new();
        //        watch.start("组装sql");

        for val in vals {
            let mut name_vals = format!("({})", column_name_vals);

            let fake_data = val.as_object().expect("错误的数据类型");
            for (key, val) in fake_data {
                name_vals = self.replace_val(name_vals, key, &val);
            }
            insert_header.push_str(&name_vals);
            insert_header.push(',');
        }
        //        watch.stop();
        insert_header.pop();
        match self.pool.get_conn().await {
            Ok(mut conn) => {
                //                watch.start("执行sql");
                if let Err(err) = insert_header.run(&mut conn).await {
                    println!("insert error: {:?}", err);
                }
                //                watch.stop();
                //                watch.print_all_task_mils();
                return Ok(());
            }
            Err(e) => {
                return Err(Error::Other(Box::new(e)));
            }
        };
    }
}
