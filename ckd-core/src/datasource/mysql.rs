use mysql_async::{from_row, prelude::*, Conn};
use mysql_async::{Opts, Pool};

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::vec;
use tokio::sync::{mpsc, Mutex, RwLock};
use uuid::Uuid;

impl MysqlDataSource {
    pub fn new(schema: DataSourceSchema, id: &str) -> Self {
        Self {
            id: id.to_owned(),
            name: schema.name,
            pool_cache: HashMap::new(),
            sources: schema.sources.unwrap_or(vec![]),
        }
    }

    pub async fn connect(&mut self) -> crate::Result<Pool> {
        //        let url = "mysql://root:wcj520600@localhost:3306/tests";
        let url = format!(
            "mysql://{}:{}@{}:{}/{}?&compression=fast&stmt_cache_size=256&conn_ttl=60",
            self.meta("user").await?.as_str().unwrap(),
            self.meta("password").await?.as_str().unwrap(),
            self.meta("host")
                .await
                .unwrap_or(json!("localhost"))
                .as_str()
                .unwrap_or("localhost"),
            self.meta("port")
                .await
                .unwrap_or(json!(3306))
                .as_u64()
                .unwrap_or(3306),
            self.meta("database").await?.as_str().unwrap(),
        );

        let pool = self.pool_cache.entry(url.clone()).or_insert_with(|| {
            let opt = Opts::from_url(&url).expect("mysql连接地址错误");

            Pool::new(opt)
        });

        Ok(pool.clone())
    }

    /// 根据数据库和表获取字段定义
    pub async fn get_columns_define(
        conn: Conn,
        database: &str,
        table: &str,
    ) -> crate::Result<Vec<DataSourceColumn>> {
        let sql = format!("desc {}.{}", database, table);
        let mut conn = conn;
        let res = sql
            .with(())
            .map(&mut conn, |row| {
                let column = from_row::<(
                    String,
                    String,
                    String,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                )>(row);
                let field = column.0;
                let cype = column.1;
                let null = column.2;
                let key = column.3.unwrap_or("NO".to_string());
                let default = column.4.unwrap_or("NULL".to_string());
                let extra = column.5.unwrap_or("".to_string());
                let column = MysqlColumnDefine {
                    field,
                    cype,
                    null,
                    key,
                    default,
                    extra,
                };

                return DataSourceColumn {
                    name: column.field.clone(),
                    data_type: DataTypeEnum::from_string(&column.cype).unwrap(),
                };
            })
            .await
            .map_err(|err| Error::Other(err.into()))?;
        Ok(res)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MysqlColumnDefine {
    pub field: String,
    pub cype: String,
    pub null: String,
    pub key: String,
    pub default: String,
    pub extra: String,
}

impl MysqlArgs {
    pub fn from_value(
        meta: Option<Json>,
        channel: Option<ChannelSchema>,
    ) -> crate::Result<MysqlArgs> {
        let meta = meta.unwrap_or(json!({
        "host":"127.0.0.1",
        "port": 3306
        }));

        let channel = channel.unwrap_or(ChannelSchema::default());

        Ok(MysqlArgs {
            host: meta["host"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("host")))?
                .to_string(),
            port: meta["port"].as_u64().unwrap_or(3306) as u16,
            user: meta["user"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("user")))?
                .to_string(),
            password: meta["password"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("password")))?
                .to_string(),
            database: meta["database"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("database")))?
                .to_string(),

            table: meta["table"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("table")))?
                .to_string(),
            batch: channel.batch.unwrap_or(1000),
            count: channel.count.unwrap_or(isize::MAX),
            concurrency: channel.concurrency.unwrap_or(1),
        })
    }
}

impl TryFrom<DataSourceSchema> for MysqlDataSource {
    type Error = Error;

    fn try_from(value: DataSourceSchema) -> std::result::Result<Self, Self::Error> {
        Ok(MysqlDataSource {
            id: Uuid::new_v4().to_string(),
            name: value.name,
            pool_cache: HashMap::new(),
            sources: value.sources.unwrap_or(vec![]),
        })
    }
}

use crate::core::error::{Error, IoError};
use crate::core::limit::token::TokenBuketLimiter;
use crate::core::shutdown::Shutdown;
use crate::core::traits::{Name, TaskDetailStatic};
use crate::datasource::DATA_SOURCE_MANAGER;
use crate::model::column::{DataSourceColumn, DataTypeEnum};
use crate::model::schema::{ChannelSchema, DataSourceSchema};
use crate::task::mysql::MysqlTask;
use crate::task::Task;
use crate::Json;
use async_trait::async_trait;

impl Name for MysqlDataSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl TaskDetailStatic for MysqlDataSource {}

#[async_trait]
impl super::DataSourceChannel for MysqlDataSource {
    async fn after_run(
        &mut self,
        _context: Arc<RwLock<DataSourceContext>>,
        _channel: ChannelContext,
    ) -> crate::Result<()> {
        for pool in self.pool_cache.values() {
            let _ = pool.to_owned().disconnect().await;
        }
        if self.need_log() {
            //更新状态
            self.update_final_status(DataSourceChannelStatus::Ended, false)
                .await;
            //拿到对应的source
            if let Some(flag) = self.is_producer().await {
                if !flag {
                    let session = DATA_SOURCE_MANAGER
                        .read()
                        .await
                        .sessions
                        .get(self.id())
                        .cloned();

                    if let Some(session) = session {
                        if let Some(sources) = session.consumer_sources.get(self.name()) {
                            for source in sources {
                                DATA_SOURCE_MANAGER
                                    .write()
                                    .await
                                    .update_will_status(
                                        self.id(),
                                        source,
                                        DataSourceChannelStatus::Ended,
                                    )
                                    .await;
                            }
                        }
                    }
                }
            }
            println!("{} Ended", self.name());
        }
        Ok(())
    }
    async fn get_columns_define(&mut self) -> crate::Result<Vec<DataSourceColumn>> {
        // 获取连接池
        let pool = self.connect().await?;

        let conn = pool.get_conn().await?;
        // 获取字段定义
        let columns = MysqlDataSource::get_columns_define(
            conn,
            self.meta("database")
                .await
                .expect("database参数为空!")
                .as_str()
                .unwrap(),
            self.meta("table")
                .await
                .expect("table参数为空!")
                .as_str()
                .unwrap(),
        )
        .await?;

        return Ok(columns);
    }

    async fn get_task(
        &mut self,
        channel: ChannelContext,
        shutdown_complete_tx: mpsc::Sender<()>,
        shutdown: Shutdown,
        count_rc: Option<Arc<AtomicI64>>,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> crate::Result<Option<Box<dyn Task>>> {
        // 获取连接池
        let pool = self.connect().await?;

        let task = MysqlTask::from_args(
            self.id(),
            self.name(),
            pool,
            shutdown_complete_tx,
            shutdown,
            limiter,
            count_rc,
            channel,
        );
        return Ok(Some(Box::new(task)));
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MysqlArgs {
    pub host: String,

    pub port: u16,

    pub user: String,

    pub password: String,

    pub database: String,

    pub table: String,

    pub batch: usize,

    pub count: isize,

    pub concurrency: usize,
}

use super::{ChannelContext, DataSourceChannelStatus, DataSourceContext};

#[derive(Debug)]
pub struct MysqlDataSource {
    pub id: String,

    pub name: String,

    pub pool_cache: HashMap<String, Pool>,

    pub sources: Vec<String>,
}

#[cfg(test)]
mod tests {

    use mysql_async::Opts;

    use super::*;

    #[derive(Debug, PartialEq, Eq, Clone)]
    struct Payment {
        customer_id: i32,
        amount: i32,
        account_name: Option<String>,
    }

    #[macro_export]
    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    #[test]
    #[ignore = "test mysql connection sometimes, not necessarily "]
    fn test_mysql_conn() {
        let res = aw!(mysql_conn());
        assert_eq!(res.unwrap(), ());
    }

    async fn mysql_conn() -> std::result::Result<(), Box<dyn std::error::Error>> {
        #[derive(Debug, PartialEq, Eq, Clone)]
        struct Payment {
            customer_id: i32,
            amount: i32,
            account_name: Option<String>,
        }

        let payments = vec![
            Payment {
                customer_id: 1,
                amount: 2,
                account_name: None,
            },
            Payment {
                customer_id: 3,
                amount: 4,
                account_name: Some("foo".into()),
            },
            Payment {
                customer_id: 5,
                amount: 6,
                account_name: None,
            },
            Payment {
                customer_id: 7,
                amount: 8,
                account_name: None,
            },
            Payment {
                customer_id: 9,
                amount: 10,
                account_name: Some("bar".into()),
            },
        ];

        let database_url = Opts::from_url("mysql://root:wcj520600@localhost:3306/tests")?;

        let pool = mysql_async::Pool::new(database_url);
        let mut conn = pool.get_conn().await?;

        r"drop table payment".with(()).run(&mut conn);

        // Create a temporary table
        r"CREATE TABLE IF NOT EXISTS payment (
        customer_id int not null,
        amount int not null,
        account_name text
    )"
        .ignore(&mut conn)
        .await?;

        // Save payments
        r"INSERT INTO payment (customer_id, amount, account_name)
      VALUES (:customer_id, :amount, :account_name)"
            .with(payments.iter().map(|payment| {
                params! {
                    "customer_id" => payment.customer_id,
                    "amount" => payment.amount,
                    "account_name" => payment.account_name.as_ref(),
                }
            }))
            .batch(&mut conn)
            .await?;

        // Load payments from the database. Type inference will work here.
        let loaded_payments = "SELECT customer_id, amount, account_name FROM payment"
            .with(())
            .map(&mut conn, |(customer_id, amount, account_name)| Payment {
                customer_id,
                amount,
                account_name,
            })
            .await?;

        // 删除表
        r"drop table payment".with(()).run(&mut conn);
        // Dropped connection will go to the pool
        drop(conn);

        // The Pool must be disconnected explicitly because
        // it's an asynchronous operation.
        pool.disconnect().await?;

        assert_eq!(loaded_payments, payments);

        // the async fn returns Result, so
        Ok(())
    }
}