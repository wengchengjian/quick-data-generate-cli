use mysql_async::{from_row, prelude::*, Conn};
use mysql_async::{Opts, Pool};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::vec;
use tokio::sync::{broadcast, mpsc, Semaphore};

use crate::core::cli::Cli;
use crate::core::error::{Error, IoError, Result};
use crate::core::shutdown::Shutdown;
use crate::model::column::{DataTypeEnum, OutputColumn};
use crate::model::schema::{ChannelSchema, OutputSchema};
use crate::task::mysql::MysqlTask;

impl MysqlOutput {
    pub fn connect(&self) -> Result<Pool> {
        //        let url = "mysql://root:wcj520600@localhost:3306/tests";
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            self.args.user, self.args.password, self.args.host, self.args.port, self.args.database
        );

        let database_url = Opts::from_url(&url).map_err(|err| Error::Other(err.into()))?;

        let pool = Pool::new(database_url);

        Ok(pool)
    }

    /// 根据数据库和表获取字段定义
    pub async fn get_columns_define(
        conn: Conn,
        database: &str,
        table: &str,
    ) -> Result<Vec<OutputColumn>> {
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
                let Field = column.0;
                let Type = column.1;
                let Null = column.2;
                let Key = column.3.unwrap_or("NO".to_string());
                let Default = column.4.unwrap_or("NULL".to_string());
                let Extra = column.5.unwrap_or("".to_string());
                let column = MysqlColumnDefine {
                    Field,
                    Type,
                    Null,
                    Key,
                    Default,
                    Extra,
                };

                return OutputColumn {
                    name: column.Field.clone(),
                    data_type: DataTypeEnum::from_string(column.Type.clone()).unwrap(),
                };
            })
            .await
            .map_err(|err| Error::Other(err.into()))?;
        Ok(res)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MysqlColumnDefine {
    pub Field: String,
    pub Type: String,
    pub Null: String,
    pub Key: String,
    pub Default: String,
    pub Extra: String,
}

impl MysqlArgs {
    pub fn from_value(meta: serde_json::Value, channel: ChannelSchema) -> Result<MysqlArgs> {
        Ok(MysqlArgs {
            host: meta["host"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("host".to_string())))?
                .to_string(),
            port: meta["port"].as_u64().unwrap_or(3306) as u16,
            user: meta["user"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("user".to_string())))?
                .to_string(),
            password: meta["password"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("password".to_string())))?
                .to_string(),
            database: meta["database"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("database".to_string())))?
                .to_string(),

            table: meta["table"]
                .as_str()
                .ok_or(Error::Io(IoError::ArgNotFound("table".to_string())))?
                .to_string(),
            batch: channel.batch,
            count: channel.count,
            concurrency: channel.concurrency,
        })
    }
}

impl TryInto<MysqlArgs> for Cli {
    type Error = Error;

    fn try_into(self) -> std::result::Result<MysqlArgs, Self::Error> {
        Ok(MysqlArgs {
            host: self.host,
            port: self.port.unwrap_or(3306),
            user: self
                .user
                .ok_or(Error::Io(IoError::ArgNotFound("user".to_string())))?
                .to_string(),
            password: self
                .password
                .ok_or(Error::Io(IoError::ArgNotFound("password".to_string())))?
                .to_string(),
            database: self
                .database
                .ok_or(Error::Io(IoError::ArgNotFound("database".to_string())))?
                .to_string(),

            table: self
                .table
                .ok_or(Error::Io(IoError::ArgNotFound("table".to_string())))?
                .to_string(),
            batch: self.batch.unwrap_or(5000),
            count: self.count.unwrap_or(0),
            concurrency: self.concurrency.unwrap_or(1),
        })
    }
}

impl TryFrom<OutputSchema> for MysqlOutput {
    type Error = Error;

    fn try_from(value: OutputSchema) -> std::result::Result<Self, Self::Error> {
        Ok(MysqlOutput {
            name: "默认mysql输出".to_string(),
            args: MysqlArgs::from_value(value.meta, value.channel)?,
            tasks: vec![],
            shutdown: AtomicBool::new(false),
            columns: OutputColumn::get_columns_from_value(&value.columns),
        })
    }
}

use async_trait::async_trait;

#[async_trait]
impl super::Output for MysqlOutput {
    fn name(&self) -> &str {
        return &self.name;
    }

    fn get_columns(&self) -> &Vec<OutputColumn> {
        return &self.columns;
    }

    async fn run(&mut self, context: &mut OutputContext) -> Result<()> {
        // 获取连接池
        let pool = self.connect().expect("获取mysql连接失败!");

        let conn = pool.get_conn().await.expect("获取mysql连接失败!");
        // 获取字段定义
        let columns = MysqlOutput::get_columns_define(conn, &self.args.database, &self.args.table)
            .await
            .expect("获取字段定义失败");
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(1);

        let batch = self.args.batch;
        let count = self.args.count;
        let mut task_num = 0;

        while !self.shutdown.load(Ordering::SeqCst) {
            let permit = context.concurrency.clone().acquire_owned().await.unwrap();

            task_num += 1;
            let task_name = format!("{}-task-{}", self.name, task_num);

            let conn = pool
                .get_conn()
                .await
                .map_err(|err| Error::Other(err.into()))?;

            let columns = columns.clone();
            let database = self.args.database.clone();
            let table = self.args.table.clone();

            let mut task = MysqlTask::new(
                task_name,
                conn,
                batch,
                count,
                database,
                table,
                columns,
                shutdown_complete_tx.clone(),
                Shutdown::new(notify_shutdown.subscribe()),
            );
            tokio::spawn(async move {
                if let Err(err) = task.run().await {
                    println!("task run error: {}", err);
                }
                drop(permit);
            });
        }

        // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
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

#[async_trait]
impl Close for MysqlOutput {
    async fn close(&mut self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        for task in &mut self.tasks {
            task.close().await?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MysqlArgs {
    pub host: String,

    pub port: u16,

    pub user: String,

    pub password: String,

    pub database: String,

    pub table: String,

    pub batch: usize,

    pub count: usize,

    pub concurrency: usize,
}

use super::{Close, OutputContext};

#[derive(Debug)]
pub struct MysqlOutput {
    pub name: String,

    pub args: MysqlArgs,

    pub columns: Vec<OutputColumn>,

    pub tasks: Vec<MysqlTask>,

    pub shutdown: AtomicBool,
}

impl TryFrom<Cli> for MysqlOutput {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: Cli) -> std::result::Result<Self, Self::Error> {
        let res = MysqlOutput {
            name: "mysql".into(),
            args: value.try_into()?,
            tasks: vec![],
            shutdown: AtomicBool::new(false),
            columns: vec![],
        };

        Ok(res)
    }
}

#[cfg(test)]
mod tests {

    use mysql_async::Opts;

    use super::*;
    use crate::*;

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
