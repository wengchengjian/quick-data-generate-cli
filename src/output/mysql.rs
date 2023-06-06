use mysql_async::{prelude::*, Conn};
use mysql_async::{Opts, Pool};
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicBool, Arc};
use std::vec;
use tokio::join;
use tokio::sync::{broadcast, mpsc, Semaphore};

use crate::cli::Cli;
use crate::column::{DataTypeEnum, OutputColumn};
use crate::log::{self, ChannelStaticsLog};
use crate::shutdown::Shutdown;
use crate::task::mysql::MysqlTask;

impl MysqlOutput {
    pub fn new(cli: Cli) -> Self {
        let interval = cli.interval.unwrap_or(5);
        Self {
            name: "Mysql".into(),
            interval: interval,
            args: cli.into(),
            logger: StaticsLogger::new(interval),
            tasks: vec![],
            shutdown: AtomicBool::new(false),
        }
    }

    pub fn connect(&self) -> crate::Result<Pool> {
        let url = "mysql://root:password@localhost:3307/db_name";
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            self.args.user, self.args.password, self.args.host, self.args.port, self.args.database
        );

        let database_url = Opts::from_url(url.as_str())?;

        let pool = Pool::new(database_url);

        Ok(pool)
    }

    pub fn get_logger_mut(&mut self) -> &mut StaticsLogger {
        &mut self.logger
    }
    async fn start_log(&mut self, msg_rx: &mut mpsc::Receiver<ChannelStaticsLog>) {
        loop {
            let msg = msg_rx.recv().await.unwrap();
            self.logger.add_total(&msg.name, msg.total);
            self.logger.add_commit(&msg.name, msg.commit);
        }
    }
    /// 根据数据库和表获取字段定义
    pub async fn get_columns_define(
        conn: Conn,
        database: &str,
        table: &str,
    ) -> crate::Result<Vec<OutputColumn>> {
        let sql = format!("desc {}.{}", database, table);
        let mut conn = conn;
        let res = sql
            .with(())
            .map(&mut conn, |(Field, Type)| OutputColumn {
                name: Field,
                data_type: DataTypeEnum::from_string(Type).unwrap(),
            })
            .await?;
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

impl Into<MysqlArgs> for Cli {
    fn into(self) -> MysqlArgs {
        MysqlArgs {
            host: self.host,
            port: self.port.unwrap_or(3306),
            user: self.user.unwrap_or("root".to_string()),
            password: self.password.unwrap_or("wcj520666".to_string()),
            database: self.database.unwrap_or("tests".to_string()),

            table: self.table.unwrap_or("payment".to_string()),
            batch: self.batch.unwrap_or(5000),
            count: self.count.unwrap_or(0),
        }
    }
}

use async_trait::async_trait;

#[async_trait]
impl super::Output for MysqlOutput {
    fn get_logger(&self) -> &StaticsLogger {
        return &self.logger;
    }

    fn set_logger(&mut self, logger: StaticsLogger) {
        self.logger = logger;
    }

    fn interval(&self) -> usize {
        return self.interval;
    }

    fn name(&self) -> &str {
        return &self.name;
    }

    async fn run(&mut self, context: &mut OutputContext) -> crate::Result<()> {
        // 获取连接池
        let pool = self.connect().expect("获取mysql连接失败!");

        let conn = pool.get_conn().await.expect("获取mysql连接失败!");
        // 获取字段定义
        let columns = MysqlOutput::get_columns_define(conn, &self.args.database, &self.args.table)
            .await
            .expect("获取字段定义失败");
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(1);
        let (msg_tx, mut msg_rx) = mpsc::channel::<ChannelStaticsLog>(10);

        let batch = self.args.batch;
        let count = self.args.count;
        let task_num = 0;

        while !self.shutdown.load(Ordering::SeqCst) {
            let permit = context.concurrency.clone().acquire_owned().await.unwrap();

            let task_name = format!("{}-task-{}", self.name, task_num);
            let conn = pool.get_conn().await?;

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
                msg_tx.clone(),
                Shutdown::new(notify_shutdown.subscribe()),
            );
            let future = tokio::spawn(async move {
                if let Err(err) = task.run().await {
                    println!("task run error: {}", err);
                }

                drop(permit);
            });

            tokio::select! {
                _ = future => {
                    continue;
                }
                _ = self.start_log(&mut msg_rx) => {
                    continue;
                }
            }
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
    async fn close(&mut self) -> crate::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        for task in &mut self.tasks {
            task.close().await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MysqlArgs {
    pub host: String,

    pub port: u16,

    pub user: String,

    pub password: String,

    pub database: String,

    pub table: String,

    pub batch: usize,

    pub count: usize,
}

use super::{super::log::StaticsLogger, Close, OutputContext};

pub struct MysqlOutput {
    pub logger: StaticsLogger,

    pub name: String,

    pub interval: usize,

    pub args: MysqlArgs,

    pub tasks: Vec<MysqlTask>,

    pub shutdown: AtomicBool,
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
    fn test_mysql_conn() {
        let res = aw!(mysql_conn());
        assert_eq!(res.unwrap(), ());
    }

    async fn mysql_conn() -> Result<()> {
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
