use mysql_async::prelude::*;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::sync::{broadcast, mpsc, Semaphore};

use crate::cli::Cli;

impl MysqlOutput {
    pub fn new(cli: Cli) -> Self {
        let interval = cli.interval.unwrap_or(5);
        Self {
            name: "Mysql".into(),
            interval: interval,
            args: cli.into(),
            logger: StaticsLogger::new(interval),
            tasks: vec![],
        }
    }

    pub fn connect(&self) -> crate::Result<()> {
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            self.args.user, self.args.password, self.args.host, self.args.port, self.args.database
        );
        Ok(())
    }
}

impl Into<MysqlArgs> for Cli {
    fn into(self) -> MysqlArgs {
        MysqlArgs {
            host: self.host.unwrap_or("192.168.180.217".to_string()),
            port: self.port.unwrap_or(8123),
            user: self.user.unwrap_or("default".to_string()),
            password: self.password.unwrap_or("!default@123".to_string()),
            database: self.database.unwrap_or("ws_dwd".to_string()),

            table: self
                .table
                .unwrap_or("dwd_standard_ip_session_all".to_string()),
            batch: self.batch.unwrap_or(50000),
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

    fn name(&self) -> &str {
        return &self.name;
    }

    fn interval(&self) -> usize {
        return self.interval;
    }

    async fn run(&mut self, context: &mut OutputContext) -> crate::Result<()> {
        Ok(())
        //        let pool = self.connect().expect("获取mysql连接失败!");
        //
        //        let mut conn = pool.get_conn()?;
        //
        //        let batch = self.args.batch;
        //        let count = self.args.count;
        //        let semaphore: Arc<Semaphore> = context.concurrency.clone();
        //        let task_num = 0;
        //        loop {
        //            let permit = semaphore.acquire_owned().await.unwrap();
        //
        //            let task_name = format!("{}-task-{}", self.name, task_num);
        //
        //            let client = client.clone();
        //            let mut task = MysqlTask::new(task_name, client, batch, count);
        //            self.tasks.push(task);
        //            let mut logger = self.logger;
        //            tokio::spawn(async move {
        //                if let Err(err) = task.run(&mut logger).await {
        //                    println!("task run error: {}", err);
        //                }
        //
        //                drop(permit);
        //            });
        //        }
    }
}

#[async_trait]
impl Close for MysqlOutput {
    async fn close(&mut self) -> crate::Result<()> {
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

    pub tasks: Vec<MysqlOutputTask>,
}

pub struct MysqlOutputTask {}

#[async_trait]
impl Close for MysqlOutputTask {
    async fn close(&mut self) -> crate::Result<()> {
        Ok(())
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
