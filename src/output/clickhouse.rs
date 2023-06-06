use clickhouse::Client;

use crate::{cli::Cli, task::clickhouse::ClickHouseTask};

impl ClickHouseOutput {
    pub fn new(cli: Cli) -> Self {
        let interval = cli.interval.unwrap_or(5);
        Self {
            name: "clickhouse".into(),
            interval,
            args: cli.into(),
            logger: StaticsLogger::new(interval),
            tasks: vec![],
        }
    }

    pub fn connect(&self) -> Client {
        let url = format!("http://{}:{}", self.args.host, self.args.port);
        Client::default()
            .with_url(url)
            .with_user(self.args.user.clone())
            .with_password(self.args.password.clone())
            .with_database("ws_dwd")
    }
}

impl Into<ClickHouseArgs> for Cli {
    fn into(self) -> ClickHouseArgs {
        ClickHouseArgs {
            host: self.host,
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
impl super::Output for ClickHouseOutput {
    fn name(&self) -> &str {
        return &self.name;
    }

    async fn run(&mut self, context: &mut OutputContext) -> crate::Result<()> {
        Ok(())
        //        let client = self.connect();
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
        //            let mut task = ClickHouseTask::new(task_name, client, batch, count);
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
impl Close for ClickHouseOutput {
    async fn close(&mut self) -> crate::Result<()> {
        for task in &mut self.tasks {
            task.close().await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ClickHouseArgs {
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

pub struct ClickHouseOutput {
    pub logger: StaticsLogger,

    pub name: String,

    pub interval: usize,

    pub args: ClickHouseArgs,

    pub tasks: Vec<ClickHouseTask>,
}
