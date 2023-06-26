use clickhouse::Client;

use crate::{
    core::{cli::Cli, log::StaticsLogger},
    task::clickhouse::ClickHouseTask, model::column::DataSourceColumn,
};

impl ClickHouseOutput {
    pub fn new(cli: Cli) -> Self {
        let interval = cli.interval.unwrap_or(1);
        Self {
            name: "clickhouse".into(),
            interval,
            args: cli.into(),
            logger: StaticsLogger::new(interval),
            tasks: vec![],
            columns: vec![],
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

#[derive(Debug)]
pub struct ClickHouseOutput {
    pub logger: StaticsLogger,

    pub name: String,

    pub interval: usize,

    pub args: ClickHouseArgs,

    pub tasks: Vec<ClickHouseTask>,

    pub columns: Vec<DataSourceColumn>,
}
