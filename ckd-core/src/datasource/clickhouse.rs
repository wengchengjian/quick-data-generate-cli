use clickhouse::Client;
use crate::core::log::StaticsLogger;
use crate::model::column::DataSourceColumn;
use crate::task::clickhouse::ClickHouseTask;

impl ClickHouseOutput {

    pub fn connect(&self) -> Client {
        let url = format!("http://{}:{}", self.args.host, self.args.port);
        Client::default()
            .with_url(url)
            .with_user(self.args.user.clone())
            .with_password(self.args.password.clone())
            .with_database("ws_dwd")
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

    pub count: isize,
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
