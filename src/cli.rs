use structopt::StructOpt;

use crate::output::OutputEnum;

#[derive(Debug, StructOpt, Clone)]
#[structopt(
    name = "ckd",
    about = "A command line tool to load data into Output source."
)]
pub struct Cli {
    #[structopt(long)]
    pub host: Option<String>,

    #[structopt(long)]
    pub database: Option<String>,

    #[structopt(long)]
    pub table: Option<String>,

    #[structopt(long)]
    pub port: Option<u16>,

    #[structopt(short, long)]
    pub user: Option<String>,

    #[structopt(short, long)]
    pub password: Option<String>,

    #[structopt(short, long)]
    pub batch: Option<usize>,

    #[structopt(long)]
    pub count: Option<usize>,

    #[structopt(short, long)]
    pub interval: Option<usize>,

    #[structopt(short, long)]
    pub concurrency: Option<usize>,

    #[structopt(short, long, default_value = "clickhouse")]
    pub output: OutputEnum,
}
