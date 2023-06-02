use structopt::StructOpt;

use crate::output::OutputEnum;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "ckd",
    about = "A command line tool to load data into ClickHouse."
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

    #[structopt(long)]
    pub user: Option<String>,

    #[structopt(long)]
    pub password: Option<String>,

    #[structopt(long)]
    pub batch: Option<usize>,

    #[structopt(long)]
    pub count: Option<usize>,

    #[structopt(long)]
    pub print: Option<usize>,

    #[structopt(long)]
    pub threats: Option<usize>,

    #[structopt(long)]
    pub output: Option<OutputEnum>,
}
