use std::path::PathBuf;

use structopt::StructOpt;

use crate::{datasource::DataSourceEnum};

#[derive(Debug, StructOpt, Clone)]
#[structopt(
    name = "ckd",
    about = "A command line tool to load data into Output source."
)]
pub struct Cli {
    /// 数据源地址
    #[structopt(long, default_value = "127.0.0.1")]
    pub host: String,

    /// 数据库
    #[structopt(long)]
    pub database: Option<String>,

    /// 表
    #[structopt(long)]
    pub table: Option<String>,

    /// topic
    #[structopt(long)]
    pub topic: Option<String>,

    /// 数据源端口
    #[structopt(long)]
    pub port: Option<u16>,

    /// 数据源用户
    #[structopt(short, long)]
    pub user: Option<String>,

    /// 数据源密码
    #[structopt(short, long)]
    pub password: Option<String>,

    /// 每次批量插入数量
    #[structopt(short, long)]
    pub batch: Option<usize>,

    /// 任务插入总数
    #[structopt(long)]
    pub count: Option<usize>,

    /// 指定数据源schema
    #[structopt(long)]
    pub schema: Option<PathBuf>,

    /// 日志打印间隔, 单位秒
    #[structopt(short, long)]
    pub interval: Option<usize>,

    /// 总并发数
    #[structopt(short, long)]
    pub concurrency: Option<usize>,

    /// 输出源， 支持kafka, mysql, clickhouse
    #[structopt(short, long)]
    pub source: Option<DataSourceEnum>,

    /// 限流输出速度, 针对每一个输出源
    #[structopt(short, long)]
    pub limit: Option<usize>,

    /// 是否跳过输出任务， 一般常用于只生成schema文件
    #[structopt(long)]
    pub skip: bool,

    /// 本地输出文件参数
    #[structopt(short, long)]
    pub filename: Option<String>,
}
