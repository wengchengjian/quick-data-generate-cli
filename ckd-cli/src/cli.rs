use ckd_core::core::cons::DEFAULT_INTERVAL;
use ckd_core::datasource::csv::CsvArgs;
use ckd_core::datasource::kafka::KafkaArgs;
use ckd_core::datasource::mysql::MysqlArgs;
use ckd_core::datasource::{DataSourceChannel, DataSourceContext, DataSourceEnum};
use std::path::PathBuf;
use std::sync::Arc;

use crate::{create_context, Json};
use ckd_core::core::error::{Error, IoError};
use ckd_core::core::parse::{parse_datasources_from_schema, parse_schema, DEFAULT_FAKE_DATASOURCE};
use ckd_core::model::schema::{ChannelSchema, DataSourceSchema, Schema};
use ckd_core::Result;
use structopt::StructOpt;

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
    pub count: Option<isize>,

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

impl TryInto<KafkaArgs> for Cli {
    type Error = Error;

    fn try_into(self) -> std::result::Result<KafkaArgs, Self::Error> {
        Ok(KafkaArgs {
            host: self.host,
            port: self.port.unwrap_or(9092),
            batch: self.batch.unwrap_or(5000),
            count: self.count.unwrap_or(isize::max_value()),
            concurrency: self.concurrency.unwrap_or(1),
            topic: self.topic.ok_or(Error::Io(IoError::ArgNotFound("topic")))?,
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
                .ok_or(Error::Io(IoError::ArgNotFound("user")))?
                .to_string(),
            password: self
                .password
                .ok_or(Error::Io(IoError::ArgNotFound("password")))?
                .to_string(),
            database: self
                .database
                .ok_or(Error::Io(IoError::ArgNotFound("database")))?
                .to_string(),

            table: self
                .table
                .ok_or(Error::Io(IoError::ArgNotFound("table")))?
                .to_string(),
            batch: self.batch.unwrap_or(1000),
            count: self.count.unwrap_or(0),
            concurrency: self.concurrency.unwrap_or(1),
        })
    }
}

impl TryInto<CsvArgs> for Cli {
    type Error = Error;

    fn try_into(self) -> std::result::Result<CsvArgs, Self::Error> {
        Ok(CsvArgs {
            filename: self.filename.unwrap_or("default.csv".to_owned()),
            batch: self.batch.unwrap_or(1000),
            count: self.count.unwrap_or(0),
            concurrency: self.concurrency.unwrap_or(1),
        })
    }
}

pub fn parse_meta_from_cli(source: &DataSourceEnum, cli: Cli) -> ckd_core::Result<Json> {
    match source {
        DataSourceEnum::Mysql => {
            Ok(serde_json::to_value(TryInto::<MysqlArgs>::try_into(cli)?).expect("json解析失败"))
        }
        DataSourceEnum::Kafka => {
            Ok(serde_json::to_value(TryInto::<KafkaArgs>::try_into(cli)?).expect("json解析失败"))
        }
        DataSourceEnum::Csv => {
            Ok(serde_json::to_value(TryInto::<CsvArgs>::try_into(cli)?).expect("json解析失败"))
        }
        _ => {
            return Err(Error::Io(IoError::UnkownSourceError("unkown".to_owned())));
        }
    }
}
use tokio::sync::Mutex;

/// 返回解析后的输出源，interval，concurrency, 以cli为准
pub async fn parse_datasource(
    cli: Cli,
) -> Result<(
    Vec<Arc<Mutex<Box<dyn DataSourceChannel>>>>,
    usize,
    DataSourceContext,
)> {
    let mut cli = cli;
    let mut datasources = vec![];
    let interval = cli.interval;

    //    let _ = cli.schema.insert(PathBuf::from("examples/schema.json"));

    if let Some(schema_path) = &cli.schema {
        let schema = parse_schema(schema_path).unwrap();

        if let Some(schema_interval) = schema.interval {
            if let None = interval {
                let _ = cli.interval.insert(schema_interval);
            }
        }

        let mut schema_datasources = parse_datasources_from_schema(schema).await?;

        datasources.append(&mut schema_datasources);
    }
    let limit = cli.limit;
    let skip = cli.skip;

    let is_force_parse_from_cli = datasources.len() == 0;

    let datasource = parse_datasource_from_cli(cli, is_force_parse_from_cli).await?;

    if let Some(datasource) = datasource {
        datasources.extend(datasource);
    }

    let interval = interval.unwrap_or(DEFAULT_INTERVAL);

    let context = create_context(limit, skip);

    return Ok((datasources, interval, context));
}

pub async fn parse_datasource_from_cli(
    cli: Cli,
    force: bool,
) -> Result<Option<Vec<Arc<Mutex<Box<dyn DataSourceChannel>>>>>> {
    match parse_schema_from_cli(cli) {
        Ok(schema) => {
            let channel = parse_datasources_from_schema(schema).await?;
            return Ok(Some(channel));
        }
        Err(e) => {
            if force {
                return Err(e);
            } else {
                return Ok(None);
            }
        }
    }
}

pub fn parse_schema_from_cli(cli: Cli) -> Result<Schema> {
    if let None = cli.source {
        return Err(Error::Io(IoError::ArgNotFound("source")));
    }

    let mut sources = Vec::new();

    let channel = Some(ChannelSchema {
        batch: cli.batch,
        concurrency: cli.concurrency,
        count: cli.count,
    });

    let meta = parse_meta_from_cli(&cli.source.unwrap(), cli.clone())?;

    let source = DataSourceSchema::new(
        0,
        "default".to_owned(),
        cli.source.unwrap(),
        Some(meta),
        None,
        channel,
        Some(vec![DEFAULT_FAKE_DATASOURCE.to_owned()]),
        time::OffsetDateTime::now_utc(),
        time::OffsetDateTime::now_utc(),
    );

    sources.push(source);

    let schema = Schema {
        interval: cli.interval,
        sources,
    };

    return Ok(schema);
}
