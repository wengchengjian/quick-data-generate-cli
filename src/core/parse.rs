use super::{
    check::{DEFAULT_INTERVAL, MIN_THREAD_SIZE},
    cli::Cli,
    error::{Error, Result, IoError},
};
use std::{path::PathBuf, collections::HashMap};

use crate::{
    create_context, impl_func_is_primitive_by_parse,
    model::{
        column::{DataTypeEnum, FixedValue, DataSourceColumn},
        schema::{Schema, DataSourceSchema},
    }, datasource::{DataSourceChannel, DataSourceContext, DataSourceEnum, mysql::MysqlDataSource, kafka::KafkaDataSource, csv::CsvDataSource, fake::FakeDataSource, MpscDataSourceChannel},
};

impl_func_is_primitive_by_parse!((is_u8, u8), (is_u16, u16), (is_u32, u32), (is_u64, u64));
impl_func_is_primitive_by_parse!((is_i8, i8), (is_i16, i16), (is_i32, i32), (is_i64, i64));

pub fn parse_schema(path: &PathBuf) -> Result<Schema> {
    let data = std::fs::read(path)?;
    let schema: Schema = serde_json::from_slice(&data).map_err(|err| Error::Other(err.into()))?;
    Ok(schema)
}

pub fn parse_mpsc_from_schema(schema: &DataSourceSchema, source_map: HashMap<&str, DataSourceSchema>) -> Result<Box<dyn DataSourceChannel>> {

    let mut source_map = source_map;

    source_map.entry(DEFAULT_FAKE_DATASOURCE).or_insert(DataSourceSchema::fake());

    let mut producer = vec![];
    let default_sources = vec![DEFAULT_FAKE_DATASOURCE.to_owned()];
    let mut sources = &schema.sources;
    if sources.len() == 0 {
        sources = &default_sources;
    }

    for source in sources {
        match source_map.get(source.as_str()) {
            Some(source_schema) => {
                let schema = (*source_schema).to_owned();
                let datasource = parse_datasource_from_schema(schema)?;
                producer.push(datasource);
            },
            None => {
                return Err(Error::Io(IoError::UnkownSourceError(source.clone())));
            },
        }
    }

    let consumer: Box<dyn DataSourceChannel> = parse_datasource_from_schema(schema.clone())?;

    let data_source_channel = MpscDataSourceChannel::new(producer, consumer);

    return Ok(Box::new(data_source_channel));
}

pub fn parse_datasource_from_schema(
    schema: DataSourceSchema
) -> Result<Box<dyn DataSourceChannel>> {

  match schema.source {
      DataSourceEnum::Mysql => Ok(Box::new(MysqlDataSource::try_from(schema)?)),
      DataSourceEnum::Kafka => Ok(Box::new(KafkaDataSource::try_from(schema)?)),
      DataSourceEnum::Csv => Ok(Box::new(CsvDataSource::try_from(schema)?)),
      DataSourceEnum::Fake => Ok(Box::new(FakeDataSource::new(schema.channel.batch, schema.channel.concurrency))),
      _ => {
            return Err(Error::Io(IoError::UnkownSourceError("source".to_owned())));
        },
    }
}

pub const DEFAULT_FAKE_DATASOURCE: &str = "fake_datasource";


pub fn merge_datasource_schema_args(source: &DataSourceSchema, target: &DataSourceSchema) -> DataSourceSchema{
    let name = target.name.clone();

    let meta = target.meta.clone();

    let source = target.source;

    let column = target.columns.clone();

    let sources = target.sources.clone();

    let channel = target.channel.clone();

    DataSourceSchema::new(name, source, meta, column, channel,sources)
}

pub fn parse_datasources_from_schema(schema: Schema) -> Result<Vec<Box<dyn DataSourceChannel>>> {
    let mut outputs = vec![];

    let sources = &schema.sources;

    for source in sources {
        // 整合参数
        let source_map: HashMap<&str, DataSourceSchema> = schema.sources.iter().map(|item| (item.name.as_str(), merge_datasource_schema_args(source, item))).collect();

        outputs.push(parse_mpsc_from_schema(source, source_map)?);
    }
    Ok(outputs)
}

pub fn parse_output_from_cli(cli: Cli) -> Option<Box<dyn DataSourceChannel>> {
    let source_enum = cli.source;
    match source_enum {
        Some(source) => match source {
            DataSourceEnum::Kafka => return KafkaDataSource::from_cli(cli).ok(),
            DataSourceEnum::Mysql => return MysqlDataSource::from_cli(cli).ok(),
            DataSourceEnum::Csv => return CsvDataSource::from_cli(cli).ok(),
            DataSourceEnum::Fake => return None,
        },
        None => {
            return None;
        }
    };
}

pub fn parse_schema_from_datasources(datasources: &Vec<Box<dyn DataSourceChannel>>) -> Vec<DataSourceSchema> {
    let mut res = vec![];

    for datasource in datasources {
        match datasource.transfer_to_schema() {
            Some(schema) => res.push(schema),
            None => continue,
        };
    }

    return res;
}

/// 返回解析后的输出源，interval，concurrency, 以cli为准
pub fn parse_datasource(cli: Cli) -> Result<(Vec<Box<dyn DataSourceChannel>>, usize, DataSourceContext)> {
    let mut cli = cli;
    let mut datasources = vec![];
    let interval = cli.interval;
    let mut concurrency = cli.concurrency;

     let _ = cli.source.insert(DataSourceEnum::Mysql);
    // let _ = cli.topic.insert("FileHttpLogPushService".to_string());
     cli.host = "192.168.180.217".to_owned();
     let _ = cli.user.insert("root".to_string());
     let _ = cli.database.insert("tests".to_string());
     let _ = cli.table.insert("bfc_model_task".to_string());
     let _ = cli.password.insert("bfcdb@123".to_string());
     let _ = cli.batch.insert(1000);
     let _ = cli.count.insert(50000);
    let _ = cli.concurrency.insert(1);
    // let _ = cli.interval.insert(1);
//    let _ = cli.schema.insert(PathBuf::from(
//        "C:\\Users\\Administrator\\23383409-6532-437b-af7e-ee9cd4b87127.json",
//    ));

    if let Some(schema_path) = &cli.schema {
        let schema = parse_schema(schema_path).unwrap();

        if let Some(schema_interval) = schema.interval {
            if let None = interval {
                let _ = cli.interval.insert(schema_interval);
            }
        }

        let mut schema_datasources = parse_datasources_from_schema(schema)?;

        datasources.append(&mut schema_datasources);
    }
    let limit = cli.limit;
    let skip = cli.skip;

    if let Some(datasource) = parse_output_from_cli(cli) {
        datasources.push(datasource);
    }
    let interval = interval.unwrap_or(DEFAULT_INTERVAL);

    let schema = Schema::new(
        Some(interval),
        parse_schema_from_datasources(&datasources),
    );
    let context = create_context(limit, skip, schema);

    return Ok((datasources, interval, context));
}

use super::check::*;

pub fn parse_type(val: &serde_json::Value) -> DataTypeEnum {
    let val = val;
    if val.is_number() {
        if val.is_u64() {
            let val = val.as_u64().map(|v| format!("{}", v));
            let d = "0".to_owned();

            let val = val.unwrap_or(d);
            if is_u8(&val) {
                return DataTypeEnum::UInt8(FixedValue::None);
            }
            if is_u16(&val) {
                return DataTypeEnum::UInt16(FixedValue::None);
            }
            if is_u32(&val) {
                return DataTypeEnum::UInt32(FixedValue::None);
            }
            if is_u64(&val) {
                return DataTypeEnum::UInt64(FixedValue::None);
            }
        }

        if val.is_i64() {
            let val = val.as_u64().map(|v| format!("{}", v));
            let d = "0".to_owned();

            let val = val.unwrap_or(d);
            if is_i8(&val) {
                return DataTypeEnum::Int8(FixedValue::None);
            }
            if is_i16(&val) {
                return DataTypeEnum::Int16(FixedValue::None);
            }
            if is_i32(&val) {
                return DataTypeEnum::Int32(FixedValue::None);
            }
            if is_i64(&val) {
                return DataTypeEnum::Int64(FixedValue::None);
            }
        }
    }

    if val.is_f64() {
        return DataTypeEnum::Float64(FixedValue::None);
    }

    if val.is_boolean() {
        return DataTypeEnum::Boolean(FixedValue::None);
    }

    if val.is_null() {
        return DataTypeEnum::Nullable(FixedValue::None);
    }

    let val = val.as_str().unwrap_or("null");

    if is_datetime(val) {
        return DataTypeEnum::DateTime(FixedValue::None);
    }
    if is_ipv6(val) {
        return DataTypeEnum::IPv6(FixedValue::None);
    }
    if is_ipv4(val) {
        return DataTypeEnum::IPv4(FixedValue::None);
    }
    if is_email(val) {
        return DataTypeEnum::Email(FixedValue::None);
    }
    if is_phone(val) {
        return DataTypeEnum::Phone(FixedValue::None);
    }
    if is_null(val) {
        return DataTypeEnum::Nullable(FixedValue::None);
    }
    if is_string(val) {
        return DataTypeEnum::String(FixedValue::None);
    }
    return DataTypeEnum::Unknown;
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::*;

    static SCHEMA_PATH: &'static str = "examples/schema.json";

    #[test]
    fn test_read_schema() {
        let path_buf = PathBuf::from(SCHEMA_PATH);

        let res = fs::read_to_string(&path_buf);

        match res {
            Ok(res) => {
                println!("read schema file string:{res}");
            }
            Err(e) => panic!("read schema file error:{e}"),
        }
    }

    #[test]
    fn test_parse_schema() {
        let path_buf = PathBuf::from(SCHEMA_PATH);

        let schema = parse_schema(&path_buf);

        match schema {
            Ok(schema) => {
                println!("read schema file to struct:{:#?}", schema);
            }
            Err(e) => panic!("read schema file error:{e}"),
        }
    }

    #[test]
    fn test_parse_datasources_schema() {
        let path_buf = PathBuf::from(SCHEMA_PATH);

        let schema = parse_schema(&path_buf).expect("解析schema文件失败");

        let datasources = parse_datasources_from_schema(schema);

        match datasources {
            Ok(datasource) => {
                println!("parse datasources struct:{:#?}", datasource);
            }
            Err(e) => panic!("read schema file error:{e}"),
        }
    }
}
