use serde_json::json;
use uuid::Uuid;

use super::{
    check::DEFAULT_INTERVAL,
    cli::Cli,
    error::{Error, IoError, Result},
};
use std::{collections::HashMap, path::PathBuf};

use crate::{
    create_context,
    datasource::{
        self, csv::CsvDataSource, fake::FakeDataSource, kafka::KafkaDataSource,
        mysql::MysqlDataSource, DataSourceChannel, DataSourceContext, DataSourceEnum,
        DataSourceTransferSession, MpscDataSourceChannel, DATA_SOURCE_MANAGER,
    },
    impl_func_is_primitive_by_parse,
    model::{
        column::{parse_json_from_column, DataSourceColumn, DataTypeEnum, FixedValue},
        schema::{ChannelSchema, DataSourceSchema, Schema},
    },
    Json,
};

impl_func_is_primitive_by_parse!((is_u8, u8), (is_u16, u16), (is_u32, u32), (is_u64, u64));
impl_func_is_primitive_by_parse!((is_i8, i8), (is_i16, i16), (is_i32, i32), (is_i64, i64));

pub fn parse_schema(path: &PathBuf) -> Result<Schema> {
    let data = std::fs::read(path)?;
    let schema: Schema = serde_json::from_slice(&data).map_err(|err| Error::Other(err.into()))?;
    Ok(schema)
}

pub fn create_seession_id() -> String {
    return Uuid::new_v4().to_string();
}

pub async fn merge_meta_to_session(session_id: &str, meta: &Json) -> Result<()> {
    let mut data_manager = DATA_SOURCE_MANAGER.write().await;

    if data_manager.sessions.contains_key(session_id) {
        let session = data_manager.sessions.get_mut(session_id).unwrap();

        if let Some(meta_s) = session.meta.as_mut() {
            merge_json(meta, meta_s);
        }
    }

    drop(data_manager);

    Ok(())
}

pub async fn merge_columns_to_session(
    session_id: &str,
    columns: &Vec<DataSourceColumn>,
) -> Result<()> {
    let mut data_manager = DATA_SOURCE_MANAGER.write().await;

    if data_manager.sessions.contains_key(session_id) {
        let session = data_manager.sessions.get_mut(session_id).unwrap();

        if let Some((columns_json_s, columns_s)) = session.columns.as_mut() {
            *columns_s = DataSourceColumn::merge_columns(columns, columns_s);
            *columns_json_s = parse_json_from_column(columns_s);
        }
    }

    drop(data_manager);

    Ok(())
}

/// 合并datasource到当前会话中去
pub async fn merge_schema_to_session(session_id: &str, schema: &DataSourceSchema) -> Result<()> {
    let columns_json = schema.columns();
    let meta = schema.meta();

    let mut data_manager = DATA_SOURCE_MANAGER.write().await;

    if data_manager.sessions.contains_key(session_id) {
        let session = data_manager.sessions.get_mut(session_id).unwrap();

        if let Some(meta) = meta {
            if let Some(meta_s) = session.meta.as_mut() {
                merge_json(meta, meta_s);
            }
        }

        if let Some(columns_json) = columns_json {
            if let Some((columns_json_s, columns_s)) = session.columns.as_mut() {
                merge_json(columns_json, columns_json_s);

                *columns_s = DataSourceColumn::merge_columns(
                    &DataSourceColumn::get_columns_from_value(columns_json),
                    columns_s,
                )
            }
        }
    } else {
        let mut session = DataSourceTransferSession::new(session_id.to_owned(), None, None);
        if let Some(meta) = meta {
            session.meta = Some(meta.clone());
        }
        if let Some(columns) = columns_json {
            session.columns = Some((
                columns.clone(),
                DataSourceColumn::get_columns_from_value(columns),
            ));
        }
        data_manager.sessions.insert(session_id.to_owned(), session);
    }

    drop(data_manager);

    Ok(())
}

pub async fn parse_mpsc_from_schema(
    schema: &DataSourceSchema,
    source_map: HashMap<&str, DataSourceSchema>,
) -> Result<Box<dyn DataSourceChannel>> {
    let session_id = create_seession_id();

    let mut source_map = source_map;

    source_map
        .entry(DEFAULT_FAKE_DATASOURCE)
        .or_insert(merge_datasource_schema_args(
            schema,
            &DataSourceSchema::fake(),
        ));

    let mut producer = vec![];
    let default_sources = vec![DEFAULT_FAKE_DATASOURCE.to_owned()];
    let mut sources: &Vec<String> = schema.sources.as_ref().unwrap_or(&default_sources);

    if sources.len() == 0 {
        sources = &default_sources;
    }

    for source in sources {
        match source_map.get(source.as_str()) {
            Some(source_schema) => {
                if source_schema.name.ne(&schema.name) {
                    let schema = (*source_schema).to_owned();
                    let datasource = parse_datasource_from_schema(schema, &session_id).await?;
                    merge_schema_to_session(&session_id, source_schema).await?;
                    producer.push(datasource);
                }
            }
            None => {
                return Err(Error::Io(IoError::UnkownSourceError(source.clone())));
            }
        }
    }

    let consumer: Box<dyn DataSourceChannel> =
        parse_datasource_from_schema(schema.clone(), &session_id).await?;
    merge_schema_to_session(&session_id, &schema).await?;

    DATA_SOURCE_MANAGER.write().await.put_session_source(
        &session_id,
        schema.name.clone(),
        sources.clone(),
    );

    let data_source_channel = MpscDataSourceChannel::new(producer, consumer);

    return Ok(Box::new(data_source_channel));
}

pub async fn parse_datasource_from_schema(
    schema: DataSourceSchema,
    session_id: &str,
) -> Result<Box<dyn DataSourceChannel>> {
    let mut schema = schema;
    if let None = schema.channel {
        let _ = schema.channel.insert(ChannelSchema::default());
    }
    let data_manager = DATA_SOURCE_MANAGER.read().await;
    let contains_key = data_manager.contains_schema(&schema.name);

    drop(data_manager);

    if !contains_key {
        DATA_SOURCE_MANAGER
            .write()
            .await
            .put_schema(&schema.name, schema.clone());
    }
    match schema.source {
        DataSourceEnum::Mysql => Ok(Box::new(MysqlDataSource::new(schema, session_id))),
        DataSourceEnum::Kafka => Ok(Box::new(KafkaDataSource::new(schema, session_id))),
        DataSourceEnum::Csv => Ok(Box::new(CsvDataSource::new(schema, session_id))),
        DataSourceEnum::Fake => Ok(Box::new(FakeDataSource::new(schema, session_id))),
        //        _ => {
        //            return Err(Error::Io(IoError::UnkownSourceError("source".to_owned())));
        //        }
    }
}

pub const DEFAULT_FAKE_DATASOURCE: &str = "fake_datasource";

///
pub fn merge_json(source: &Json, target: &mut Json) {
    match (source, target) {
        (&Json::Object(ref source), &mut Json::Object(ref mut target)) => {
            for (k, v) in source {
                merge_json(v, target.entry(k.clone()).or_insert(Json::Null));
            }
        }

        (a, b) => {
            if let Json::Null = b {
                *b = a.clone();
            }
        }
    }
}

pub fn merge_datasource_schema_args(
    source: &DataSourceSchema,
    target: &DataSourceSchema,
) -> DataSourceSchema {
    let mut result = target.clone();
    if result.name.eq(source.name.as_str()) {
        return result;
    }
    let source_meta = source.meta.clone().unwrap_or(json!({}));
    let source_columns = source.columns.clone().unwrap_or(json!({}));
    if let Some(meta) = result.meta.as_mut() {
        merge_json(&source_meta, meta);
    } else {
        result.meta = Some(source_meta);
    }
    if let Some(columns) = result.columns.as_mut() {
        merge_json(&source_columns, columns);
    } else {
        result.columns = Some(source_columns);
    }
    return result;
}

pub async fn parse_datasources_from_schema(
    schema: Schema,
) -> Result<Vec<Box<dyn DataSourceChannel>>> {
    let mut outputs = vec![];

    let sources = &schema.sources;

    for source in sources {
        {
            DATA_SOURCE_MANAGER
                .write()
                .await
                .put_schema(&source.name, source.clone());
        }

        // 整合参数
        let source_map: HashMap<&str, DataSourceSchema> = schema
            .sources
            .iter()
            .map(|item| {
                (
                    item.name.as_str(),
                    merge_datasource_schema_args(source, item),
                )
            })
            .collect();

        outputs.push(parse_mpsc_from_schema(source, source_map).await?);
    }
    Ok(outputs)
}

pub async fn parse_datasource_from_cli(
    cli: Cli,
    force: bool,
) -> Result<Option<Vec<Box<dyn DataSourceChannel>>>> {
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

    let meta = cli.source.unwrap().parse_meta_from_cli(cli.clone())?;

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

/// 返回解析后的输出源，interval，concurrency, 以cli为准
pub async fn parse_datasource(
    cli: Cli,
) -> Result<(Vec<Box<dyn DataSourceChannel>>, usize, DataSourceContext)> {
    let mut cli = cli;
    let mut datasources = vec![];
    let interval = cli.interval;

    // let _ = cli.source.insert(DataSourceEnum::Mysql);
    // let _ = cli.topic.insert("FileHttpLogPushService".to_string());
    // cli.host = "192.168.180.217".to_owned();
    // let _ = cli.user.insert("root".to_string());
    // let _ = cli.database.insert("tests".to_string());
    // let _ = cli.table.insert("bfc_model_task".to_string());
    // let _ = cli.password.insert("bfcdb@123".to_string());
    // let _ = cli.batch.insert(1000);
    // let _ = cli.concurrency.insert(1);
    // let _ = cli.interval.insert(1);
    let _ = cli.schema.insert(PathBuf::from("examples/schema.json"));

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

    use serde_json::json;
    use tokio_test::block_on;

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
    fn test_json_merge() {
        let mut a = json!({
            "title": "this is a title",
            "person" : {
                "firstName": "wengs",
            },
            "citys":["chengdu"]
        });

        let b = json!({
            "title": "This is a title",
            "person" : {
                "firstName": "weng",
                "lastName": "chengjian",
                "but": null
            },
            "citys":["chengdu","banas"]
        });

        merge_json(&b, &mut a);
        assert_eq!(a["title"], json!("this is a title"));
        assert_eq!(a["person"]["firstName"], json!("wengs"));
        assert_eq!(a["person"]["lastName"], json!("chengjian"));
        assert_eq!(a["citys"], json!(["chengdu"]));
    }

    #[test]
    fn test_parse_datasources_schema() {
        block_on(async {
            let path_buf = PathBuf::from(SCHEMA_PATH);

            let schema = parse_schema(&path_buf).expect("解析schema文件失败");

            let datasources = parse_datasources_from_schema(schema).await;

            match datasources {
                Ok(datasource) => {
                    println!("parse datasources struct:{:#?}", datasource);
                }
                Err(e) => panic!("read schema file error:{e}"),
            }
        });
    }
}
