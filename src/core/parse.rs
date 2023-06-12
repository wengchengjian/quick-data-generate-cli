use super::{
    check::{DEFAULT_INTERVAL, MIN_THREAD_SIZE},
    cli::Cli,
    error::{Error, Result},
};
use std::path::PathBuf;

use crate::{
    impl_func_is_primitive_by_parse,
    model::{
        column::DataTypeEnum,
        schema::{OutputSchema, Schema},
    },
    output::{self, mysql::MysqlOutput},
};

impl_func_is_primitive_by_parse!((is_u8, u8), (is_u16, u16), (is_u32, u32), (is_u64, u64));
impl_func_is_primitive_by_parse!((is_i8, i8), (is_i16, i16), (is_i32, i32), (is_i64, i64));

pub fn parse_schema(path: &PathBuf) -> Result<Schema> {
    let data = std::fs::read(path)?;
    let schema: Schema = serde_json::from_slice(&data).map_err(|err| Error::Other(err.into()))?;
    Ok(schema)
}

pub fn parse_output_from_schema(
    name: String,
    schema: OutputSchema,
) -> Result<Box<dyn output::Output>> {
    match schema.output {
        output::OutputEnum::Mysql => {
            let mut output = MysqlOutput::try_from(schema)?;
            output.name = name;
            Ok(Box::new(output))
        }
    }
}

pub fn parse_outputs_from_schema(schema: Schema) -> Result<Vec<Box<dyn output::Output>>> {
    let mut outputs = vec![];

    for (key, value) in schema.outputs {
        outputs.push(parse_output_from_schema(key, value)?);
    }
    Ok(outputs)
}

pub fn parse_output_from_cli(cli: Cli) -> Option<Box<dyn output::Output>> {
    let output_enum = cli.output;

    let output = match output_enum {
        Some(output) => match output {
            output::OutputEnum::Mysql => TryInto::<MysqlOutput>::try_into(cli),
        },
        None => {
            return None;
        }
    };
    if let Ok(output) = output {
        let res = Box::new(output);
        Some(res)
    } else {
        None
    }
}

/// 返回解析后的输出源，interval，concurrency, 以cli为准
pub fn parse_output(cli: Cli) -> Result<(Vec<Box<dyn output::Output>>, usize, usize)> {
    let mut cli = cli;
    let mut outputs = vec![];
    let interval = cli.interval;
    let mut concurrency = cli.concurrency;

    if let Some(schema_path) = &cli.schema {
        let schema = parse_schema(schema_path).unwrap();

        if let Some(schema_interval) = schema.interval {
            if let None = interval {
                let _ = cli.interval.insert(schema_interval);
            }
        }

        if let Some(schema_concurrency) = schema.concurrency {
            if let None = concurrency {
                let _ = cli.concurrency.insert(schema_concurrency);
            }
        }

        let mut schema_outputs = parse_outputs_from_schema(schema)?;

        outputs.append(&mut schema_outputs);
    }

    //    let _ = cli.output.insert(output::OutputEnum::Mysql);
    //    let _ = cli.database.insert("tests".to_string());
    //    let _ = cli.table.insert("UPGRADE_PACKET_INFO".to_string());
    //    let _ = cli.user.insert("root".to_string());
    //    let _ = cli.password.insert("wcj520600".to_string());
    //    let _ = cli.batch.insert(10);
    //    let _ = cli.concurrency.insert(1);

    if let Some(output) = parse_output_from_cli(cli) {
        outputs.push(output);
    }

    let interval = interval.unwrap_or(DEFAULT_INTERVAL);

    concurrency = concurrency.and_then(|concurrency| {
        if concurrency < outputs.len() {
            Some(outputs.len())
        } else {
            Some(concurrency)
        }
    });

    let concurrency = concurrency.unwrap_or(MIN_THREAD_SIZE);

    return Ok((outputs, interval, concurrency));
}

use super::check::*;

pub fn parse_type(val: &serde_json::Value) -> DataTypeEnum {
    let val = val;

    if val.is_u64() {
        return DataTypeEnum::UInt64;
    }

    if val.is_i64() {
        return DataTypeEnum::Int64;
    }

    if val.is_f64() {
        return DataTypeEnum::Float64;
    }

    if val.is_boolean() {
        return DataTypeEnum::Boolean;
    }

    if val.is_null() {
        return DataTypeEnum::Nullable;
    }

    let val = val.as_str().unwrap_or("null");

    if is_u8(val) {
        return DataTypeEnum::UInt8;
    }
    if is_u16(val) {
        return DataTypeEnum::UInt16;
    }
    if is_u32(val) {
        return DataTypeEnum::UInt32;
    }
    if is_u64(val) {
        return DataTypeEnum::UInt64;
    }
    if is_i8(val) {
        return DataTypeEnum::Int8;
    }
    if is_i16(val) {
        return DataTypeEnum::Int16;
    }
    if is_i32(val) {
        return DataTypeEnum::Int32;
    }
    if is_i64(val) {
        return DataTypeEnum::Int64;
    }
    if is_datetime(val) {
        return DataTypeEnum::DateTime;
    }
    if is_ipv6(val) {
        return DataTypeEnum::IPv6;
    }
    if is_ipv4(val) {
        return DataTypeEnum::IPv4;
    }
    if is_email(val) {
        return DataTypeEnum::Email;
    }
    if is_phone(val) {
        return DataTypeEnum::Phone;
    }
    if is_null(val) {
        return DataTypeEnum::Nullable;
    }
    if is_string(val) {
        return DataTypeEnum::String;
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
                println!("read schema file to struct:{:?}", schema);
            }
            Err(e) => panic!("read schema file error:{e}"),
        }
    }

    #[test]
    fn test_parse_output_schema() {
        let path_buf = PathBuf::from(SCHEMA_PATH);

        let schema = parse_schema(&path_buf).expect("解析schema文件失败");

        let outputs = parse_outputs_from_schema(schema);

        match outputs {
            Ok(outputs) => {
                println!("parse outputs struct:{:#?}", outputs);
            }
            Err(e) => panic!("read schema file error:{e}"),
        }
    }
}
