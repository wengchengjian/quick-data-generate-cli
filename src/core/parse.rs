use super::{
    check::{DEFAULT_INTERVAL, MIN_THREAD_SIZE},
    cli::Cli,
    error::{Result, Error, IoError},
};
use std::path::PathBuf;

use crate::{
    model::schema::{OutputSchema, Schema},
    output::{self, mysql::MysqlOutput},
};

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
        },
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
        // output::OutputEnum::ClickHouse => {
        //     // 初始化输出源
        //     let mut output = ClickHouseOutput::new(cli);
        //     output
        // }
        output::OutputEnum::Mysql => TryInto::<MysqlOutput>::try_into(cli),
        //        output::OutputEnum::Kafka => todo!(),
        //        output::OutputEnum::ElasticSearch => todo!(),
        //        output::OutputEnum::CSV => todo!(),
        //        output::OutputEnum::SqlServer => todo!(),
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

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::{parse_schema, parse_outputs_from_schema};

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
