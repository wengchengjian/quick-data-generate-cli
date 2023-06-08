use std::path::PathBuf;
use crate::error::Result;

use crate::schema::OutputSchema;
use crate::{schema::Schema, output::{self, mysql::MysqlOutput}, cli::Cli};

pub fn parse_schema(path: &PathBuf) -> Result<Schema> {
    let data = std::fs::read(path).expect("读取schema文件失败");
    let schema: Schema = serde_json::from_slice(&data).expect("解析schema文件失败");

    Ok(schema)
}

pub fn parse_output_from_schema(name: String, schema: OutputSchema) -> Result<Box<dyn output::Output>> {
    match schema.output {
        output::OutputEnum::Mysql => Ok(Box::new(MysqlOutput::try_from(schema)?)),
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
pub fn parse_output(cli: Cli) -> Result<(Vec<Box<dyn output::Output>>, Option<usize>, Option<usize>)> {
    let mut cli = cli;
    let mut outputs = vec![];
    let mut interval = cli.interval;
    let mut concurrency = cli.concurrency;

    if let Some(schema_path) = cli.schema {
        let schema = parse_schema(&schema_path).unwrap();

        let mut schema_outputs = parse_outputs_from_schema(schema)?;

        outputs.append(&mut schema_outputs);

        if let Some(schema_interval) = schema.interval {
            if let None = interval {
                cli.interval.insert(schema_interval);
            }
        }

        if let Some(schema_concurrency) = schema.concurrency {
            if let None = concurrency {
                cli.concurrency.insert(schema_concurrency);
            }
        }
    }

    if let Some(output) = parse_output_from_cli(cli) {
        outputs.push(output);
    }
    
    return Ok((outputs, interval, concurrency));
}