use async_trait::async_trait;
use core::fmt::Debug;
use serde::{Deserialize, Serialize};
use std::{str::FromStr};

use crate::model::column::OutputColumn;


pub mod clickhouse;
pub mod csv;
pub mod elasticsearch;
pub mod kafka;
pub mod mysql;

#[async_trait]
pub trait Close {
    async fn close(&mut self) -> crate::Result<()>;
}

#[async_trait]
pub trait Output: Send + Close + Sync + Debug {

    fn get_columns(&self) -> Option<&Vec<OutputColumn>>;

    /// 通用初始化逻辑
    fn init(&mut self, _context: &mut OutputContext) {}

    async fn before_run(&mut self, _context: &mut OutputContext) -> crate::Result<()> {
        Ok(())
    }

    async fn after_run(&mut self, _context: &mut OutputContext) -> crate::Result<()> {
        Ok(())
    }

    async fn execute(&mut self, context: &mut OutputContext) -> crate::Result<()> {
        self.before_run(context).await?;
        self.init(context);
        self.run(context).await?;
        self.after_run(context).await
    }

    fn name(&self) -> &str;

    async fn run(&mut self, context: &mut OutputContext) -> crate::Result<()>;
}

#[derive(Debug)]
pub struct DelegatedOutput {
    outputs: Vec<Box<dyn Output>>,
    name: String,
}

#[async_trait]
impl Close for DelegatedOutput {
    async fn close(&mut self) -> crate::Result<()> {
        let outputs = &mut self.outputs;

        for output in outputs {
            output.close().await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Output for DelegatedOutput {

    fn get_columns(&self) -> Option<&Vec<OutputColumn>> {
        return None;
    }


    fn name(&self) -> &str {
        return &self.name;
    }

    async fn run(&mut self, context: &mut OutputContext) -> crate::Result<()> {
        let outputs = &mut self.outputs;

        for output in outputs {
            output.run(context).await?;
        }
        Ok(())
    }
}

impl DelegatedOutput {
    pub fn new(outputs: Vec<Box<dyn Output>>) -> Self {
        Self {
            outputs,
            name: "delegate".to_string(),
        }
    }

    /// 注册日志并执行任务
    pub async fn start_output(
        &mut self,
        output: &mut Box<dyn Output>,
        context: &mut OutputContext,
    ) -> crate::Result<()> {
        output.run(context).await
    }
}


pub struct OutputContext {
    pub concurrency:usize
}

impl OutputContext {
    pub fn new(concurrency:usize) -> Self {
        Self {
            concurrency
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OutputEnum {
    // ClickHouse,
    Mysql,
    //
    //    Kafka,
    //
    //    ElasticSearch,
    //
    //    CSV,
    //
    //    SqlServer,
}

impl FromStr for OutputEnum {
    type Err = Box<dyn std::error::Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();

        let s = s.as_str().to_lowercase();
        let s = s.as_str();

        match s {
            // "clickhouse" => Ok(OutputEnum::ClickHouse),
            "mysql" => Ok(OutputEnum::Mysql),
            //            "kafka" => Ok(OutputEnum::Kafka),
            //            "elasticsearch" => Ok(OutputEnum::ElasticSearch),
            //            "csv" => Ok(OutputEnum::CSV),
            //            "sqlserver" => Ok(OutputEnum::SqlServer),
            _ => Err("不支持该输出源".into()),
        }
    }
}
