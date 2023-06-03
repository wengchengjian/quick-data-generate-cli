use super::{StaticsLogFactory, StaticsLogger};
use async_trait::async_trait;
use core::fmt::Debug;
use std::str::FromStr;

pub mod clickhouse;
pub mod csv;
pub mod elasticsearch;
pub mod kafka;
pub mod mysql;

#[async_trait]
pub trait Output: Send +  Debug  {
    fn get_logger(&self) -> &StaticsLogger;

    fn set_logger(&mut self, logger: StaticsLogger);

    /// 通用初始化逻辑
    fn init(&mut self) {
        let log_factory = StaticsLogFactory::new();
        let mut logger = StaticsLogger::new(log_factory, self.interval());
        // 注册一个自身的log任务
        let log = StaticsLog::new(self.name(), self.interval());

        logger.register(log);
        self.set_logger(logger);
    }
    
    async fn before_run(&mut self) -> crate::Result<()> {
        Ok(())
    }
    
    async fn after_run(&mut self) -> crate::Result<()> {
        Ok(())
    }
    
    async fn execute(&mut self) -> crate::Result<()> {
        self.before_run().await?;
        self.init();
        let logger = self.get_logger();
        print_log(&logger).await;
        self.run().await?;        
        self.after_run().await
    }

    fn interval(&self) -> usize;

    fn name(&self) -> String;

    async fn run(&mut self) -> crate::Result<()>;
    
}

async fn print_log(logger: &StaticsLogger) {
    logger.log().await
}

#[derive(Debug)]
pub struct DelegatedOutput {
    outputs: Vec<Box<dyn Output>>,
    logger: super::StaticsLogger,
    name: String,
}

#[async_trait]
impl Output for DelegatedOutput {
    fn name(&self) -> String {
        return self.name;
    }

    async fn run(&mut self) -> crate::Result<()> {
        for output in self.outputs {
            self.start_output(&mut output).await?;
        }
        Ok(())
    }
}
use super::log::StaticsLog;

impl DelegatedOutput {
    pub fn new(outputs: Vec<Box<dyn Output>>, logger: super::StaticsLogger) -> Self {
        Self {
            outputs,
            logger,
            name: "delegate".to_string(),
        }
    }

    /// 注册日志并执行任务
    pub async fn start_output(&mut self, output: &mut Box<dyn Output>) -> crate::Result<()> {
        output.run().await
    }
}

#[derive(Debug)]
pub enum OutputEnum {
    ClickHouse,
    //    Mysql,
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
        match s {
            "clickhouse" => Ok(OutputEnum::ClickHouse),
            //            "mysql" => Ok(OutputEnum::Mysql),
            //            "kafka" => Ok(OutputEnum::Kafka),
            //            "elasticsearch" => Ok(OutputEnum::ElasticSearch),
            //            "csv" => Ok(OutputEnum::CSV),
            //            "sqlserver" => Ok(OutputEnum::SqlServer),
            _ => Err("不支持该输出源".into()),
        }
    }
}
