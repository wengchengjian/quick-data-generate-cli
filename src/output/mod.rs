use super::{StaticsLogFactory, StaticsLogger};
use async_trait::async_trait;
use core::fmt::Debug;
use std::{str::FromStr, sync::Arc};
use tokio::sync::Semaphore;

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
pub trait Output: Send + Close + Sync {
    fn get_logger(&self) -> &StaticsLogger;

    fn set_logger(&mut self, logger: StaticsLogger);

    /// 通用初始化逻辑
    fn init(&mut self, context: &mut OutputContext) {
        let mut logger = StaticsLogger::new(self.interval());
        // 注册一个自身的log任务
        let log = StaticsLog::new(self.name().to_string(), self.interval());

        logger.register(log);
        self.set_logger(logger);
    }

    async fn before_run(&mut self, context: &mut OutputContext) -> crate::Result<()> {
        Ok(())
    }

    async fn after_run(&mut self, context: &mut OutputContext) -> crate::Result<()> {
        Ok(())
    }

    async fn execute(&mut self, context: &mut OutputContext) -> crate::Result<()> {
        self.before_run(context).await?;
        self.init(context);
        self.run(context).await?;
        self.get_logger().log().await;
        self.after_run(context).await
    }

    fn interval(&self) -> usize;

    fn name(&self) -> &str;

    async fn run(&mut self, context: &mut OutputContext) -> crate::Result<()>;
}

pub struct DelegatedOutput {
    outputs: Vec<Box<dyn Output>>,
    logger: super::StaticsLogger,
    name: String,
    interval: usize,
}

#[async_trait]
impl Close for DelegatedOutput {
    async fn close(&mut self) -> crate::Result<()> {
        let outputs = &mut self.outputs;

        for output in outputs {
            output.close().await?;
        }
        // 关闭日志输出
        self.logger.close();

        Ok(())
    }
}

#[async_trait]
impl Output for DelegatedOutput {
    fn get_logger(&self) -> &StaticsLogger {
        &self.logger
    }

    fn set_logger(&mut self, logger: StaticsLogger) {
        self.logger = logger;
    }

    fn name(&self) -> &str {
        return &self.name;
    }

    fn interval(&self) -> usize {
        self.interval
    }

    async fn run(&mut self, context: &mut OutputContext) -> crate::Result<()> {
        let outputs = &mut self.outputs;

        for output in outputs {
            output.run(context).await?;
        }
        Ok(())
    }
}
use super::log::StaticsLog;

impl DelegatedOutput {
    pub fn new(
        outputs: Vec<Box<dyn Output>>,
        logger: super::StaticsLogger,
        interval: usize,
    ) -> Self {
        Self {
            outputs,
            logger,
            name: "delegate".to_string(),
            interval,
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
    pub concurrency: Arc<Semaphore>,
}

impl OutputContext {
    pub fn new(concurrency: usize) -> Self {
        Self {
            concurrency: Arc::new(Semaphore::new(concurrency)),
        }
    }
}

#[derive(Debug, Clone, Copy)]
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
