use std::sync::{atomic::AtomicI64, Arc};

use async_trait::async_trait;

use tokio::sync::mpsc::Sender;

use crate::{
    core::{
        error::{Error, IoError},
        fake::get_fake_data,
        traits::{Name, TaskDetailStatic},
    },
    datasource::DATA_SOURCE_MANAGER,
    model::column::DataSourceColumn,
};

use super::Exector;

/// 仅作为生产者输出
#[derive(Debug, Clone)]
pub struct FakeTaskExecutor {
    pub task_name: String,
    pub sender: Option<Sender<serde_json::Value>>,
    pub next: usize,
}

impl Name for FakeTaskExecutor {
    fn name(&self) -> &str {
        &self.task_name
    }
}

impl TaskDetailStatic for FakeTaskExecutor {}

impl FakeTaskExecutor {
    pub fn new(task_name: String, sender: Option<Sender<serde_json::Value>>) -> Self {
        Self {
            task_name,
            sender,
            next: 0,
        }
    }
}

#[async_trait]
impl Exector for FakeTaskExecutor {
    fn sender(&mut self) -> Option<&mut Sender<serde_json::Value>> {
        return self.sender.as_mut();
    }

    async fn handle_fetch(&mut self) -> crate::Result<Vec<serde_json::Value>> {
        let mut datas = vec![];
        let columns = DATA_SOURCE_MANAGER
            .columns(self.name())
            .await
            .ok_or(Error::Io(IoError::UndefinedColumns))?;
        for _ in 0..self.batch().await {
            datas.push(get_fake_data(&columns));
        }

        Ok(datas)
    }
}
