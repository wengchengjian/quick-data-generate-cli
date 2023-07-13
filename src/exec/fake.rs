use async_trait::async_trait;

use tokio::sync::mpsc::Sender;

use crate::{
    core::{
        error::{Error, IoError},
        fake::get_fake_data,
        traits::{Name, TaskDetailStatic},
    },
    datasource::DATA_SOURCE_MANAGER,
};

use super::Exector;

/// 仅作为生产者输出
#[derive(Debug, Clone)]
pub struct FakeTaskExecutor {
    pub id: String,
    pub task_name: String,
    pub sender: Option<Sender<serde_json::Value>>,
    pub next: usize,
}

impl Name for FakeTaskExecutor {
    fn name(&self) -> &str {
        &self.task_name
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl TaskDetailStatic for FakeTaskExecutor {}

impl FakeTaskExecutor {
    pub fn new(pid: String, task_name: String, sender: Option<Sender<serde_json::Value>>) -> Self {
        Self {
            id: pid,
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
        let data_manager = DATA_SOURCE_MANAGER.read().await;
        let columns = data_manager
            .columns(self.id())
            .cloned()
            .ok_or(Error::Io(IoError::UndefinedColumns))?;
        // 解锁
        drop(data_manager);

        if columns.len() == 0 {
            return Err(Error::Io(IoError::UndefinedColumns));
        }
        for _ in 0..self.batch().await {
            datas.push(get_fake_data(&columns));
        }

        Ok(datas)
    }
}
