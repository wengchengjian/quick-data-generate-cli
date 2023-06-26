

use async_trait::async_trait;

use tokio::sync::{mpsc::{Sender}};

use crate::{
    core::{fake::get_fake_data},
    model::column::DataSourceColumn,
};

use super::Exector;

/// 仅作为生产者输出
#[derive(Debug, Clone)]
pub struct FakeTaskExecutor {
    pub batch: usize,
    pub columns: Vec<DataSourceColumn>,
    pub task_name: String,
    pub sender: Option<Sender<serde_json::Value>>,
    pub next: usize
}

impl FakeTaskExecutor {
    pub fn new(
        batch: usize,
        columns: Vec<DataSourceColumn>,
        task_name: String,
        sender: Option<Sender<serde_json::Value>>
    ) -> Self {
        Self {
            batch,
            columns,
            task_name,
            sender,
            next: 0
        }
    }
}

#[async_trait]
impl Exector for FakeTaskExecutor {
    fn batch(&self) -> usize {
        return self.batch;
    }
    fn columns(&self) -> &Vec<DataSourceColumn> {
        return &self.columns;
    }

    fn name(&self) -> &str {
        return &self.task_name;
    }

    fn sender(&mut self) -> Option<&mut Sender<serde_json::Value>> {
        return self.sender.as_mut();
    }

    async fn handle_fetch(&mut self) -> crate::Result<Vec<serde_json::Value>> {
        
        let mut datas = vec![];
        
        for _ in 0..self.batch {
            datas.push(get_fake_data(&self.columns));
        }
      
        Ok(datas)
    }
}
