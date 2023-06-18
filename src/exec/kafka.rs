use std::sync::{atomic::AtomicI64, Arc};

use async_trait::async_trait;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use tokio::sync::Mutex;

use crate::{
    core::{fake::get_random_string, limit::token::TokenBuketLimiter},
    model::column::OutputColumn,
};

use super::Exector;

#[derive(Clone)]
pub struct KafkaTaskExecutor {
    pub batch: usize,
    pub count: Option<Arc<AtomicI64>>,
    pub columns: Vec<OutputColumn>,
    pub task_name: String,
    pub limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    pub producer: FutureProducer,
    pub topic: String,
}
#[async_trait]
impl Exector for KafkaTaskExecutor {
    fn batch(&self) -> usize {
        return self.batch;
    }
    fn columns(&self) -> &Vec<OutputColumn> {
        return &self.columns;
    }

    fn is_multi_handle(&self) -> bool {
        return false;
    }

    fn limiter(&mut self) -> Option<&mut Arc<Mutex<TokenBuketLimiter>>> {
        return self.limiter.as_mut();
    }

    fn count(&mut self) -> Option<&Arc<AtomicI64>> {
        return self.count.as_ref();
    }

    fn name(&self) -> &str {
        return &self.task_name;
    }

    async fn handle_batch(&mut self, _v: Vec<serde_json::Value>) -> crate::Result<()> {
        // nothing
        Ok(())
    }

    async fn handle_single(&mut self, data: &mut serde_json::Value) -> crate::Result<()> {
        let key = get_random_string();
        match serde_json::to_string(data) {
            Ok(data_str) => {
                if data_str.len() == 0 {
                    return Ok(());
                }
                self.producer
                    .send(
                        FutureRecord::to(self.topic.as_str())
                            .key(&key)
                            .payload(&data_str),
                        Timeout::Never,
                    )
                    .await
                    .expect("发送kafka数据失败");

                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}
