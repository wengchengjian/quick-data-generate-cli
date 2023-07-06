use std::sync::{
    atomic::{AtomicI64, AtomicU64},
    Arc,
};

use async_trait::async_trait;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use tokio::sync::Mutex;

use crate::{
    core::{
        error::{Error, IoError},
        fake::get_random_string,
        limit::token::TokenBuketLimiter,
        traits::{Name, TaskDetailStatic},
    },
    model::column::DataSourceColumn,
};

use super::Exector;

#[derive(Clone)]
pub struct KafkaTaskExecutor {
    pub task_name: String,
    pub count_rc: Option<Arc<AtomicI64>>,
    pub limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    pub producer: FutureProducer,
}

impl Name for KafkaTaskExecutor {
    fn name(&self) -> &str {
        &self.task_name
    }
}

impl KafkaTaskExecutor {
    pub async fn topic(&self) -> crate::Result<&str> {
        let topic = self
            .meta().await
            .ok_or(Error::Io(IoError::ArgNotFound("topic")))?
            .as_str()
            .ok_or(Error::Io(IoError::ArgNotFound("topic")))?;
        return Ok(topic);
    }
}

impl TaskDetailStatic for KafkaTaskExecutor {}

#[async_trait]
impl Exector for KafkaTaskExecutor {
    fn limiter(&mut self) -> Option<&mut Arc<Mutex<TokenBuketLimiter>>> {
        return self.limiter.as_mut();
    }
    fn count_rc(&self) -> Option<Arc<AtomicI64>> {
        return self.count_rc.clone();
    }

    fn is_multi_handle(&self) -> bool {
        return false;
    }

    async fn handle_batch(&mut self, _v: Vec<serde_json::Value>) -> crate::Result<()> {
        // nothing
        Ok(())
    }

    async fn handle_single(&mut self, data: &mut serde_json::Value) -> crate::Result<()> {
        let key = get_random_string();
        let topic = self.topic().await?;
        match serde_json::to_string(data) {
            Ok(data_str) => {
                if data_str.len() == 0 {
                    return Ok(());
                }
                self.producer
                    .send(
                        FutureRecord::to(topic).key(&key).payload(&data_str),
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
