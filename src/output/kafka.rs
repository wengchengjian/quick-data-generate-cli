use async_trait::async_trait;
use crate::core::error::Result;

use super::{Close};

#[derive(Debug)]
pub struct KafkaOutput {
    pub name: String,
}


#[async_trait]
impl Close for KafkaOutput {
    async fn close(&mut self) ->  Result<()> {
        todo!()
    }
}
