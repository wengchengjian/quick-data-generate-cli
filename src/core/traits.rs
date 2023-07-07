use async_trait::async_trait;

use crate::{
    datasource::DATA_SOURCE_MANAGER,
    model::{column::DataSourceColumn, schema::DataSourceSchema},
    Json,
};

use super::{
    check::{DEFAULT_BATCH_SIZE, MIN_THREAD_SIZE},
};

pub trait Name {
    fn name(&self) -> &str;
}

/// 用于实现一些公共方法
#[async_trait]
pub trait TaskDetailStatic: Name {
    async fn schema(&self) -> Option<DataSourceSchema> {
        DATA_SOURCE_MANAGER
            .read()
            .await
            .get_schema(self.name())
            .cloned()
    }

    async fn batch(&self) -> usize {
        if let Some(schema) = self.schema().await {
            if let Some(channel) = &schema.channel {
                return channel.batch.unwrap_or(DEFAULT_BATCH_SIZE);
            }
        }
        0
    }

    async fn count(&self) -> usize {
        if let Some(schema) = self.schema().await {
            if let Some(channel) = &schema.channel {
                return channel.count.unwrap_or(0);
            }
        }
        0
    }

    async fn concurrency(&self) -> usize {
        if let Some(schema) = self.schema().await {
            if let Some(channel) = schema.channel.as_ref() {
                return channel.concurrency.unwrap_or(MIN_THREAD_SIZE);
            }
        }
        0
    }

    async fn meta(&self) -> Option<Json> {
        if let Some(schema) = self.schema().await {
            return schema.meta.as_ref().cloned();
        }
        None
    }

    async fn columns(&self) -> Option<Vec<DataSourceColumn>> {
        DATA_SOURCE_MANAGER
            .read()
            .await
            .columns(self.name())
            .cloned()
    }
}
