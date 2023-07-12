use async_trait::async_trait;

use crate::{
    datasource::{DataSourceChannelStatus, DATA_SOURCE_MANAGER},
    model::{column::DataSourceColumn, schema::DataSourceSchema},
    Json,
};

use super::{
    check::{DEFAULT_BATCH_SIZE, MIN_THREAD_SIZE},
    error::{Error, IoError},
};

pub trait Name {
    fn id(&self) -> &str;

    fn name(&self) -> &str;
}

/// 用于实现一些公共方法
#[async_trait]
pub trait TaskDetailStatic: Name {
    async fn is_shutdown(&self) -> bool {
        DATA_SOURCE_MANAGER
            .read()
            .await
            .is_shutdown(self.id(), self.name())
    }

    async fn update_final_status(
        &mut self,
        status: DataSourceChannelStatus,
        overide: bool,
    ) -> Option<DataSourceChannelStatus> {
        DATA_SOURCE_MANAGER.write().await.update_final_status(
            self.id(),
            self.name(),
            status,
            overide,
        )
    }

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

    async fn count_inner(&self) -> Option<isize> {
        if let Some(schema) = self.schema().await {
            if let Some(channel) = &schema.channel {
                return channel.count;
            }
        }
        None
    }

    async fn count(&self) -> isize {
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

    async fn meta(&self) -> crate::Result<&Json> {
        if let Some(schema) = self.schema().await {
            return Ok(schema.meta.as_ref().unwrap());
        }
        Err(Error::Io(IoError::MetaNotFound))
    }

    async fn columns(&self) -> crate::Result<&Vec<DataSourceColumn>> {
        DATA_SOURCE_MANAGER
            .read()
            .await
            .columns(self.id())
            .ok_or(Error::Io(IoError::UndefinedColumns))
    }
}
