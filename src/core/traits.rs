use async_trait::async_trait;

use crate::{
    datasource::{DataSourceChannelStatus, DataSourceTransferSession, DATA_SOURCE_MANAGER},
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
    async fn is_producer(&self) -> Option<bool> {
        let session = DATA_SOURCE_MANAGER
            .read()
            .await
            .sessions
            .get(self.id())?
            .clone();
        return Some(!session.consumer_sources.get(self.name()).is_some());
    }

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
        DEFAULT_BATCH_SIZE
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
        MIN_THREAD_SIZE
    }

    async fn session(&self) -> Option<DataSourceTransferSession> {
        DATA_SOURCE_MANAGER
            .read()
            .await
            .sessions
            .get(self.id())
            .cloned()
    }

    async fn meta_all(&self) -> crate::Result<Json> {
        if let Some(schema) = self.schema().await {
            if let Some(meta) = schema.meta.as_ref() {
                return Ok(meta.clone());
            }
        }
        Err(Error::Io(IoError::MetaNotFound))
    }

    async fn meta(&self, name: &'static str) -> crate::Result<Json> {
        if let Some(schema) = self.schema().await {
            let meta = schema.meta.as_ref().unwrap().get(name);
            match meta {
                Some(meta) => {
                    return Ok(meta.clone());
                }
                None => {
                    let session = self
                        .session()
                        .await
                        .ok_or(Error::Io(IoError::SessionNotFound))?;
                    if let Some(meta) = session.meta {
                        if let Some(meta) = meta.get(name) {
                            return Ok(meta.clone());
                        }
                    }
                }
            }
        }
        Err(Error::Io(IoError::ArgNotFound(name)))
    }

    async fn columns_json(&self) -> crate::Result<Json> {
        DATA_SOURCE_MANAGER
            .read()
            .await
            .columns_json(self.id())
            .cloned()
            .ok_or(Error::Io(IoError::UndefinedColumns))
    }

    async fn columns(&self) -> crate::Result<Vec<DataSourceColumn>> {
        DATA_SOURCE_MANAGER
            .read()
            .await
            .columns(self.id())
            .cloned()
            .ok_or(Error::Io(IoError::UndefinedColumns))
    }
}
