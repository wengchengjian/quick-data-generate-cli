use crate::core::parse::DEFAULT_FAKE_DATASOURCE;
use crate::datasource::DataSourceEnum;
use crate::Json;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub interval: Option<usize>,

    pub sources: Vec<DataSourceSchema>,
}

impl Schema {
    pub fn new(interval: Option<usize>, sources: Vec<DataSourceSchema>) -> Self {
        Self { interval, sources }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataSourceSchema {
    pub id: Option<u32>,

    pub name: String,

    pub source: DataSourceEnum,

    pub meta: Option<Json>,

    pub columns: Option<Json>,

    pub channel: Option<ChannelSchema>,

    pub sources: Option<Vec<String>>,

    pub create_time: Option<time::OffsetDateTime>,

    pub update_time: Option<time::OffsetDateTime>,

    pub deleted: Option<u8>,
}

impl DataSourceSchema {
    pub fn new(
        id: u32,
        name: String,
        source: DataSourceEnum,
        meta: Option<Json>,
        columns: Option<Json>,
        channel: Option<ChannelSchema>,
        sources: Option<Vec<String>>,
        create_time: time::OffsetDateTime,
        update_time: time::OffsetDateTime,
    ) -> Self {
        Self {
            id: Some(id),
            name,
            source,
            meta,
            columns,
            channel,
            sources,
            create_time: Some(create_time),
            update_time: Some(update_time),
            deleted: Some(0),
        }
    }

    pub fn meta(&self) -> Option<&Json> {
        return self.meta.as_ref();
    }

    pub fn columns(&self) -> Option<&Json> {
        return self.columns.as_ref();
    }
    pub fn channel(&self) -> Option<&ChannelSchema> {
        return self.channel.as_ref();
    }
    pub fn sources(&self) -> Option<&Vec<String>> {
        return self.sources.as_ref();
    }
    pub fn fake() -> Self {
        Self {
            id: Some(0),
            name: DEFAULT_FAKE_DATASOURCE.to_owned(),
            source: DataSourceEnum::Fake,
            meta: None,
            columns: None,
            channel: Some(ChannelSchema::default()),
            sources: None,
            create_time: Some(time::OffsetDateTime::now_utc()),
            update_time: Some(time::OffsetDateTime::now_utc()),
            deleted: Some(0),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelSchema {
    // 批量提交数量
    pub batch: Option<usize>,
    pub concurrency: Option<usize>,
    pub count: Option<isize>,
}

impl ChannelSchema {
    pub fn default() -> Self {
        Self {
            batch: Some(1000),
            concurrency: Some(1),
            count: Some(isize::MAX),
        }
    }
}
