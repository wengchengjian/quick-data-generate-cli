use serde::{Deserialize, Serialize};

use crate::{core::parse::DEFAULT_FAKE_DATASOURCE, datasource::DataSourceEnum, Json};

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataSourceSchema {
    pub name: String,

    pub source: DataSourceEnum,

    pub meta: Option<Json>,

    pub columns: Option<Json>,

    pub channel: Option<ChannelSchema>,

    pub sources: Option<Vec<String>>,
}

impl DataSourceSchema {
    pub fn new(
        name: String,
        source: DataSourceEnum,
        meta: Option<Json>,
        columns: Option<Json>,
        channel: Option<ChannelSchema>,
        sources: Option<Vec<String>>,
    ) -> Self {
        Self {
            name,
            source,
            meta,
            columns,
            channel,
            sources,
        }
    }

    pub fn fake() -> Self {
        Self {
            name: DEFAULT_FAKE_DATASOURCE.to_owned(),
            source: DataSourceEnum::Fake,
            meta: None,
            columns: None,
            channel: Some(ChannelSchema {
                batch: Some(5000),
                concurrency: Some(1),
                count: Some(usize::MAX),
            }),
            sources: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelSchema {
    // 批量提交数量
    pub batch: Option<usize>,
    pub concurrency: Option<usize>,
    pub count: Option<usize>,
}

impl ChannelSchema {
    pub fn default() -> Self {
        Self {
            batch: Some(1000),
            concurrency: Some(1),
            count: Some(usize::MAX),
        }
    }
}
