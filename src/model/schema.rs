

use serde::{Deserialize, Serialize};

use crate::{datasource::{DataSourceEnum}, core::parse::DEFAULT_FAKE_DATASOURCE, Json};


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub interval: Option<usize>,

    pub sources: Vec<DataSourceSchema>,
}

impl Schema {
    pub fn new(
        interval: Option<usize>,
        sources: Vec<DataSourceSchema>,
    ) -> Self {
        Self {
            interval,
            sources,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataSourceSchema {
    pub name: String,

    pub source: DataSourceEnum,

    pub meta: Option<Json>,

    pub columns: Option<Json>,

    pub channel: Option<ChannelSchema>,

    pub sources: Option<Vec<String>>
}

impl DataSourceSchema {

    pub fn new(name: String, source: DataSourceEnum, meta: serde_json::Value, columns: serde_json::Value, channel: ChannelSchema, sources: Vec<String>) -> Self {
        Self {
            name,
            source,
            meta: Some(meta),
            columns: Some(columns),
            channel: Some(channel),
            sources: Some(sources),
        }
    }

    pub fn fake() -> Self {
        Self {
            name: DEFAULT_FAKE_DATASOURCE.to_owned(),
            source: DataSourceEnum::Fake,
            meta: None,
            columns: None,
            channel: Some(ChannelSchema {
                batch: 5000,
                concurrency: 1,
                count: usize::MAX,
            }),
            sources: None,
        }
    }
}



#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelSchema {
    // 批量提交数量
    pub batch: usize,
    pub concurrency: usize,
    pub count: usize,
}
