

use serde::{Deserialize, Serialize};

use crate::datasource::{DataSourceEnum};


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

    pub meta: serde_json::Value,

    pub columns: serde_json::Value,

    pub channel: ChannelSchema,

    pub sources: Vec<String>
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelSchema {
    // 批量提交数量
    pub batch: usize,
    pub concurrency: usize,
    pub count: usize,
}
