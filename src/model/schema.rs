use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::output::OutputEnum;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub interval: Option<usize>,

    pub concurrency: Option<usize>,

    pub outputs: HashMap<String, OutputSchema>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputSchema {
    pub output: OutputEnum,

    pub meta: serde_json::Value,

    pub columns: serde_json::Value,

    pub channel: ChannelSchema,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelSchema {
    // 批量提交数量
    pub batch: usize,
    pub concurrency: usize,
    pub count: usize,
}
