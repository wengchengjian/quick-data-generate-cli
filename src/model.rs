use std::net::{Ipv4Addr, Ipv6Addr};

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Debug, Deserialize)]
pub struct Column {
    name: String,
    data_type: ColumnDataType,
    default: Option<String>,
    value: Option<String>,
    is_nullable: bool,
    is_primary: bool,
    is_unique: bool,
    is_auto_increment: bool,
    is_index: bool,
}

#[derive(Clone, Serialize, Debug, Deserialize)]
pub enum ColumnDataType {
    String(String),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Date(String),
    Timestamp(u64),
    DateTime(String),
    Enum(String),
    Array(Vec<ColumnDataType>),
    Boolean(bool),
    Ipv4Addr(Ipv4Addr),
    Ipv6Addr(Ipv6Addr),
}

pub struct Row {
    columns: Vec<Column>,
}
