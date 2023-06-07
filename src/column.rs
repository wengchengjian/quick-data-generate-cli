use std::str::FromStr;

use fake::locales::Data;
use serde::{Deserialize, Serialize};
use time::{Date, OffsetDateTime};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutputColumn {
    pub name: String,
    pub data_type: DataTypeEnum,
}

impl OutputColumn {
    pub fn new(name: &str, data_type: &str) -> Self {
        let name = name.to_string();
        let data_type = DataTypeEnum::from_str(data_type).unwrap();
        Self { name, data_type }
    }
}

impl OutputColumn {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataTypeEnum {
        &self.data_type
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataTypeEnum {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Email,
    Password,
    Username,
    Word,
    Sentence,
    Paragraph,
    City,
    Country,
    Phone,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    FixedString,
    Date,
    Time,
    Timestamp,
    DateTime,
    DateTime64,
    Nullable,
    UUID,
    IPv4,
    IPv6,
    Unknown,
}

impl DataTypeEnum {
    pub fn from_string(str: String) -> crate::Result<Self> {
        let data_type = DataTypeEnum::from_str(str.as_str())?;
        Ok(data_type)
    }
}
use regex::Regex;

lazy_static! {
    pub static ref DATA_TYPE_REGEX: Regex = Regex::new(r"(?P<tp>\w+)(\((\d+)\))?\s*(?P<ex>\w*)").unwrap();
}

impl FromStr for DataTypeEnum {
    type Err = Box<dyn std::error::Error>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s: String = s.to_uppercase();
        let match_str = s.as_str();
        let caps = DATA_TYPE_REGEX.captures(match_str).unwrap();
        let mut tp_str = String::from(&caps["tp"]);

        if &caps["ex"].len() > &0 {
            tp_str = format!("{} {}", &caps["tp"], &caps["ex"]);
        } 
        match tp_str.as_str() {
            "TINYINT UNSIGNED" => Ok(DataTypeEnum::UInt8),
            "TINYINT" => Ok(DataTypeEnum::Int8),
            "SMALLINT" => Ok(DataTypeEnum::Int16),
            "SMALLINT UNSIGNED" => Ok(DataTypeEnum::UInt16),
            "MEDIUMINT" => Ok(DataTypeEnum::Int32),
            "MEDIUMINT UNSIGNED" => Ok(DataTypeEnum::UInt32),
            "INT" => Ok(DataTypeEnum::Int32),
            "INT UNSIGNED" => Ok(DataTypeEnum::Int32),
            "BIGINT" => Ok(DataTypeEnum::Int64),
            "BIGINT UNSIGNED" => Ok(DataTypeEnum::UInt64),
            "FLOAT" => Ok(DataTypeEnum::Float32),
            "DOUBLE" => Ok(DataTypeEnum::Float64),
            "DATE" => Ok(DataTypeEnum::Date),
            "TIME" => Ok(DataTypeEnum::Time),
            "DATETIME" => Ok(DataTypeEnum::DateTime),
            "TIMESTAMP" => Ok(DataTypeEnum::Timestamp),
            "CHAR" => Ok(DataTypeEnum::String),
            "VARCHAR" => Ok(DataTypeEnum::String),
            "TINYBLOB" => Ok(DataTypeEnum::String),
            "TINYTEXT" => Ok(DataTypeEnum::String),
            "BLOB" => Ok(DataTypeEnum::String),
            "TEXT" => Ok(DataTypeEnum::String),
            "MEDIUMBLOB" => Ok(DataTypeEnum::String),
            "MEDIUMTEXT" => Ok(DataTypeEnum::String),
            "LONGBLOB" => Ok(DataTypeEnum::String),
            "LONGTEXT" => Ok(DataTypeEnum::String),
            _ => Ok(DataTypeEnum::Unknown),
        }
    }
}
