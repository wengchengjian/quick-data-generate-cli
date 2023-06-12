use std::{collections::HashMap, str::FromStr};

use crate::core::error::{Error, IoError, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};

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

    /// 返回column的map 映射
    pub fn map_columns(columns: &Vec<OutputColumn>) -> HashMap<&str, &DataTypeEnum> {
        columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect()
    }
    /// 从schema中获取columns定义，允许类型未知
    pub fn get_columns_from_schema(value: &serde_json::Value) -> Vec<OutputColumn> {
        if let Some(map) = value.as_object() {
            return map
                .into_iter()
                .map(|(key, value)| OutputColumn {
                    name: key.clone(),
                    data_type: match DataTypeEnum::from_str(value.as_str().unwrap()) {
                        Ok(dt) => dt,
                        Err(_) => DataTypeEnum::Unknown,
                    },
                })
                .collect();
        } else {
            vec![]
        }
    }

    /// 从schema中获取columns定义，允许类型未知
    pub fn get_columns_from_value(value: &serde_json::Value) -> Vec<OutputColumn> {
        if let Some(map) = value.as_object() {
            return map
                .into_iter()
                .map(|(key, value)| OutputColumn {
                    name: key.clone(),
                    data_type: match DataTypeEnum::from_str(value.as_str().unwrap()) {
                        Ok(dt) => dt,
                        Err(_) => DataTypeEnum::Unknown,
                    },
                })
                .collect();
        } else {
            vec![]
        }
    }

    /// 合并两个列数组,target中已经有的,以target为准
    pub fn merge_columns(
        source: &Vec<OutputColumn>,
        target: &Vec<OutputColumn>,
    ) -> Vec<OutputColumn> {
        let mut c1 = OutputColumn::map_columns(source);
        let c2 = OutputColumn::map_columns(target);

        c1.extend(c2);

        c1.into_iter()
            .map(|(key, val)| OutputColumn {
                name: key.to_string(),
                data_type: val.clone(),
            })
            .collect()
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
    pub fn parse_type_from_str(val: &str) -> DataTypeEnum {
        return parse_type(val);
    }
}

impl DataTypeEnum {
    pub fn from_string(str: String) -> Result<Self> {
        let data_type = DataTypeEnum::from_str(str.as_str())?;
        Ok(data_type)
    }
}

lazy_static! {
    pub static ref DATA_TYPE_REGEX: Regex =
        Regex::new(r"(?P<tp>\w+)(\((\d+)\))?\s*(?P<ex>\w*)").unwrap();
}

impl FromStr for DataTypeEnum {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
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
            "PASSWORD" => Ok(DataTypeEnum::Password),
            "USERNAME" => Ok(DataTypeEnum::Username),
            "EMAIL" => Ok(DataTypeEnum::Email),
            "PHONE" => Ok(DataTypeEnum::Phone),
            "IPV4" => Ok(DataTypeEnum::IPv4),
            "IPV6" => Ok(DataTypeEnum::IPv6),
            "UUID" => Ok(DataTypeEnum::UUID),
            "CITY" => Ok(DataTypeEnum::City),
            "COUNTRY" => Ok(DataTypeEnum::Country),
            at => Err(Error::Io(IoError::UnkownTypeError(at.to_string()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::core::parse::{parse_outputs_from_schema, parse_schema};

    use super::*;

    static SCHEMA_PATH: &'static str = "examples/schema2.json";
    static _SCHEMA_PATH2: &'static str = "examples/schema3.json";

    #[test]
    fn test_merge_columns() {
        let path_buf = PathBuf::from(SCHEMA_PATH);

        let schema = parse_schema(&path_buf).expect("解析schema文件失败");

        let outputs1 = parse_outputs_from_schema(schema).expect("解析output失败");
        let c1 = outputs1[0].get_columns().unwrap();
        let c2 = outputs1[1].get_columns().unwrap();

        let mut _c3 = vec![];
        if outputs1[0].name().eq("mysql-output") {
            _c3 = OutputColumn::merge_columns(c1, c2);
        } else {
            _c3 = OutputColumn::merge_columns(c2, c1);
        }

        let c4m = OutputColumn::map_columns(&_c3);
        assert!((*c4m.get("PACKET_ID").unwrap()).eq(&DataTypeEnum::UInt64));
        assert!((*c4m.get("PACKET_NAME").unwrap()).eq(&DataTypeEnum::String));
        assert!((*c4m.get("ip").unwrap()).eq(&DataTypeEnum::IPv4));
        assert!((*c4m.get("password").unwrap()).eq(&DataTypeEnum::String));
        assert!((*c4m.get("username").unwrap()).eq(&DataTypeEnum::String));
    }
}
