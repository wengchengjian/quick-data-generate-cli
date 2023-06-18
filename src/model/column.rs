use std::{collections::HashMap, str::FromStr};

use crate::core::{
    error::{Error, IoError, Result},
    parse::parse_type,
};
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
                    data_type: DataTypeEnum::parse_type_from_value(value),
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

///UInt8(8)
///UInt8([8,8,4])
///UInt8(8..4)

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FixedValue {
    Single(String),
    Array(Vec<String>),
    Range(String, String),
    None,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataTypeEnum {
    UInt8(FixedValue),
    UInt16(FixedValue),
    UInt32(FixedValue),
    UInt64(FixedValue),
    Int8(FixedValue),
    Int16(FixedValue),
    Email(FixedValue),
    Password(FixedValue),
    Username(FixedValue),
    Word(FixedValue),
    Sentence(FixedValue),
    Paragraph(FixedValue),
    City(FixedValue),
    Country(FixedValue),
    Phone(FixedValue),
    Int32(FixedValue),
    Int64(FixedValue),
    Float32(FixedValue),
    Float64(FixedValue),
    String(FixedValue),
    FixedString(FixedValue),
    Date(FixedValue),
    Time(FixedValue),
    Timestamp(FixedValue),
    DateTime(FixedValue),
    DateTime64(FixedValue),
    Nullable(FixedValue),
    UUID(FixedValue),
    IPv4(FixedValue),
    IPv6(FixedValue),
    Boolean(FixedValue),

    Unknown,
}

impl DataTypeEnum {
    pub fn parse_type_from_value(val: &serde_json::Value) -> DataTypeEnum {
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
        Regex::new(r"(?P<tp>\w+)(\((?P<range>.+)\))?\s*(?P<ex>\w*)").unwrap();
}

pub fn parse_fixed_value(val: &str) -> FixedValue {
    if val.len() == 0 {
        return FixedValue::None;
    }

    if val.contains(',') {
        let vals: Vec<String> = val.split(',').map(|s| s.to_owned()).collect();
        return FixedValue::Array(vals);
    }

    if val.contains("..") {
        let vals: Vec<String> = val.split(',').map(|s| s.to_owned()).collect();
        if vals.len() != 2 {
            return FixedValue::Single(val.to_owned());
        }
        return FixedValue::Range(vals[0].to_owned(), vals[1].to_owned());
    }

    return FixedValue::Single(val.to_owned());
}

impl FromStr for DataTypeEnum {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s: String = s.to_uppercase();
        let match_str = s.as_str();
        let caps = DATA_TYPE_REGEX.captures(match_str).unwrap();
        let mut tp_str = String::from(&caps["tp"]);
        let range = &caps["range"];
        let fixed_value: FixedValue = parse_fixed_value(range);
        if &caps["ex"].len() > &0 {
            tp_str = format!("{} {}", &caps["tp"], &caps["ex"]);
        }
        match tp_str.as_str() {
            "TINYINT UNSIGNED" => Ok(DataTypeEnum::UInt8(fixed_value)),
            "TINYINT" => Ok(DataTypeEnum::Int8(fixed_value)),
            "SMALLINT" => Ok(DataTypeEnum::Int16(fixed_value)),
            "SMALLINT UNSIGNED" => Ok(DataTypeEnum::UInt16(fixed_value)),
            "MEDIUMINT" => Ok(DataTypeEnum::Int32(fixed_value)),
            "MEDIUMINT UNSIGNED" => Ok(DataTypeEnum::UInt32(fixed_value)),
            "INT" => Ok(DataTypeEnum::Int32(fixed_value)),
            "INT UNSIGNED" => Ok(DataTypeEnum::Int32(fixed_value)),
            "BIGINT" => Ok(DataTypeEnum::Int64(fixed_value)),
            "BIGINT UNSIGNED" => Ok(DataTypeEnum::UInt64(fixed_value)),
            "FLOAT" => Ok(DataTypeEnum::Float32(fixed_value)),
            "DOUBLE" => Ok(DataTypeEnum::Float64(fixed_value)),
            "DATE" => Ok(DataTypeEnum::Date(fixed_value)),
            "TIME" => Ok(DataTypeEnum::Time(fixed_value)),
            "DATETIME" => Ok(DataTypeEnum::DateTime(fixed_value)),
            "TIMESTAMP" => Ok(DataTypeEnum::Timestamp(fixed_value)),
            "CHAR" => Ok(DataTypeEnum::String(fixed_value)),
            "VARCHAR" => Ok(DataTypeEnum::String(fixed_value)),
            "TINYBLOB" => Ok(DataTypeEnum::String(fixed_value)),
            "TINYTEXT" => Ok(DataTypeEnum::String(fixed_value)),
            "BLOB" => Ok(DataTypeEnum::String(fixed_value)),
            "TEXT" => Ok(DataTypeEnum::String(fixed_value)),
            "MEDIUMBLOB" => Ok(DataTypeEnum::String(fixed_value)),
            "MEDIUMTEXT" => Ok(DataTypeEnum::String(fixed_value)),
            "LONGBLOB" => Ok(DataTypeEnum::String(fixed_value)),
            "PASSWORD" => Ok(DataTypeEnum::Password(fixed_value)),
            "USERNAME" => Ok(DataTypeEnum::Username(fixed_value)),
            "EMAIL" => Ok(DataTypeEnum::Email(fixed_value)),
            "PHONE" => Ok(DataTypeEnum::Phone(fixed_value)),
            "IPV4" => Ok(DataTypeEnum::IPv4(fixed_value)),
            "IPV6" => Ok(DataTypeEnum::IPv6(fixed_value)),
            "UUID" => Ok(DataTypeEnum::UUID(fixed_value)),
            "CITY" => Ok(DataTypeEnum::City(fixed_value)),
            "COUNTRY" => Ok(DataTypeEnum::Country(fixed_value)),
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
    static VALUE_PATH: &'static str = "examples/other.json";

    #[test]
    fn test_parse_value_type() {
        let path_buf = PathBuf::from(VALUE_PATH);
        let data = std::fs::read(path_buf).unwrap();

        let val = serde_json::from_slice(&data).unwrap();
        let columns = OutputColumn::get_columns_from_value(&val);

        assert!(columns.len() > 0);
    }

    #[test]
    fn test_merge_columns() {
        let path_buf = PathBuf::from(SCHEMA_PATH);

        let schema = parse_schema(&path_buf).expect("解析schema文件失败");

        let outputs1 = parse_outputs_from_schema(schema).expect("解析output失败");
        let c1 = outputs1[0].columns().unwrap();
        let c2 = outputs1[1].columns().unwrap();

        let mut _c3 = vec![];
        if outputs1[0].name().eq("mysql-output") {
            _c3 = OutputColumn::merge_columns(c1, c2);
        } else {
            _c3 = OutputColumn::merge_columns(c2, c1);
        }

        let c4m = OutputColumn::map_columns(&_c3);
        assert!((*c4m.get("PACKET_ID").unwrap()).eq(&DataTypeEnum::UInt64(FixedValue::None)));
        assert!((*c4m.get("PACKET_NAME").unwrap()).eq(&DataTypeEnum::String(FixedValue::None)));
        assert!((*c4m.get("ip").unwrap()).eq(&DataTypeEnum::IPv4(FixedValue::None)));
        assert!((*c4m.get("password").unwrap()).eq(&DataTypeEnum::String(FixedValue::None)));
        assert!((*c4m.get("username").unwrap()).eq(&DataTypeEnum::String(FixedValue::None)));
    }
}
