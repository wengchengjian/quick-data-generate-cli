use std::{collections::HashMap, fmt::Display, str::FromStr};

use crate::core::{
    error::{Error, IoError, Result},
    parse::parse_type,
};
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataSourceColumn {
    pub name: String,
    pub data_type: DataTypeEnum,
}

impl DataSourceColumn {
    pub fn new(name: &str, data_type: &str) -> Self {
        let name = name.to_string();
        let data_type = DataTypeEnum::from_str(data_type).unwrap();
        Self { name, data_type }
    }

    /// 返回column的map 映射
    pub fn map_columns(columns: &Vec<DataSourceColumn>) -> HashMap<&str, &DataTypeEnum> {
        columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect()
    }
    /// 从schema中获取columns定义，允许类型未知
    pub fn get_columns_from_schema(value: &serde_json::Value) -> Vec<DataSourceColumn> {
        if let Some(map) = value.as_object() {
            return map
                .into_iter()
                .map(|(key, value)| DataSourceColumn {
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
    pub fn get_columns_from_value(value: &serde_json::Value) -> Vec<DataSourceColumn> {
        if let Some(map) = value.as_object() {
            return map
                .into_iter()
                .map(|(key, value)| DataSourceColumn {
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
        source: &Vec<DataSourceColumn>,
        target: &Vec<DataSourceColumn>,
    ) -> Vec<DataSourceColumn> {
        let mut c1 = DataSourceColumn::map_columns(source);
        let c2 = DataSourceColumn::map_columns(target);

        c1.extend(c2);

        c1.into_iter()
            .map(|(key, val)| DataSourceColumn {
                name: key.to_string(),
                data_type: val.clone(),
            })
            .collect()
    }
}

impl DataSourceColumn {
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

macro_rules! write_schema_string {
    ($tp: expr, $val: ident, $f: ident) => {
        match $val {
            FixedValue::Single(val) => write!($f, "{}([{}])", $tp, val),
            FixedValue::Array(val) => write!($f, "{}([{}])", $tp, val.join(",")),
            FixedValue::Range(val1, val2) => write!($f, "{}({}..{})", $tp, val1, val2),
            FixedValue::None => write!($f, "{}", $tp),
        }
    };
}

impl Display for DataTypeEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataTypeEnum::UInt8(val) => write_schema_string!("UInt8", val, f),
            DataTypeEnum::UInt16(val) => write_schema_string!("UInt16", val, f),
            DataTypeEnum::UInt32(val) => write_schema_string!("UInt32", val, f),
            DataTypeEnum::UInt64(val) => write_schema_string!("UInt64", val, f),
            DataTypeEnum::Int8(val) => write_schema_string!("Int8", val, f),
            DataTypeEnum::Int16(val) => write_schema_string!("Int16", val, f),
            DataTypeEnum::Email(val) => write_schema_string!("Email", val, f),
            DataTypeEnum::Password(val) => write_schema_string!("Password", val, f),
            DataTypeEnum::Username(val) => write_schema_string!("Username", val, f),
            DataTypeEnum::Word(val) => write_schema_string!("Word", val, f),
            DataTypeEnum::Sentence(val) => write_schema_string!("Sentence", val, f),
            DataTypeEnum::Paragraph(val) => write_schema_string!("Paragraph", val, f),
            DataTypeEnum::City(val) => write_schema_string!("City", val, f),
            DataTypeEnum::Country(val) => write_schema_string!("Country", val, f),
            DataTypeEnum::Phone(val) => write_schema_string!("Phone", val, f),
            DataTypeEnum::Int32(val) => write_schema_string!("Int32", val, f),
            DataTypeEnum::Int64(val) => write_schema_string!("Int64", val, f),
            DataTypeEnum::Float32(val) => write_schema_string!("Float32", val, f),
            DataTypeEnum::Float64(val) => write_schema_string!("Float64", val, f),
            DataTypeEnum::String(val) => write_schema_string!("String", val, f),
            DataTypeEnum::FixedString(val) => write_schema_string!("FixedString", val, f),
            DataTypeEnum::Date(val) => write_schema_string!("Date", val, f),
            DataTypeEnum::Time(val) => write_schema_string!("Time", val, f),
            DataTypeEnum::Timestamp(val) => write_schema_string!("Timestamp", val, f),
            DataTypeEnum::DateTime(val) => write_schema_string!("DateTime", val, f),
            DataTypeEnum::DateTime64(val) => write_schema_string!("DateTime64", val, f),
            DataTypeEnum::Nullable(val) => write_schema_string!("Nullable", val, f),
            DataTypeEnum::UUID(val) => write_schema_string!("UUID", val, f),
            DataTypeEnum::IPv4(val) => write_schema_string!("IPv4", val, f),
            DataTypeEnum::IPv6(val) => write_schema_string!("IPv6", val, f),
            DataTypeEnum::Boolean(val) => write_schema_string!("Boolean", val, f),
            DataTypeEnum::Unknown => write!(f, "Unknown"),
        }
    }
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
        Regex::new(r"(?P<tp>\w+)(\((?P<range>.+)\))?\s*(?P<ex>\w*)?").unwrap();
}

pub fn parse_fixed_value(val: &str) -> FixedValue {
    if val.len() == 0 {
        return FixedValue::None;
    }
    if val.starts_with('[') && val.starts_with(']') {
        let val = val.strip_prefix('[').unwrap().strip_suffix(']').unwrap();
        let vals: Vec<String> = val.split(',').map(|s| s.to_owned()).collect();
        return FixedValue::Array(vals);
    }

    if val.contains("..") {
        let vals: Vec<String> = val.split("..").map(|s| s.to_owned()).collect();
        if vals.len() != 2 {
            return FixedValue::Single(val.to_owned());
        }
        return FixedValue::Range(vals[0].to_owned(), vals[1].to_owned());
    }

    return FixedValue::None;
}

impl FromStr for DataTypeEnum {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s: String = s.to_uppercase();
        let match_str = s.as_str();
        let caps = DATA_TYPE_REGEX.captures(match_str).unwrap();
        let mut tp_str = String::from(&caps["tp"]);
        let fixed_value: FixedValue = match caps.name("range") {
            Some(range) => parse_fixed_value(range.as_str()),
            None => FixedValue::None,
        };
        match caps.name("ex") {
            Some(ex) => {
                if ex.as_str().len() > 0 {
                    tp_str = format!("{} {}", &caps["tp"], &caps["ex"]);
                }
            }
            None => {}
        }
        match tp_str.as_str() {
            "TINYINT UNSIGNED" => Ok(DataTypeEnum::UInt8(fixed_value)),
            "TINYINT" => Ok(DataTypeEnum::Int8(fixed_value)),
            "SMALLINT" => Ok(DataTypeEnum::Int16(fixed_value)),
            "SMALLINT UNSIGNED" => Ok(DataTypeEnum::UInt16(fixed_value)),
            "MEDIUMINT" => Ok(DataTypeEnum::Int32(fixed_value)),
            "MEDIUMINT UNSIGNED" => Ok(DataTypeEnum::UInt32(fixed_value)),
            "INT" => Ok(DataTypeEnum::Int32(fixed_value)),
            "INT64" => Ok(DataTypeEnum::Int64(fixed_value)),
            "INT32" => Ok(DataTypeEnum::Int32(fixed_value)),
            "INT8" => Ok(DataTypeEnum::Int8(fixed_value)),
            "INT16" => Ok(DataTypeEnum::Int16(fixed_value)),
            "UINT16" => Ok(DataTypeEnum::UInt16(fixed_value)),
            "UINT8" => Ok(DataTypeEnum::UInt8(fixed_value)),
            "UINT32" => Ok(DataTypeEnum::UInt32(fixed_value)),
            "UINT64" => Ok(DataTypeEnum::UInt64(fixed_value)),
            "INT UNSIGNED" => Ok(DataTypeEnum::Int32(fixed_value)),
            "BIGINT" => Ok(DataTypeEnum::Int64(fixed_value)),
            "BIGINT UNSIGNED" => Ok(DataTypeEnum::UInt64(fixed_value)),
            "FLOAT" => Ok(DataTypeEnum::Float32(fixed_value)),
            "DOUBLE" => Ok(DataTypeEnum::Float64(fixed_value)),
            "DATE" => Ok(DataTypeEnum::Date(fixed_value)),
            "TIME" => Ok(DataTypeEnum::Time(fixed_value)),
            "DATETIME" => Ok(DataTypeEnum::DateTime(fixed_value)),
            "TIMESTAMP" => Ok(DataTypeEnum::Timestamp(fixed_value)),
            "STRING" => Ok(DataTypeEnum::String(fixed_value)),

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

    use crate::core::parse::{parse_schema, parse_datasources_from_schema};

    use super::*;

    static SCHEMA_PATH: &'static str = "examples/schema2.json";
    static _SCHEMA_PATH2: &'static str = "examples/schema3.json";
    static VALUE_PATH: &'static str = "examples/other.json";

    #[test]
    fn test_parse_value_type() {
        let path_buf = PathBuf::from(VALUE_PATH);
        let data = std::fs::read(path_buf).unwrap();

        let val = serde_json::from_slice(&data).unwrap();
        let columns = DataSourceColumn::get_columns_from_value(&val);

        assert!(columns.len() > 0);
    }

    #[test]
    fn test_merge_columns() {
        let path_buf = PathBuf::from(SCHEMA_PATH);

        let schema = parse_schema(&path_buf).expect("解析schema文件失败");

        let outputs1 = parse_datasources_from_schema(schema).expect("解析output失败");
        let c1 = outputs1[0].columns().unwrap();
        let c2 = outputs1[1].columns().unwrap();

        let mut _c3 = vec![];
        if outputs1[0].name().eq("mysql-output") {
            _c3 = DataSourceColumn::merge_columns(c1, c2);
        } else {
            _c3 = DataSourceColumn::merge_columns(c2, c1);
        }

        let c4m = DataSourceColumn::map_columns(&_c3);
        assert!((*c4m.get("PACKET_ID").unwrap()).eq(&DataTypeEnum::UInt64(FixedValue::None)));
        assert!((*c4m.get("PACKET_NAME").unwrap()).eq(&DataTypeEnum::String(FixedValue::None)));
        assert!((*c4m.get("ip").unwrap()).eq(&DataTypeEnum::IPv4(FixedValue::None)));
        assert!((*c4m.get("password").unwrap()).eq(&DataTypeEnum::String(FixedValue::None)));
        assert!((*c4m.get("username").unwrap()).eq(&DataTypeEnum::String(FixedValue::None)));
    }
}
