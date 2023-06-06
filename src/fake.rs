use std::ops::Range;

use crate::column::{DataTypeEnum, OutputColumn};

use fake::faker::address::raw::*;
use fake::faker::internet::raw::*;
use fake::faker::job::raw::Title;
use fake::faker::name::raw::Name;
use fake::faker::phone_number::raw::*;
use fake::faker::time::raw::*;
use fake::locales::*;
use fake::uuid::UUIDv4;
use fake::{Fake, Faker};
use serde_json::{json, Value};

pub fn get_random_u8() -> u8 {
    Faker.fake::<u8>()
}

pub fn get_random_u16() -> u16 {
    Faker.fake::<u16>()
}

pub fn get_random_u32() -> u32 {
    Faker.fake::<u32>()
}

pub fn get_random_u64() -> u64 {
    Faker.fake::<u64>()
}

pub fn get_random_i8() -> i8 {
    Faker.fake::<i8>()
}

pub fn get_random_i16() -> i16 {
    Faker.fake::<i16>()
}

pub fn get_random_i32() -> i32 {
    Faker.fake::<i32>()
}

pub fn get_random_i64() -> i64 {
    Faker.fake::<i64>()
}

pub fn get_random_f32() -> f32 {
    Faker.fake::<f32>()
}

pub fn get_random_f64() -> f64 {
    Faker.fake::<f64>()
}

pub fn get_random_string() -> String {
    Faker.fake::<String>()
}

pub fn get_random_name_en() -> String {
    Name(EN).fake()
}

pub fn get_random_name_zh() -> String {
    Name(ZH_CN).fake()
}

pub fn get_random_title_en() -> String {
    Title(EN).fake()
}

pub fn get_random_title_zh() -> String {
    Title(ZH_CN).fake()
}

pub fn get_random_free_email() -> String {
    FreeEmail(ZH_CN).fake()
}

pub fn get_random_safe_email() -> String {
    SafeEmail(ZH_CN).fake()
}

pub fn get_random_username() -> String {
    Username(EN).fake()
}

pub fn get_random_password() -> String {
    Password(EN, 8..16).fake()
}

pub fn get_random_ipv4_en() -> String {
    IPv4(EN).fake()
}

pub fn get_random_ipv4_zh() -> String {
    IPv4(ZH_CN).fake()
}

pub fn get_random_ipv6_zh() -> String {
    IPv6(ZH_CN).fake()
}

pub fn get_random_ipv6_en() -> String {
    IPv6(EN).fake()
}

pub fn get_random_city_en() -> String {
    CityName(EN).fake()
}

pub fn get_random_city_zh() -> String {
    CityName(ZH_CN).fake()
}

pub fn get_random_country_zh() -> String {
    CountryName(ZH_CN).fake()
}

pub fn get_random_country_en() -> String {
    CountryName(EN).fake()
}

pub fn get_random_phone_en() -> String {
    PhoneNumber(EN).fake()
}

pub fn get_random_phone_zh() -> String {
    PhoneNumber(ZH_CN).fake()
}

pub fn get_random_date_zh() -> String {
    Date(ZH_CN).fake()
}

pub fn get_random_date_en() -> String {
    Date(EN).fake()
}

pub fn get_random_time_zh() -> String {
    Time(ZH_CN).fake()
}

pub fn get_random_time_en() -> String {
    Time(EN).fake()
}

pub fn get_random_datetime_zh() -> String {
    DateTime(ZH_CN).fake()
}

pub fn get_random_datetime_en() -> String {
    DateTime(EN).fake()
}

pub fn get_random_uuid() -> String {
    UUIDv4.fake()
}

use fake::faker::lorem::raw::*;

pub fn get_random_word_en() -> String {
    Word(EN).fake()
}

pub fn get_random_word_zh() -> String {
    Word(ZH_CN).fake()
}

pub fn get_random_words_en(count: Range<usize>) -> Vec<String> {
    Words(EN, count).fake()
}

pub fn get_random_words_zh(count: Range<usize>) -> Vec<String> {
    Words(ZH_CN, count).fake()
}

pub fn get_random_sentence_zh(count: Range<usize>) -> String {
    Sentence(ZH_CN, count).fake()
}

pub fn get_random_sentence_en(count: Range<usize>) -> String {
    Sentence(EN, count).fake()
}

pub fn get_random_sentences_en(count: Range<usize>) -> Vec<String> {
    Sentences(EN, count).fake()
}

pub fn get_random_sentences_zh(count: Range<usize>) -> Vec<String> {
    Sentences(ZH_CN, count).fake()
}

pub fn get_random_paragraph_zh(count: Range<usize>) -> String {
    Paragraph(ZH_CN, count).fake()
}

pub fn get_random_paragraph_en(count: Range<usize>) -> String {
    Paragraph(EN, count).fake()
}

pub fn get_random_paragraphs_en(count: Range<usize>) -> Vec<String> {
    Paragraphs(EN, count).fake()
}

pub fn get_random_paragraphs_zh(count: Range<usize>) -> Vec<String> {
    Paragraphs(ZH_CN, count).fake()
}

pub fn get_fake_data(columns: &Vec<OutputColumn>) -> Value {
    let mut data = json!({});
    for colum in columns {
        let name = colum.name();
        let data_type = colum.data_type();
        match data_type {
            DataTypeEnum::UInt8 => {
                data[name] = json!(get_random_u8());
            }
            DataTypeEnum::UInt16 => {
                data[name] = json!(get_random_u16());
            }
            DataTypeEnum::UInt32 => {
                data[name] = json!(get_random_u32());
            }
            DataTypeEnum::UInt64 => {
                data[name] = json!(get_random_u64());
            }
            DataTypeEnum::Int8 => {
                data[name] = json!(get_random_i8());
            }
            DataTypeEnum::Int16 => {
                data[name] = json!(get_random_i16());
            }
            DataTypeEnum::Int32 => {
                data[name] = json!(get_random_i32());
            }
            DataTypeEnum::Int64 => {
                data[name] = json!(get_random_i64());
            }
            DataTypeEnum::Float32 => {
                data[name] = json!(get_random_f32());
            }
            DataTypeEnum::Float64 => {
                data[name] = json!(get_random_f64());
            }
            DataTypeEnum::String => {
                data[name] = json!(get_random_string());
            }
            DataTypeEnum::FixedString => {
                data[name] = json!(get_random_string());
            }
            DataTypeEnum::Date => {
                data[name] = json!(get_random_date_zh());
            }
            DataTypeEnum::Time => {
                data[name] = json!(get_random_time_zh());
            }
            DataTypeEnum::Timestamp => {
                data[name] = json!(get_random_datetime_zh());
            }
            DataTypeEnum::DateTime => {
                data[name] = json!(get_random_datetime_zh());
            }
            DataTypeEnum::DateTime64 => {
                data[name] = json!(get_random_datetime_zh());
            }
            DataTypeEnum::Nullable => {
                data[name] = json!(null);
            }
            DataTypeEnum::UUID => {
                data[name] = json!(get_random_uuid());
            }
            DataTypeEnum::IPv4 => {
                data[name] = json!(get_random_ipv4_zh());
            }
            DataTypeEnum::IPv6 => {
                data[name] = json!(get_random_ipv4_zh());
            }
            DataTypeEnum::Email => {
                data[name] = json!(get_random_free_email());
            }
            DataTypeEnum::Password => {
                data[name] = json!(get_random_password());
            }
            DataTypeEnum::Username => {
                data[name] = json!(get_random_username());
            }
            DataTypeEnum::Word => {
                data[name] = json!(get_random_word_zh());
            }
            DataTypeEnum::Sentence => {
                data[name] = json!(get_random_sentence_zh(1..3));
            }
            DataTypeEnum::Paragraph => {
                data[name] = json!(get_random_paragraph_zh(1..3));
            }
            DataTypeEnum::City => {
                data[name] = json!(get_random_city_zh());
            }
            DataTypeEnum::Country => {
                data[name] = json!(get_random_country_zh());
            }
            DataTypeEnum::Phone => {
                data[name] = json!(get_random_phone_zh());
            }
            DataTypeEnum::Unknown => {
                data[name] = json!(null);
            }
        }
    }
    return data;
}
