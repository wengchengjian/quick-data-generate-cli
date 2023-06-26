
use std::ops::Range;


use fake::faker::address::raw::*;
use fake::faker::internet::raw::*;
use fake::faker::job::raw::Title;
use fake::faker::name::raw::Name;
use fake::faker::phone_number::raw::*;
use fake::faker::time::en::DateTimeBetween;
use fake::faker::time::raw::*;
use fake::locales::*;
use fake::uuid::UUIDv4;
use fake::{Fake, Faker};

use rand::{thread_rng, Rng};
use serde_json::json;

static DATE_TIME_FORMAT: &[FormatItem<'_>] = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
    sign:mandatory]:[offset_minute]:[offset_second]"
);

static FORMAT_DATE_TIME: &[FormatItem<'_>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

pub fn get_random_u8() -> u8 {
    Faker.fake::<u8>()
}

pub fn get_random_number(count: Range<usize>) -> usize {
    Faker.fake::<usize>() % count.end
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

pub fn get_random_free_email_between(_: &str, _: &str) -> String {
    FreeEmail(ZH_CN).fake()
}

pub fn get_random_safe_email() -> String {
    SafeEmail(ZH_CN).fake()
}

pub fn get_random_username() -> String {
    Username(EN).fake()
}
pub fn get_random_username_between(_val: &str, _val2: &str) -> String {
    Username(EN).fake()
}

pub fn get_random_password() -> String {
    Password(EN, 8..16).fake()
}

pub fn get_random_password_between(_val: &str, _val2: &str) -> String {
    Password(EN, 8..16).fake()
}

pub fn get_random_ipv4_en() -> String {
    IPv4(EN).fake()
}

pub fn get_random_ipv4_zh() -> String {
    IPv4(ZH_CN).fake()
}

pub fn get_random_ipv4_between(_val: &str, _val2: &str) -> String {
    IPv4(ZH_CN).fake()
}

pub fn get_random_ipv6_zh() -> String {
    IPv6(ZH_CN).fake()
}
pub fn get_random_ipv6_between(_val: &str, _val2: &str) -> String {
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
pub fn get_random_city_between(_: &str, _: &str) -> String {
    CityName(ZH_CN).fake()
}

pub fn get_random_country_zh() -> String {
    CountryName(ZH_CN).fake()
}

pub fn get_random_country_between(_: &str, _: &str) -> String {
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
pub fn get_random_phone_between(_: &str, _: &str) -> String {
    PhoneNumber(ZH_CN).fake()
}

pub fn get_random_date_zh() -> time::Date {
    Date(ZH_CN).fake()
}

pub fn get_random_date_en() -> time::Date {
    Date(EN).fake()
}

pub fn get_random_time_zh() -> time::Time {
    Time(ZH_CN).fake()
}

pub fn get_random_time_en() -> time::Time {
    Time(EN).fake()
}

pub fn get_random_date_between_str(start: &str, end: &str) -> time::OffsetDateTime {
    let start = format!("{} +08:00:00", start);
    let end = format!("{} +08:00:00", end);
    let start = time::OffsetDateTime::parse(&start, &DATE_TIME_FORMAT)
        .expect("时间格式错误，请满足[year]-[month]-[day] [hour]:[minute]:[second]");
    let end = time::OffsetDateTime::parse(&end, &DATE_TIME_FORMAT)
        .expect("时间格式错误，请满足[year]-[month]-[day] [hour]:[minute]:[second]");
    DateTimeBetween(start, end).fake()
}

pub fn get_random_date_between_str_to(start: &str, end: &str) -> String {
    let start = format!("{} +08:00:00", start);
    let end = format!("{} +08:00:00", end);
    let start = time::OffsetDateTime::parse(&start, &DATE_TIME_FORMAT)
        .expect("时间格式错误，请满足[year]-[month]-[day] [hour]:[minute]:[second]");
    let end = time::OffsetDateTime::parse(&end, &DATE_TIME_FORMAT)
        .expect("时间格式错误，请满足[year]-[month]-[day] [hour]:[minute]:[second]");
    let time: OffsetDateTime = DateTimeBetween(start, end).fake();

    time.format(&FORMAT_DATE_TIME).unwrap()
}

pub fn get_random_date_between_zh(
    start: time::OffsetDateTime,
    end: time::OffsetDateTime,
) -> time::OffsetDateTime {
    DateTimeBetween(start, end).fake()
}

pub fn get_random_timestamp() -> time::OffsetDateTime {
    get_random_date_between_zh(
        time::OffsetDateTime::UNIX_EPOCH,
        datetime!(2038-01-19 03:14:07 UTC),
    )
}

pub fn get_random_timestamp_string() -> String {
    get_random_date_between_zh(
        time::OffsetDateTime::UNIX_EPOCH,
        datetime!(2038-01-19 03:14:07 UTC),
    )
    .format(&DATE_TIME_FORMAT)
    .unwrap()
}

pub fn get_random_timestamp_zh_between(
    start: time::OffsetDateTime,
    end: time::OffsetDateTime,
) -> time::OffsetDateTime {
    get_random_date_between_zh(start, end)
}

pub fn get_random_timestamp_zh_between_string(
    start: time::OffsetDateTime,
    end: time::OffsetDateTime,
) -> String {
    get_random_date_between_zh(start, end)
        .format(&DATE_TIME_FORMAT)
        .unwrap()
}

pub fn get_random_datetime_zh_between(
    start: time::OffsetDateTime,
    end: time::OffsetDateTime,
) -> time::OffsetDateTime {
    get_random_date_between_zh(start, end)
}

pub fn get_random_datetime_zh_between_string(
    start: time::OffsetDateTime,
    end: time::OffsetDateTime,
) -> String {
    get_random_date_between_zh(start, end)
        .format(&DATE_TIME_FORMAT)
        .unwrap()
}

pub fn get_random_datetime_zh() -> time::OffsetDateTime {
    get_random_date_between_zh(
        time::OffsetDateTime::UNIX_EPOCH,
        datetime!(2038-01-19 03:14:07 UTC),
    )
}

pub fn get_random_datetime_string() -> String {
    get_random_date_between_zh(
        time::OffsetDateTime::UNIX_EPOCH,
        datetime!(2038-01-19 03:14:07 UTC),
    )
    .format(&DATE_TIME_FORMAT)
    .unwrap()
}

pub fn get_random_datetime_en() -> time::OffsetDateTime {
    get_random_date_between_zh(
        time::OffsetDateTime::UNIX_EPOCH,
        datetime!(2038-01-19 03:14:07 UTC),
    )
}

pub fn get_random_uuid() -> String {
    UUIDv4.fake()
}

pub fn get_random_uuid_between(_: &str, _: &str) -> String {
    UUIDv4.fake()
}

use fake::faker::lorem::raw::*;
use time::format_description::FormatItem;
use time::macros::{datetime, format_description};
use time::OffsetDateTime;

use crate::model::column::{DataTypeEnum, FixedValue, DataSourceColumn};
use crate::{impl_block_parse_type_val_for_number, impl_block_parse_type_val_for_str};

pub fn get_random_word_en() -> String {
    Word(EN).fake()
}

pub fn get_random_word_zh() -> String {
    Word(ZH_CN).fake()
}

pub fn get_random_word_zh_between(_: &str, _: &str) -> String {
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
pub fn get_random_sentence_between(_: &str, _: &str) -> String {
    Sentence(ZH_CN, 1..3).fake()
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
pub fn get_random_paragraph_between(_: &str, _: &str) -> String {
    Paragraph(ZH_CN, 1..3).fake()
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

pub fn get_random_bool() -> bool {
    Faker.fake()
}

pub type Json = serde_json::Value;

pub fn get_random_string_two(_: &str, _: &str) -> String {
    return get_random_string();
}


/// 通用生成测试数据方法
pub fn get_fake_data(columns: &Vec<DataSourceColumn>) -> Json {
    let mut data = json!({});
    let mut rng = thread_rng();
    for column in columns {
        let name = column.name();
        let data_type = column.data_type();
        match data_type {
            DataTypeEnum::UInt8(val) => {
                impl_block_parse_type_val_for_number!(u8, rng, get_random_u8, val, data, name)
            }
            DataTypeEnum::UInt16(val) => {
                impl_block_parse_type_val_for_number!(u16, rng, get_random_u16, val, data, name)
            }
            DataTypeEnum::UInt32(val) => {
                impl_block_parse_type_val_for_number!(u32, rng, get_random_u32, val, data, name)
            }
            DataTypeEnum::UInt64(val) => {
                impl_block_parse_type_val_for_number!(u64, rng, get_random_u64, val, data, name)
            }
            DataTypeEnum::Int8(val) => {
                impl_block_parse_type_val_for_number!(i8, rng, get_random_i8, val, data, name)
            }
            DataTypeEnum::Int16(val) => {
                impl_block_parse_type_val_for_number!(i16, rng, get_random_i16, val, data, name)
            }
            DataTypeEnum::Int32(val) => {
                impl_block_parse_type_val_for_number!(i32, rng, get_random_i32, val, data, name)
            }
            DataTypeEnum::Int64(val) => {
                impl_block_parse_type_val_for_number!(i64, rng, get_random_i64, val, data, name)
            }
            DataTypeEnum::Float32(val) => {
                impl_block_parse_type_val_for_number!(f32, rng, get_random_f32, val, data, name)
            }
            DataTypeEnum::Float64(val) => {
                impl_block_parse_type_val_for_number!(f64, rng, get_random_f64, val, data, name);
            }
            DataTypeEnum::String(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_string,
                    val,
                    data,
                    name,
                    get_random_string_two
                )
            }
            DataTypeEnum::FixedString(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_string,
                    val,
                    data,
                    name,
                    get_random_string_two
                )
            }
            DataTypeEnum::Date(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_date_zh,
                    val,
                    data,
                    name,
                    get_random_string_two
                )
            }
            DataTypeEnum::Time(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_time_zh,
                    val,
                    data,
                    name,
                    get_random_string_two
                )
            }
            DataTypeEnum::Timestamp(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_timestamp_string,
                    val,
                    data,
                    name,
                    get_random_date_between_str_to
                )
            }
            DataTypeEnum::DateTime(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_datetime_string,
                    val,
                    data,
                    name,
                    get_random_date_between_str_to
                )
            }
            DataTypeEnum::DateTime64(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_datetime_string,
                    val,
                    data,
                    name,
                    get_random_date_between_str_to
                )
            }
            DataTypeEnum::Nullable(_) => {
                data[name] = json!(null);
            }
            DataTypeEnum::UUID(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_uuid,
                    val,
                    data,
                    name,
                    get_random_uuid_between
                )
            }
            DataTypeEnum::IPv4(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_ipv4_zh,
                    val,
                    data,
                    name,
                    get_random_ipv4_between
                );
            }
            DataTypeEnum::IPv6(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_ipv6_zh,
                    val,
                    data,
                    name,
                    get_random_ipv6_between
                );
            }
            DataTypeEnum::Email(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_free_email,
                    val,
                    data,
                    name,
                    get_random_free_email_between
                );
            }
            DataTypeEnum::Password(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_password,
                    val,
                    data,
                    name,
                    get_random_password_between
                );
            }
            DataTypeEnum::Username(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_username,
                    val,
                    data,
                    name,
                    get_random_username_between
                );
            }
            DataTypeEnum::Word(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_word_zh,
                    val,
                    data,
                    name,
                    get_random_word_zh_between
                );
            }
            DataTypeEnum::Sentence(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_sentence_zh(1..3),
                    val,
                    data,
                    name,
                    get_random_sentence_between
                );
            }
            DataTypeEnum::Paragraph(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_paragraph_zh(1..3),
                    val,
                    data,
                    name,
                    get_random_paragraph_between
                );
            }
            DataTypeEnum::City(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_city_zh,
                    val,
                    data,
                    name,
                    get_random_city_between
                );
            }
            DataTypeEnum::Country(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_country_zh,
                    val,
                    data,
                    name,
                    get_random_country_between
                );
            }
            DataTypeEnum::Phone(val) => {
                impl_block_parse_type_val_for_str!(
                    rng,
                    get_random_phone_zh,
                    val,
                    data,
                    name,
                    get_random_phone_between
                );
            }
            DataTypeEnum::Unknown => {
                data[name] = json!(null);
            }
            DataTypeEnum::Boolean(val) => match val {
                FixedValue::Single(val) => {
                    data[name] = json!(val.parse::<bool>().unwrap_or(get_random_bool()))
                }
                FixedValue::Array(_) => data[name] = json!(get_random_bool()),
                FixedValue::Range(_, _) => data[name] = json!(get_random_bool()),
                FixedValue::None => data[name] = json!(get_random_bool()),
            },
        }
    }
    return data;
}
