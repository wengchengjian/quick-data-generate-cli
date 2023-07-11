use std::str::FromStr;

use sqlx::{sqlite::SqliteRow, Row};

use crate::{
    datasource::{DataSourceEnum, DATA_SOURCE_MANAGER},
    model::schema::DataSourceSchema,
    Json, core::fake::FORMAT_DATE_TIME,
};
/// 定义一些sql查询函数

pub fn trans_schema_from_row(row: SqliteRow) -> DataSourceSchema {
    let id: u32 = row.try_get("id").unwrap();
    let name: &str = row.try_get("name").unwrap();
    let source: &str = row.try_get("source").unwrap();
    let meta: &str = row.try_get("meta").unwrap();
    let columns: &str = row.try_get("columns").unwrap();
    let channel: &str = row.try_get("channel").unwrap();
    let sources: &str = row.try_get("sources").unwrap();
    let create_time: &str = row.try_get("create_time").unwrap();
    let create_time = format!("{} +08:00:00", create_time);
    let update_time: &str = row.try_get("update_time").unwrap();
    let update_time = format!("{} +08:00:00", update_time);
    DataSourceSchema::new(
        id,
        name.to_owned(),
        DataSourceEnum::from_str(source).unwrap(),
        Some(Json::from_str(meta).unwrap()),
        Some(Json::from_str(columns).unwrap()),
        Some(serde_json::from_str(channel).unwrap()),
        Some(serde_json::from_str(sources).unwrap()),
        time::OffsetDateTime::parse(&create_time, &FORMAT_DATE_TIME).unwrap(),
        time::OffsetDateTime::parse(&update_time, &FORMAT_DATE_TIME).unwrap()
    )
}

/// 找到最新的schema, 可以用于命令行调用的省略参数
pub async fn find_last_schema() -> crate::Result<Option<DataSourceSchema>> {
    let data_manager = DATA_SOURCE_MANAGER.read().await;
    let pool = data_manager.pool().unwrap();
    let rows: Vec<DataSourceSchema> = sqlx::query("SELECT * FROM data_source_schema order by update_time desc limit 1")
        .map(trans_schema_from_row)
        .fetch_all(pool)
        .await?;
    drop(data_manager);

    if rows.len() == 0 {
        Ok(None)
    } else {
        Ok(Some(rows[0].clone()))
    }
}

pub async fn find_all_schemas() -> crate::Result<Vec<DataSourceSchema>> {
    let data_manager = DATA_SOURCE_MANAGER.read().await;
    let pool = data_manager.pool().unwrap();
    let rows: Vec<DataSourceSchema> = sqlx::query("SELECT * FROM data_source_schema where deleted = 0 order by update_time desc, create_time desc")
        .map(trans_schema_from_row)
        .fetch_all(pool)
        .await?;
    drop(data_manager);

    Ok(rows)
}

#[cfg(test)]
mod tests {

    use super::*;
}
