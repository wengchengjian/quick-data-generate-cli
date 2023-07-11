use serde_json::json;

use crate::{
    core::fake::FORMAT_DATE_TIME,
    datasource::DATA_SOURCE_MANAGER,
    model::schema::{ChannelSchema, DataSourceSchema},
    Json,
};

use super::{get_conn, init_pool};

pub async fn insert_schema_batch(schemas: &Vec<DataSourceSchema>) -> crate::Result<()> {
    for schema in schemas {
        insert_schema(schema).await?;
    }
    Ok(())
}

pub async fn insert_schema(schema: &DataSourceSchema) -> crate::Result<()> {
    init_pool().await?;

    let data_manager = DATA_SOURCE_MANAGER.read().await;
    let pool = data_manager.pool().unwrap();
    sqlx::query("INSERT INTO data_source_schema(name, source, meta, columns, channel, sources, create_time, update_time, deleted) VALUES(?, ?, ?, ?, ?, ?, ?)")
    .bind(&schema.name)
    .bind(&schema.source.to_string())
    .bind(serde_json::to_string(schema.meta().unwrap_or(&json!({})))?)
    .bind(serde_json::to_string(schema.columns().unwrap_or(&json!({})))?)
    .bind(serde_json::to_string(schema.channel().unwrap_or(&ChannelSchema::default()))?)
    .bind(serde_json::to_string(schema.sources().unwrap_or(&vec![]))?)
    .bind(schema.create_time.format(&FORMAT_DATE_TIME).unwrap())
    .bind(schema.update_time.format(&FORMAT_DATE_TIME).unwrap())
    .bind(0)
    .execute(pool).await?;
    drop(data_manager);
    Ok(())
}
