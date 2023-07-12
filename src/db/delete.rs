use crate::datasource::DATA_SOURCE_MANAGER;

use super::init_pool;

pub async fn delete_schema_by_id(id: u32) -> crate::Result<()> {
    init_pool().await?;

    let data_manager = DATA_SOURCE_MANAGER.read().await;
    let pool = data_manager.pool().unwrap();

    sqlx::query("delete FROM data_source_schema where id = ?")
        .bind(id)
        .execute(pool)
        .await?;
    drop(data_manager);

    Ok(())
}

pub async fn delete_schema_by_name(name: &str) -> crate::Result<()> {
    init_pool().await?;

    let data_manager = DATA_SOURCE_MANAGER.read().await;
    let pool = data_manager.pool().unwrap();

    sqlx::query("delete FROM data_source_schema where name = ?")
        .bind(name)
        .execute(pool)
        .await?;
    drop(data_manager);

    Ok(())
}

pub async fn clear_schemas() -> crate::Result<()> {
    init_pool().await?;

    let data_manager = DATA_SOURCE_MANAGER.read().await;
    let pool = data_manager.pool().unwrap();

    sqlx::query("truncate table data_source_schema")
        .execute(pool)
        .await?;
    drop(data_manager);

    Ok(())
}
