use crate::datasource::DATA_SOURCE_MANAGER;
use super::init_pool;

pub async fn delete_schema_by_id_logic(id: u32) -> crate::Result<()> {
    init_pool().await?;

    let data_manager = DATA_SOURCE_MANAGER.read().await;
    let pool = data_manager.pool().unwrap();

    sqlx::query("update data_source_schema set deleted = 1 where id = ?")
        .bind(id)
        .execute(pool)
        .await?;
    drop(data_manager);
    Ok(())
}

pub async fn delete_schema_by_name_logic(name: &str) -> crate::Result<()> {
    init_pool().await?;

    let data_manager = DATA_SOURCE_MANAGER.read().await;
    let pool = data_manager.pool().unwrap();

    sqlx::query("update data_source_schema set deleted = 1 where name = ?")
        .bind(name)
        .execute(pool)
        .await?;
    drop(data_manager);
    Ok(())
}
