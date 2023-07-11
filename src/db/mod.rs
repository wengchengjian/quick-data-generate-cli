use std::sync::Arc;

use sqlx::{
    pool::PoolConnection, sqlite::SqliteConnectOptions, Connection, Executor, Pool, Sqlite,
    SqliteConnection, SqlitePool,
};

use crate::datasource::DATA_SOURCE_MANAGER;

pub mod insert;
pub mod delete;
pub mod query;

pub const DB_NAME: &'static str = "data.db";
pub const SCHEMA_TABLE_NAME: &'static str = "data_source_schema";

pub async fn init_pool() -> crate::Result<()> {
    let data_manager = DATA_SOURCE_MANAGER.read().await;
    let is_init = data_manager.pool.is_some();
    drop(data_manager);
    if !is_init {
        let _ = DATA_SOURCE_MANAGER
            .write()
            .await
            .pool
            .insert(create_pool().await?);
    }

    Ok(())
}

pub async fn get_conn() -> crate::Result<PoolConnection<Sqlite>> {
    init_pool().await?;
    let conn = DATA_SOURCE_MANAGER
        .read()
        .await
        .pool()
        .unwrap()
        .acquire()
        .await?;
    Ok(conn)
}

pub async fn create_pool() -> crate::Result<Pool<Sqlite>> {
    let opt = SqliteConnectOptions::new()
        .filename(DB_NAME)
        .create_if_missing(true);
    let pool = SqlitePool::connect_with(opt).await?;
    let mut conn = pool.acquire().await?;
    init(&mut conn).await?;
    Ok(pool)
}

pub async fn init(conn: &mut SqliteConnection) -> crate::Result<()> {
    conn.execute(
        "
        CREATE TABLE IF NOT EXISTS data_source_schema (
        id integer primary key,
        name varchar(255) not null,
        source varchar(50) not null,
        meta text ,
        columns text,
        channel text,
        sources text
        )
    ",
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio_test::block_on;

    use crate::model::schema::DataSourceSchema;

    use super::{insert::insert_schema, query::find_all_schemas, *};

    #[test]
    fn test_create_db() {
        block_on(async {
            let pool = create_pool().await.unwrap();
        });
    }
    #[test]
    fn test_query_schema() {
        block_on(async {
            let schema = DataSourceSchema::fake();
            insert_schema(&schema).await.unwrap();

            let data = find_all_schemas().await.unwrap();
            println!("{}", data.len());
        });
    }
}
