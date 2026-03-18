mod models;
pub use models::Erc20TransferRecord;

use sqlx::{Pool, Postgres, QueryBuilder, postgres::PgPoolOptions};
//use crate::models::Erc20TransferRecord;

pub type PgPool = Pool<Postgres>;

const MAX_POOL_SIZE: u32 = 10;

pub async fn create_db_pool(database_url: &str) -> PgPool {

    PgPoolOptions::new()
        .max_connections(MAX_POOL_SIZE)
        .connect(database_url)
        .await
        .expect("Failed to connect to database.")
}

pub async fn insert_one_erc20_transfer(
    pool: &PgPool,
    transfer: &Erc20TransferRecord
) -> Result<(), sqlx::Error> {

    sqlx::query(
        r#"
        INSERT INTO erc20_transfers (
            block_number,
            block_hash,
            txn_hash,
            log_index,
            contract_address,
            from_address,
            to_address,
            value,
            timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (txn_hash, log_index) DO NOTHING
        "#
    )
    .bind(transfer.block_number)
    .bind(&transfer.block_hash)
    .bind(&transfer.txn_hash)
    .bind(transfer.log_index)
    .bind(&transfer.contract_address)
    .bind(&transfer.from_address)
    .bind(&transfer.to_address)
    .bind(&transfer.value)
    .bind(transfer.timestamp)
    .execute(pool)
    .await?;

    Ok(())
}


pub async fn insert_batch_erc20_transfers(
    pool: &PgPool,
    transfer_records: &[Erc20TransferRecord]
) -> Result<(), sqlx::Error> {

    if transfer_records.is_empty() {
        return Ok(());
    }

    let mut query_builder = QueryBuilder::new(
        "INSERT INTO erc20_transfers (
            block_number,
            block_hash,
            txn_hash,
            log_index,
            contract_address,
            from_address,
            to_address,
            value,
            timestamp
        ) "
    );

    query_builder.push_values(transfer_records, |mut b, record| {
        b.push_bind(record.block_number)
            .push_bind(&record.block_hash)
            .push_bind(&record.txn_hash)
            .push_bind(record.log_index)
            .push_bind(&record.contract_address)
            .push_bind(&record.from_address)
            .push_bind(&record.to_address)
            .push_bind(&record.value)
            .push_bind(record.timestamp);
    });

    query_builder.push(" ON CONFLICT (txn_hash, log_index, block_number) DO NOTHING");

    query_builder.build().execute(pool).await?;

    Ok(())
}


pub async fn get_checkpoint(
    db_pool: &PgPool,
) -> Result<u64, sqlx::Error> {

    let record = sqlx::query!(
        "SELECT last_processed_block
         FROM indexer_checkpoint
         WHERE id = 1"
    )
    .fetch_one(db_pool)
    .await?;

    Ok(record.last_processed_block as u64)
}


pub async fn update_checkpoint(
    db_pool: &PgPool,
    last_processed_block: u64
) -> Result<(), sqlx::Error> {

    sqlx::query!(
        "UPDATE indexer_checkpoint
         SET last_processed_block = $1
         WHERE id = 1",
         last_processed_block as i64
    )
    .execute(db_pool)
    .await?;
    
    Ok(())
}