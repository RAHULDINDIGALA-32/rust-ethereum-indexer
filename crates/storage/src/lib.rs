mod models;
pub use models::Erc20TransferRecord;

use sqlx::{Pool, Postgres, QueryBuilder, postgres::PgPoolOptions};
//use crate::models::Erc20TransferRecord;

pub type PgPool = Pool<Postgres>;

pub async fn create_db_pool(database_url: &str) -> PgPool {

    PgPoolOptions::new()
        .max_connections(10)
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