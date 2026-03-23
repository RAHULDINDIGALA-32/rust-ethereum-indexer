mod models;
pub use models::Erc20TransferRecord;

use sqlx::{Pool, Postgres, QueryBuilder, Row, postgres::PgPoolOptions};

pub type PgPool = Pool<Postgres>;

const MAX_POOL_SIZE: u32 = 10;
const BLOCK_PARTITION_SIZE: i64 = 100_000;

pub async fn create_db_pool(database_url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(MAX_POOL_SIZE)
        .connect(database_url)
        .await
        .expect("Failed to connect to database.")
}

pub async fn insert_one_erc20_transfer(
    pool: &PgPool,
    transfer: &Erc20TransferRecord,
) -> Result<(), sqlx::Error> {
    ensure_partitions_for_range(pool, transfer.block_number, transfer.block_number).await?;

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
        ON CONFLICT (txn_hash, log_index, block_number) DO NOTHING
        "#,
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
    transfer_records: &[Erc20TransferRecord],
) -> Result<(), sqlx::Error> {
    if transfer_records.is_empty() {
        return Ok(());
    }

    let min_block = transfer_records
        .iter()
        .map(|record| record.block_number)
        .min()
        .unwrap();

    let max_block = transfer_records
        .iter()
        .map(|record| record.block_number)
        .max()
        .unwrap();

    ensure_partitions_for_range(pool, min_block, max_block).await?;

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
        ) ",
    );

    query_builder.push_values(transfer_records, |mut builder, record| {
        builder
            .push_bind(record.block_number)
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

pub async fn get_checkpoint(db_pool: &PgPool) -> Result<u64, sqlx::Error> {
    let record = sqlx::query_scalar::<_, i64>(
        "SELECT last_processed_block
         FROM indexer_checkpoint
         WHERE id = 1",
    )
    .fetch_one(db_pool)
    .await?;

    Ok(record as u64)
}

pub async fn update_checkpoint(
    db_pool: &PgPool,
    last_processed_block: u64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE indexer_checkpoint
         SET last_processed_block = $1
         WHERE id = 1",
    )
    .bind(last_processed_block as i64)
    .execute(db_pool)
    .await?;

    Ok(())
}

pub async fn insert_block(
    db_pool: &PgPool,
    block_number: u64,
    block_hash: Vec<u8>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO indexed_blocks (block_number, block_hash)
         VALUES ($1, $2)
         ON CONFLICT (block_number) DO UPDATE
         SET block_hash = EXCLUDED.block_hash",
    )
    .bind(block_number as i64)
    .bind(block_hash)
    .execute(db_pool)
    .await?;

    Ok(())
}

pub async fn insert_blocks(
    db_pool: &PgPool,
    indexed_blocks: &[(u64, Vec<u8>)],
) -> Result<(), sqlx::Error> {
    if indexed_blocks.is_empty() {
        return Ok(());
    }

    let mut query_builder =
        QueryBuilder::new("INSERT INTO indexed_blocks (block_number, block_hash) ");

    query_builder.push_values(indexed_blocks, |mut builder, (block_number, block_hash)| {
        builder
            .push_bind(*block_number as i64)
            .push_bind(block_hash);
    });

    query_builder.push(
        " ON CONFLICT (block_number) DO UPDATE
          SET block_hash = EXCLUDED.block_hash",
    );

    query_builder.build().execute(db_pool).await?;

    Ok(())
}

pub async fn get_block_hash(
    db_pool: &PgPool,
    block_number: u64,
) -> Result<Option<Vec<u8>>, sqlx::Error> {
    let record = sqlx::query(
        "SELECT block_hash
         FROM indexed_blocks
         WHERE block_number = $1",
    )
    .bind(block_number as i64)
    .fetch_optional(db_pool)
    .await?;

    Ok(record.map(|row| row.get::<Vec<u8>, _>("block_hash")))
}

pub async fn rollback_from_block(db_pool: &PgPool, block_number: u64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "DELETE FROM erc20_transfers
         WHERE block_number >= $1",
    )
    .bind(block_number as i64)
    .execute(db_pool)
    .await?;

    sqlx::query(
        "DELETE FROM indexed_blocks
         WHERE block_number >= $1",
    )
    .bind(block_number as i64)
    .execute(db_pool)
    .await?;

    Ok(())
}

async fn ensure_partitions_for_range(
    pool: &PgPool,
    start_block: i64,
    end_block: i64,
) -> Result<(), sqlx::Error> {
    let first_partition_start = (start_block / BLOCK_PARTITION_SIZE) * BLOCK_PARTITION_SIZE;
    let last_partition_start = (end_block / BLOCK_PARTITION_SIZE) * BLOCK_PARTITION_SIZE;

    let mut partition_start = first_partition_start;
    while partition_start <= last_partition_start {
        let partition_end = partition_start + BLOCK_PARTITION_SIZE;
        let partition_name = format!("erc20_transfers_p{}_{}", partition_start, partition_end);
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {partition_name}
             PARTITION OF erc20_transfers
             FOR VALUES FROM ({partition_start}) TO ({partition_end})"
        );

        sqlx::query(&query).execute(pool).await?;
        partition_start += BLOCK_PARTITION_SIZE;
    }

    Ok(())
}
