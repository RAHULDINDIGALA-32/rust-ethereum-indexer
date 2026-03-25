mod models;
pub use models::Erc20TransferRecord;

use sqlx::{Pool, Postgres, QueryBuilder, Row, Transaction, postgres::PgPoolOptions};

pub type PgPool = Pool<Postgres>;

const MAX_POOL_SIZE: u32 = 10;
const BLOCK_PARTITION_SIZE: i64 = 100_000;
const COLD_TRANSFERS_TABLE: &str = "erc20_transfers";
const HOT_TRANSFERS_TABLE: &str = "hot_erc20_transfers";
const COLD_BLOCKS_TABLE: &str = "indexed_blocks";
const HOT_BLOCKS_TABLE: &str = "hot_indexed_blocks";

pub async fn create_db_pool(database_url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(MAX_POOL_SIZE)
        .connect(database_url)
        .await
        .expect("Failed to connect to database.")
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

pub async fn get_block_hash(
    db_pool: &PgPool,
    block_number: u64,
) -> Result<Option<Vec<u8>>, sqlx::Error> {
    get_block_hash_from_table(db_pool, COLD_BLOCKS_TABLE, block_number).await
}

pub async fn get_hot_block_hash(
    db_pool: &PgPool,
    block_number: u64,
) -> Result<Option<Vec<u8>>, sqlx::Error> {
    get_block_hash_from_table(db_pool, HOT_BLOCKS_TABLE, block_number).await
}

pub async fn list_hot_block_numbers(
    db_pool: &PgPool,
    start_block: u64,
    end_block: u64,
) -> Result<Vec<u64>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT block_number
         FROM hot_indexed_blocks
         WHERE block_number BETWEEN $1 AND $2
         ORDER BY block_number ASC",
    )
    .bind(start_block as i64)
    .bind(end_block as i64)
    .fetch_all(db_pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| row.get::<i64, _>("block_number") as u64)
        .collect())
}

pub async fn list_hot_blocks(db_pool: &PgPool) -> Result<Vec<(u64, Vec<u8>)>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT block_number, block_hash
         FROM hot_indexed_blocks
         ORDER BY block_number ASC",
    )
    .fetch_all(db_pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.get::<i64, _>("block_number") as u64,
                row.get::<Vec<u8>, _>("block_hash"),
            )
        })
        .collect())
}

pub async fn commit_finalized_range(
    db_pool: &PgPool,
    transfer_records: &[Erc20TransferRecord],
    indexed_blocks: &[(u64, Vec<u8>)],
    start_block: u64,
    checkpoint: u64,
) -> Result<(), sqlx::Error> {
    let mut tx = db_pool.begin().await?;

    insert_transfers_into_tx(&mut tx, COLD_TRANSFERS_TABLE, transfer_records).await?;
    insert_blocks_into_tx(&mut tx, COLD_BLOCKS_TABLE, indexed_blocks).await?;
    delete_range_between_blocks_tx(
        &mut tx,
        HOT_TRANSFERS_TABLE,
        HOT_BLOCKS_TABLE,
        start_block,
        checkpoint,
    )
    .await?;
    update_checkpoint_tx(&mut tx, checkpoint).await?;

    tx.commit().await?;
    Ok(())
}

pub async fn commit_hot_range(
    db_pool: &PgPool,
    transfer_records: &[Erc20TransferRecord],
    indexed_blocks: &[(u64, Vec<u8>)],
) -> Result<(), sqlx::Error> {
    let mut tx = db_pool.begin().await?;

    insert_transfers_into_tx(&mut tx, HOT_TRANSFERS_TABLE, transfer_records).await?;
    insert_blocks_into_tx(&mut tx, HOT_BLOCKS_TABLE, indexed_blocks).await?;

    tx.commit().await?;
    Ok(())
}

pub async fn rollback_from_block(db_pool: &PgPool, block_number: u64) -> Result<(), sqlx::Error> {
    let mut tx = db_pool.begin().await?;

    delete_range_from_lane_tx(
        &mut tx,
        COLD_TRANSFERS_TABLE,
        COLD_BLOCKS_TABLE,
        block_number,
    )
    .await?;
    delete_range_from_lane_tx(&mut tx, HOT_TRANSFERS_TABLE, HOT_BLOCKS_TABLE, block_number).await?;
    update_checkpoint_tx(&mut tx, block_number.saturating_sub(1)).await?;

    tx.commit().await?;
    Ok(())
}

pub async fn rollback_hot_from_block(
    db_pool: &PgPool,
    block_number: u64,
) -> Result<(), sqlx::Error> {
    let mut tx = db_pool.begin().await?;
    delete_range_from_lane_tx(&mut tx, HOT_TRANSFERS_TABLE, HOT_BLOCKS_TABLE, block_number).await?;
    tx.commit().await?;
    Ok(())
}

async fn get_block_hash_from_table(
    db_pool: &PgPool,
    table_name: &str,
    block_number: u64,
) -> Result<Option<Vec<u8>>, sqlx::Error> {
    let query = format!(
        "SELECT block_hash
         FROM {table_name}
         WHERE block_number = $1"
    );

    let record = sqlx::query(&query)
        .bind(block_number as i64)
        .fetch_optional(db_pool)
        .await?;

    Ok(record.map(|row| row.get::<Vec<u8>, _>("block_hash")))
}

async fn update_checkpoint_tx(
    tx: &mut Transaction<'_, Postgres>,
    checkpoint: u64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE indexer_checkpoint
         SET last_processed_block = $1
         WHERE id = 1",
    )
    .bind(checkpoint as i64)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn insert_transfers_into_tx(
    tx: &mut Transaction<'_, Postgres>,
    table_name: &str,
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

    ensure_partitions_for_range_tx(tx, table_name, min_block, max_block).await?;

    let mut query_builder = QueryBuilder::<Postgres>::new(format!(
        "INSERT INTO {table_name} (
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
    ));

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
    query_builder.build().execute(&mut **tx).await?;

    Ok(())
}

async fn insert_blocks_into_tx(
    tx: &mut Transaction<'_, Postgres>,
    table_name: &str,
    indexed_blocks: &[(u64, Vec<u8>)],
) -> Result<(), sqlx::Error> {
    if indexed_blocks.is_empty() {
        return Ok(());
    }

    let mut query_builder = QueryBuilder::<Postgres>::new(format!(
        "INSERT INTO {table_name} (block_number, block_hash) "
    ));

    query_builder.push_values(indexed_blocks, |mut builder, (block_number, block_hash)| {
        builder
            .push_bind(*block_number as i64)
            .push_bind(block_hash);
    });

    query_builder.push(
        " ON CONFLICT (block_number) DO UPDATE
          SET block_hash = EXCLUDED.block_hash",
    );

    query_builder.build().execute(&mut **tx).await?;

    Ok(())
}

async fn delete_range_from_lane_tx(
    tx: &mut Transaction<'_, Postgres>,
    transfers_table: &str,
    blocks_table: &str,
    from_block: u64,
) -> Result<(), sqlx::Error> {
    let delete_transfers = format!(
        "DELETE FROM {transfers_table}
         WHERE block_number >= $1"
    );
    sqlx::query(&delete_transfers)
        .bind(from_block as i64)
        .execute(&mut **tx)
        .await?;

    let delete_blocks = format!(
        "DELETE FROM {blocks_table}
         WHERE block_number >= $1"
    );
    sqlx::query(&delete_blocks)
        .bind(from_block as i64)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

async fn delete_range_between_blocks_tx(
    tx: &mut Transaction<'_, Postgres>,
    transfers_table: &str,
    blocks_table: &str,
    start_block: u64,
    end_block: u64,
) -> Result<(), sqlx::Error> {
    let delete_transfers = format!(
        "DELETE FROM {transfers_table}
         WHERE block_number BETWEEN $1 AND $2"
    );
    sqlx::query(&delete_transfers)
        .bind(start_block as i64)
        .bind(end_block as i64)
        .execute(&mut **tx)
        .await?;

    let delete_blocks = format!(
        "DELETE FROM {blocks_table}
         WHERE block_number BETWEEN $1 AND $2"
    );
    sqlx::query(&delete_blocks)
        .bind(start_block as i64)
        .bind(end_block as i64)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

async fn ensure_partitions_for_range_tx(
    tx: &mut Transaction<'_, Postgres>,
    table_name: &str,
    start_block: i64,
    end_block: i64,
) -> Result<(), sqlx::Error> {
    let first_partition_start = (start_block / BLOCK_PARTITION_SIZE) * BLOCK_PARTITION_SIZE;
    let last_partition_start = (end_block / BLOCK_PARTITION_SIZE) * BLOCK_PARTITION_SIZE;

    let mut partition_start = first_partition_start;
    while partition_start <= last_partition_start {
        let partition_end = partition_start + BLOCK_PARTITION_SIZE;
        let partition_name = format!("{table_name}_p{partition_start}_{partition_end}");
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {partition_name}
             PARTITION OF {table_name}
             FOR VALUES FROM ({partition_start}) TO ({partition_end})"
        );

        sqlx::query(&query).execute(&mut **tx).await?;
        partition_start += BLOCK_PARTITION_SIZE;
    }

    Ok(())
}
