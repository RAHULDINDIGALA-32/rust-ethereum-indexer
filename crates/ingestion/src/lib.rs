use bigdecimal::BigDecimal;
use decoder::decode_erc20_transfer;
use rpc::{BlockMetadata, RpcClient};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use storage::{
    Erc20TransferRecord, PgPool, get_block_hash, get_checkpoint, insert_batch_erc20_transfers,
    insert_blocks, rollback_from_block, update_checkpoint,
};
use tokio::time::{Duration, sleep};

pub struct BackfillEngine {
    pub rpc_client: RpcClient,
    pub db_pool: PgPool,
    pub contract_address: Vec<u8>,
    pub confirmation_depth: u64,
    pub reorg_lookback: u64,
    pub live_poll_interval_secs: u64,
}

const BLOCK_BATCH_SIZE: u64 = 10;
const RECORD_BATCH_SIZE: usize = 100;

impl BackfillEngine {
    pub async fn run(
        self: Arc<Self>,
        start_block: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let latest_block = self.rpc_client.get_latest_block_number().await?;
            let safe_head = latest_block.saturating_sub(self.confirmation_depth);
            let checkpoint = get_checkpoint(&self.db_pool).await?;

            if checkpoint >= start_block
                && self
                    .reconcile_recent_history(start_block, checkpoint)
                    .await?
            {
                continue;
            }

            let checkpoint = get_checkpoint(&self.db_pool).await?;
            let next_block = checkpoint.saturating_add(1).max(start_block);

            if next_block > safe_head {
                println!(
                    "Checkpoint {} is at the current safe head {} (latest {}). Sleeping {}s.",
                    checkpoint, safe_head, latest_block, self.live_poll_interval_secs
                );

                sleep(Duration::from_secs(self.live_poll_interval_secs)).await;
                continue;
            }

            let end_block = std::cmp::min(next_block + BLOCK_BATCH_SIZE - 1, safe_head);

            println!(
                "Indexing blocks {} -> {} (safe head {}, latest {})",
                next_block, end_block, safe_head, latest_block
            );

            if self.process_range(next_block, end_block).await? {
                update_checkpoint(&self.db_pool, end_block).await?;
                println!("Checkpoint advanced to {}", end_block);
            }
        }
    }

    pub async fn process_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let block_metadata = self
            .fetch_block_metadata_range(start_block, end_block)
            .await?;

        for block in &block_metadata {
            if let Some(existing_hash) = get_block_hash(&self.db_pool, block.number).await? {
                if existing_hash != block.hash {
                    println!("Reorg detected at block {}", block.number);
                    self.handle_reorg(block.number).await?;
                    return Ok(false);
                }
            }
        }

        let block_timestamps: HashMap<u64, i64> = block_metadata
            .iter()
            .map(|block| (block.number, block.timestamp as i64))
            .collect();
        let block_hashes: HashMap<u64, Vec<u8>> = block_metadata
            .iter()
            .map(|block| (block.number, block.hash.clone()))
            .collect();

        let logs = self
            .rpc_client
            .fetch_erc20_transfer_logs(start_block, end_block)
            .await?;

        let mut records_batch = Vec::new();

        for log in logs {
            let block_number = log
                .block_number
                .map(|value| value as u64)
                .ok_or_else(|| missing_field_error("block_number"))?;

            if let Some(erc20_transfer_event) = decode_erc20_transfer(&log) {
                let record = Erc20TransferRecord {
                    block_number: block_number as i64,
                    block_hash: block_hashes
                        .get(&block_number)
                        .cloned()
                        .ok_or_else(|| missing_field_error("block_hash"))?,
                    txn_hash: log
                        .transaction_hash
                        .map(|value| value.to_vec())
                        .ok_or_else(|| missing_field_error("transaction_hash"))?,
                    log_index: log
                        .log_index
                        .map(|value| value as i32)
                        .ok_or_else(|| missing_field_error("log_index"))?,
                    contract_address: log.address().to_vec(),
                    from_address: erc20_transfer_event.from.to_vec(),
                    to_address: erc20_transfer_event.to.to_vec(),
                    value: BigDecimal::from_str(&erc20_transfer_event.value.to_string()).unwrap(),
                    timestamp: *block_timestamps
                        .get(&block_number)
                        .ok_or_else(|| missing_field_error("timestamp"))?,
                };

                records_batch.push(record);

                if records_batch.len() >= RECORD_BATCH_SIZE {
                    insert_batch_erc20_transfers(&self.db_pool, &records_batch).await?;
                    records_batch.clear();
                }
            }
        }

        if !records_batch.is_empty() {
            insert_batch_erc20_transfers(&self.db_pool, &records_batch).await?;
        }

        let indexed_blocks: Vec<(u64, Vec<u8>)> = block_metadata
            .into_iter()
            .map(|block| (block.number, block.hash))
            .collect();

        insert_blocks(&self.db_pool, &indexed_blocks).await?;

        Ok(true)
    }

    pub async fn handle_reorg(
        &self,
        block_number: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Rolling back from block {}", block_number);

        rollback_from_block(&self.db_pool, block_number).await?;
        update_checkpoint(&self.db_pool, block_number.saturating_sub(1)).await?;

        Ok(())
    }

    async fn reconcile_recent_history(
        &self,
        start_block: u64,
        checkpoint: u64,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let lookback = self.reorg_lookback.max(self.confirmation_depth).max(1);
        let validation_start = checkpoint
            .saturating_sub(lookback.saturating_sub(1))
            .max(start_block);

        for block_number in validation_start..=checkpoint {
            let chain_block = self
                .rpc_client
                .get_block_metadata(block_number)
                .await?
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Block {block_number} not found on the RPC provider"),
                    )
                })?;

            match get_block_hash(&self.db_pool, block_number).await? {
                Some(stored_hash) if stored_hash != chain_block.hash => {
                    println!("Reorg detected while revalidating block {}", block_number);
                    self.handle_reorg(block_number).await?;
                    return Ok(true);
                }
                Some(_) => {}
                None => {
                    println!(
                        "Missing block metadata for indexed block {}. Rebuilding recent history.",
                        block_number
                    );
                    self.handle_reorg(block_number).await?;
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    async fn fetch_block_metadata_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<BlockMetadata>, Box<dyn std::error::Error + Send + Sync>> {
        let mut block_metadata = Vec::with_capacity((end_block - start_block + 1) as usize);

        for block_number in start_block..=end_block {
            let block = self
                .rpc_client
                .get_block_metadata(block_number)
                .await?
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Block {block_number} not found on the RPC provider"),
                    )
                })?;

            block_metadata.push(block);
        }

        Ok(block_metadata)
    }
}

fn missing_field_error(field_name: &'static str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("RPC log is missing required field: {field_name}"),
    )
}
