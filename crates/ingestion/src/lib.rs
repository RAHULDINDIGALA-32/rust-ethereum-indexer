use bigdecimal::BigDecimal;
use decoder::decode_erc20_transfer;
use rpc::{BlockMetadata, RpcClient};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use storage::{
    Erc20TransferRecord, PgPool, commit_finalized_range, commit_hot_range, get_block_hash,
    get_checkpoint, get_hot_block_hash, list_hot_block_numbers, list_hot_blocks,
    rollback_from_block, rollback_hot_from_block,
};
use tokio::time::{Duration, sleep};

pub struct BackfillEngine {
    pub rpc_client: RpcClient,
    pub db_pool: PgPool,
    pub contract_address: Vec<u8>,
    pub confirmation_depth: u64,
    pub reorg_lookback: u64,
    pub hot_window_size: u64,
    pub live_poll_interval_secs: u64,
}

const BLOCK_BATCH_SIZE: u64 = 10;

struct RangePayload {
    transfer_records: Vec<Erc20TransferRecord>,
    indexed_blocks: Vec<(u64, Vec<u8>)>,
}

impl BackfillEngine {
    pub async fn run(
        self: Arc<Self>,
        start_block: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let latest_block = self.rpc_client.get_latest_block_number().await?;
            let safe_head = latest_block.saturating_sub(self.confirmation_depth);
            let finalized_checkpoint = get_checkpoint(&self.db_pool).await?;

            if finalized_checkpoint >= start_block
                && self
                    .reconcile_finalized_history(start_block, finalized_checkpoint)
                    .await?
            {
                continue;
            }

            if self.reconcile_hot_history().await? {
                continue;
            }

            let mut made_progress = false;

            let finalized_checkpoint = get_checkpoint(&self.db_pool).await?;
            let next_finalized_block = finalized_checkpoint.saturating_add(1).max(start_block);

            if next_finalized_block <= safe_head {
                let end_block =
                    std::cmp::min(next_finalized_block + BLOCK_BATCH_SIZE - 1, safe_head);
                println!(
                    "Finalizing blocks {} -> {} (safe head {}, latest {})",
                    next_finalized_block, end_block, safe_head, latest_block
                );

                if self
                    .process_finalized_range(next_finalized_block, end_block)
                    .await?
                {
                    made_progress = true;
                }
            }

            let finalized_checkpoint = get_checkpoint(&self.db_pool).await?;
            let hot_window_start = self
                .compute_hot_window_start(start_block, latest_block)
                .max(finalized_checkpoint.saturating_add(1));

            if finalized_checkpoint.saturating_add(1)
                >= self.compute_hot_window_start(start_block, latest_block)
            {
                if let Some(next_hot_block) = self
                    .next_hot_block_to_index(hot_window_start, latest_block)
                    .await?
                {
                    let end_block =
                        std::cmp::min(next_hot_block + BLOCK_BATCH_SIZE - 1, latest_block);
                    println!(
                        "Indexing hot blocks {} -> {} (latest {})",
                        next_hot_block, end_block, latest_block
                    );

                    if self.process_hot_range(next_hot_block, end_block).await? {
                        made_progress = true;
                    }
                }
            }

            if !made_progress {
                println!(
                    "Finalized checkpoint {} is current; hot window is caught up through latest {}. Sleeping {}s.",
                    finalized_checkpoint, latest_block, self.live_poll_interval_secs
                );

                sleep(Duration::from_secs(self.live_poll_interval_secs)).await;
            }
        }
    }

    async fn process_finalized_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let payload = self.build_range_payload(start_block, end_block).await?;

        for (block_number, block_hash) in &payload.indexed_blocks {
            if let Some(existing_hash) = get_block_hash(&self.db_pool, *block_number).await? {
                if existing_hash != *block_hash {
                    println!("Finalized reorg detected at block {}", block_number);
                    self.handle_finalized_reorg(*block_number).await?;
                    return Ok(false);
                }
            }
        }

        commit_finalized_range(
            &self.db_pool,
            &payload.transfer_records,
            &payload.indexed_blocks,
            start_block,
            end_block,
        )
        .await?;

        println!("Finalized checkpoint advanced to {}", end_block);
        Ok(true)
    }

    async fn process_hot_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let payload = self.build_range_payload(start_block, end_block).await?;

        for (block_number, block_hash) in &payload.indexed_blocks {
            if let Some(existing_hash) = get_hot_block_hash(&self.db_pool, *block_number).await? {
                if existing_hash != *block_hash {
                    println!("Hot reorg detected at block {}", block_number);
                    self.handle_hot_reorg(*block_number).await?;
                    return Ok(false);
                }
            }
        }

        commit_hot_range(
            &self.db_pool,
            &payload.transfer_records,
            &payload.indexed_blocks,
        )
        .await?;

        Ok(true)
    }

    async fn handle_finalized_reorg(
        &self,
        block_number: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!(
            "Rolling back finalized and hot state from block {}",
            block_number
        );
        rollback_from_block(&self.db_pool, block_number).await?;
        Ok(())
    }

    async fn handle_hot_reorg(
        &self,
        block_number: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Rolling back hot state from block {}", block_number);
        rollback_hot_from_block(&self.db_pool, block_number).await?;
        Ok(())
    }

    async fn reconcile_finalized_history(
        &self,
        start_block: u64,
        checkpoint: u64,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let lookback = self.reorg_lookback.max(self.confirmation_depth).max(1);
        let validation_start = checkpoint
            .saturating_sub(lookback.saturating_sub(1))
            .max(start_block);

        for block_number in validation_start..=checkpoint {
            let chain_block = self.fetch_block_metadata(block_number).await?;

            match get_block_hash(&self.db_pool, block_number).await? {
                Some(stored_hash) if stored_hash != chain_block.hash => {
                    println!(
                        "Finalized reorg detected while revalidating block {}",
                        block_number
                    );
                    self.handle_finalized_reorg(block_number).await?;
                    return Ok(true);
                }
                Some(_) => {}
                None => {
                    println!(
                        "Missing finalized block metadata for block {}. Rebuilding from there.",
                        block_number
                    );
                    self.handle_finalized_reorg(block_number).await?;
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    async fn reconcile_hot_history(
        &self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        for (block_number, stored_hash) in list_hot_blocks(&self.db_pool).await? {
            let chain_block = self.fetch_block_metadata(block_number).await?;

            if stored_hash != chain_block.hash {
                println!(
                    "Hot reorg detected while revalidating block {}",
                    block_number
                );
                self.handle_hot_reorg(block_number).await?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn next_hot_block_to_index(
        &self,
        hot_window_start: u64,
        latest_block: u64,
    ) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
        if hot_window_start > latest_block {
            return Ok(None);
        }

        let stored_blocks =
            list_hot_block_numbers(&self.db_pool, hot_window_start, latest_block).await?;

        let mut expected_block = hot_window_start;
        for block_number in stored_blocks {
            if block_number < expected_block {
                continue;
            }

            if block_number > expected_block {
                return Ok(Some(expected_block));
            }

            expected_block = expected_block.saturating_add(1);
        }

        if expected_block <= latest_block {
            Ok(Some(expected_block))
        } else {
            Ok(None)
        }
    }

    async fn build_range_payload(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<RangePayload, Box<dyn std::error::Error + Send + Sync>> {
        let block_metadata = self
            .fetch_block_metadata_range(start_block, end_block)
            .await?;
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
            .fetch_erc20_transfer_logs(
                start_block,
                end_block,
                if self.contract_address.is_empty() {
                    None
                } else {
                    Some(self.contract_address.as_slice())
                },
            )
            .await?;

        let mut transfer_records = Vec::new();

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

                transfer_records.push(record);
            }
        }

        let indexed_blocks = block_metadata
            .into_iter()
            .map(|block| (block.number, block.hash))
            .collect();

        Ok(RangePayload {
            transfer_records,
            indexed_blocks,
        })
    }

    async fn fetch_block_metadata_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<BlockMetadata>, Box<dyn std::error::Error + Send + Sync>> {
        let mut block_metadata = Vec::with_capacity((end_block - start_block + 1) as usize);

        for block_number in start_block..=end_block {
            block_metadata.push(self.fetch_block_metadata(block_number).await?);
        }

        Ok(block_metadata)
    }

    async fn fetch_block_metadata(
        &self,
        block_number: u64,
    ) -> Result<BlockMetadata, Box<dyn std::error::Error + Send + Sync>> {
        self.rpc_client
            .get_block_metadata(block_number)
            .await?
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Block {block_number} not found on the RPC provider"),
                )
            })
            .map_err(Into::into)
    }

    fn compute_hot_window_start(&self, start_block: u64, latest_block: u64) -> u64 {
        let hot_window_size = self.hot_window_size.max(self.confirmation_depth + 1).max(1);
        latest_block
            .saturating_sub(hot_window_size.saturating_sub(1))
            .max(start_block)
    }
}

fn missing_field_error(field_name: &'static str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("RPC log is missing required field: {field_name}"),
    )
}
