use bigdecimal::BigDecimal;
use decoder::decode_erc20_transfer;
use futures::stream::{self, StreamExt, TryStreamExt};
use observability::{BatchMetrics, IndexerMetrics};
use rpc::{BlockMetadata, RpcClient};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH};
use storage::{
    CommitStats, Erc20TransferRecord, PgPool, commit_finalized_range, commit_hot_range,
    get_block_hash, get_checkpoint, get_hot_block_hash, get_live_checkpoint,
    prune_hot_before_block, rollback_from_block, rollback_hot_from_block,
};
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

pub struct BackfillEngine {
    pub rpc_client: RpcClient,
    pub db_pool: PgPool,
    pub metrics: Arc<IndexerMetrics>,
    pub contract_address: Vec<u8>,
    pub confirmation_depth: u64,
    pub reorg_lookback: u64,
    pub hot_window_size: u64,
    pub live_start_depth: u64,
    pub live_poll_interval_secs: u64,
}

const BLOCK_BATCH_SIZE: u64 = 10;
const MAX_CONCURRENT_BLOCK_FETCHES: usize = 4;

struct RangePayload {
    transfer_records: Vec<Erc20TransferRecord>,
    indexed_blocks: Vec<(u64, Vec<u8>)>,
    block_metadata: Vec<BlockMetadata>,
    tx_count: u64,
}

struct RangeBuild {
    payload: RangePayload,
    block_fetch_duration: StdDuration,
    log_fetch_duration: StdDuration,
    decode_duration: StdDuration,
}

impl BackfillEngine {
    pub async fn run_backfill(
        self: Arc<Self>,
        start_block: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let checkpoint = get_checkpoint(&self.db_pool).await?;
        self.metrics
            .record_checkpoint_resume("finalized", checkpoint, start_block);

        loop {
            let latest_block = self.rpc_client.get_latest_block_number().await?;
            let safe_head = latest_block.saturating_sub(self.confirmation_depth);
            let finalized_checkpoint = get_checkpoint(&self.db_pool).await?;
            self.metrics
                .record_tip_lag("finalized", safe_head.saturating_sub(finalized_checkpoint));

            if finalized_checkpoint >= start_block
                && self
                    .reconcile_finalized_history(start_block, finalized_checkpoint)
                    .await?
            {
                continue;
            }

            let finalized_checkpoint = get_checkpoint(&self.db_pool).await?;
            let next_finalized_block = finalized_checkpoint.saturating_add(1).max(start_block);

            if next_finalized_block <= safe_head {
                let end_block =
                    std::cmp::min(next_finalized_block + BLOCK_BATCH_SIZE - 1, safe_head);
                info!(
                    start_block = next_finalized_block,
                    end_block, safe_head, latest_block, "backfill finalizing block range"
                );

                if let Err(error) = self
                    .process_finalized_range(next_finalized_block, end_block, latest_block)
                    .await
                {
                    self.metrics.record_batch_failure(
                        "finalized",
                        next_finalized_block,
                        end_block,
                        end_block
                            .saturating_sub(next_finalized_block)
                            .saturating_add(1),
                        &*error,
                    );
                    return Err(error);
                }
                continue;
            }

            info!(
                checkpoint = finalized_checkpoint,
                safe_head,
                sleep_secs = self.live_poll_interval_secs,
                "backfill is caught up to the safe head"
            );
            sleep(Duration::from_secs(self.live_poll_interval_secs)).await;
        }
    }

    pub async fn run_live(
        self: Arc<Self>,
        start_block: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let checkpoint = get_live_checkpoint(&self.db_pool).await?;
        self.metrics
            .record_checkpoint_resume("hot", checkpoint, start_block);

        loop {
            let latest_block = self.rpc_client.get_latest_block_number().await?;
            let live_start_block = self.compute_live_start_block(start_block, latest_block);

            prune_hot_before_block(&self.db_pool, live_start_block).await?;

            if self.reconcile_hot_history(live_start_block).await? {
                continue;
            }

            let live_checkpoint = get_live_checkpoint(&self.db_pool).await?;
            let next_hot_block = live_checkpoint.saturating_add(1).max(live_start_block);
            self.metrics
                .record_tip_lag("hot", latest_block.saturating_sub(live_checkpoint));

            if next_hot_block <= latest_block {
                let end_block = std::cmp::min(next_hot_block + BLOCK_BATCH_SIZE - 1, latest_block);
                info!(
                    start_block = next_hot_block,
                    end_block, latest_block, "live indexing hot block range"
                );

                if let Err(error) = self
                    .process_hot_range(next_hot_block, end_block, latest_block)
                    .await
                {
                    self.metrics.record_batch_failure(
                        "hot",
                        next_hot_block,
                        end_block,
                        end_block.saturating_sub(next_hot_block).saturating_add(1),
                        &*error,
                    );
                    return Err(error);
                }
                continue;
            }

            info!(
                checkpoint = live_checkpoint,
                latest_block,
                sleep_secs = self.live_poll_interval_secs,
                "live worker is caught up to the chain head"
            );
            sleep(Duration::from_secs(self.live_poll_interval_secs)).await;
        }
    }

    async fn process_finalized_range(
        &self,
        start_block: u64,
        end_block: u64,
        latest_block: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let started_at = Instant::now();
        let build = self.build_range_payload(start_block, end_block).await?;
        let payload = build.payload;

        for (block_number, block_hash) in &payload.indexed_blocks {
            if let Some(existing_hash) = get_block_hash(&self.db_pool, *block_number).await? {
                if existing_hash != *block_hash {
                    self.metrics.record_reorg_detected(
                        "finalized",
                        "range_commit_validation",
                        *block_number,
                    );
                    self.handle_finalized_reorg(*block_number).await?;
                    return Ok(());
                }
            }
        }

        let db_started_at = Instant::now();
        let commit_stats = commit_finalized_range(
            &self.db_pool,
            &payload.transfer_records,
            &payload.indexed_blocks,
            start_block,
            end_block,
        )
        .await?;
        let db_commit_duration = db_started_at.elapsed();

        self.metrics.record_batch_success(self.build_batch_metrics(
            "finalized",
            start_block,
            end_block,
            latest_block,
            &payload,
            commit_stats,
            build.block_fetch_duration,
            build.log_fetch_duration,
            build.decode_duration,
            db_commit_duration,
            started_at.elapsed(),
        ));
        Ok(())
    }

    async fn process_hot_range(
        &self,
        start_block: u64,
        end_block: u64,
        latest_block: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let started_at = Instant::now();
        let build = self.build_range_payload(start_block, end_block).await?;
        let payload = build.payload;

        for (block_number, block_hash) in &payload.indexed_blocks {
            if let Some(existing_hash) = get_hot_block_hash(&self.db_pool, *block_number).await? {
                if existing_hash != *block_hash {
                    self.metrics.record_reorg_detected(
                        "hot",
                        "range_commit_validation",
                        *block_number,
                    );
                    self.handle_hot_reorg(*block_number).await?;
                    return Ok(());
                }
            }
        }

        let db_started_at = Instant::now();
        let commit_stats = commit_hot_range(
            &self.db_pool,
            &payload.transfer_records,
            &payload.indexed_blocks,
            end_block,
        )
        .await?;
        let db_commit_duration = db_started_at.elapsed();

        self.metrics.record_batch_success(self.build_batch_metrics(
            "hot",
            start_block,
            end_block,
            latest_block,
            &payload,
            commit_stats,
            build.block_fetch_duration,
            build.log_fetch_duration,
            build.decode_duration,
            db_commit_duration,
            started_at.elapsed(),
        ));
        Ok(())
    }

    async fn handle_finalized_reorg(
        &self,
        block_number: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.metrics.record_rollback("finalized", block_number);
        rollback_from_block(&self.db_pool, block_number).await?;
        Ok(())
    }

    async fn handle_hot_reorg(
        &self,
        block_number: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.metrics.record_rollback("hot", block_number);
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
                    self.metrics.record_reorg_detected(
                        "finalized",
                        "history_revalidation",
                        block_number,
                    );
                    self.handle_finalized_reorg(block_number).await?;
                    return Ok(true);
                }
                Some(_) => {}
                None => {
                    self.metrics.record_reorg_detected(
                        "finalized",
                        "missing_block_metadata",
                        block_number,
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
        start_block: u64,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let latest_block = self.rpc_client.get_latest_block_number().await?;
        let validation_start = self
            .compute_live_start_block(start_block, latest_block)
            .max(latest_block.saturating_sub(self.reorg_lookback.max(1).saturating_sub(1)));

        for block_number in validation_start..=latest_block {
            let Some(stored_hash) = get_hot_block_hash(&self.db_pool, block_number).await? else {
                continue;
            };

            let chain_block = self.fetch_block_metadata(block_number).await?;
            if stored_hash != chain_block.hash {
                self.metrics
                    .record_reorg_detected("hot", "history_revalidation", block_number);
                self.handle_hot_reorg(block_number).await?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn build_range_payload(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<RangeBuild, Box<dyn std::error::Error + Send + Sync>> {
        let block_fetch_started_at = Instant::now();
        let block_metadata = self
            .fetch_block_metadata_range(start_block, end_block)
            .await?;
        let block_fetch_duration = block_fetch_started_at.elapsed();
        let block_timestamps: HashMap<u64, i64> = block_metadata
            .iter()
            .map(|block| (block.number, block.timestamp as i64))
            .collect();
        let block_hashes: HashMap<u64, Vec<u8>> = block_metadata
            .iter()
            .map(|block| (block.number, block.hash.clone()))
            .collect();

        let logs_started_at = Instant::now();
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
        let log_fetch_duration = logs_started_at.elapsed();

        let decode_started_at = Instant::now();
        let mut transfer_records = Vec::new();
        let mut unique_transactions = HashSet::new();

        for log in logs {
            let block_number = log
                .block_number
                .map(|value| value as u64)
                .ok_or_else(|| missing_field_error("block_number"))?;

            if let Some(erc20_transfer_event) = decode_erc20_transfer(&log) {
                let txn_hash = log
                    .transaction_hash
                    .map(|value| value.to_vec())
                    .ok_or_else(|| missing_field_error("transaction_hash"))?;
                unique_transactions.insert(txn_hash.clone());

                let record = Erc20TransferRecord {
                    block_number: block_number as i64,
                    block_hash: block_hashes
                        .get(&block_number)
                        .cloned()
                        .ok_or_else(|| missing_field_error("block_hash"))?,
                    txn_hash,
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
        let decode_duration = decode_started_at.elapsed();

        let indexed_blocks = block_metadata
            .iter()
            .map(|block| (block.number, block.hash.clone()))
            .collect();

        Ok(RangeBuild {
            payload: RangePayload {
                transfer_records,
                indexed_blocks,
                block_metadata,
                tx_count: unique_transactions.len() as u64,
            },
            block_fetch_duration,
            log_fetch_duration,
            decode_duration,
        })
    }

    async fn fetch_block_metadata_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<BlockMetadata>, Box<dyn std::error::Error + Send + Sync>> {
        stream::iter(start_block..=end_block)
            .map(|block_number| async move { self.fetch_block_metadata(block_number).await })
            .buffered(MAX_CONCURRENT_BLOCK_FETCHES)
            .try_collect()
            .await
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

    fn compute_live_start_block(&self, start_block: u64, latest_block: u64) -> u64 {
        let live_start_depth = self
            .live_start_depth
            .max(self.hot_window_size)
            .max(self.reorg_lookback)
            .max(1);
        latest_block
            .saturating_sub(live_start_depth.saturating_sub(1))
            .max(start_block)
    }

    fn build_batch_metrics(
        &self,
        lane: &'static str,
        start_block: u64,
        end_block: u64,
        latest_block: u64,
        payload: &RangePayload,
        commit_stats: CommitStats,
        block_fetch_duration: StdDuration,
        log_fetch_duration: StdDuration,
        decode_duration: StdDuration,
        db_commit_duration: StdDuration,
        total_duration: StdDuration,
    ) -> BatchMetrics {
        let ingestion_now = unix_timestamp_secs();
        let mut total_delay_secs = 0.0;
        let mut max_delay_secs = 0.0_f64;

        for block in &payload.block_metadata {
            let delay_secs = ingestion_now.saturating_sub(block.timestamp) as f64;
            total_delay_secs += delay_secs;
            max_delay_secs = max_delay_secs.max(delay_secs);
            self.metrics.record_block_ingestion_delay(
                lane,
                StdDuration::from_secs_f64(delay_secs.max(0.0)),
            );
        }

        let blocks = payload.indexed_blocks.len() as u64;
        let avg_ingestion_delay_secs = if blocks == 0 {
            0.0
        } else {
            total_delay_secs / blocks as f64
        };

        if commit_stats.attempted_transfers != commit_stats.inserted_transfers
            || commit_stats.block_rows != blocks
        {
            warn!(
                lane,
                start_block,
                end_block,
                attempted_transfers = commit_stats.attempted_transfers,
                inserted_transfers = commit_stats.inserted_transfers,
                duplicate_transfers = commit_stats.duplicate_transfers,
                block_rows = commit_stats.block_rows,
                expected_blocks = blocks,
                "idempotent write path skipped duplicate rows or replayed a batch"
            );
        }

        BatchMetrics {
            lane,
            start_block,
            end_block,
            latest_block,
            checkpoint: end_block,
            blocks,
            events: payload.transfer_records.len() as u64,
            tx_count: payload.tx_count,
            duplicate_events: commit_stats.duplicate_transfers,
            tip_lag_blocks: latest_block.saturating_sub(end_block),
            avg_ingestion_delay_secs,
            max_ingestion_delay_secs: max_delay_secs,
            block_fetch_duration,
            log_fetch_duration,
            decode_duration,
            db_commit_duration,
            total_duration,
        }
    }
}

fn missing_field_error(field_name: &'static str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("RPC log is missing required field: {field_name}"),
    )
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
