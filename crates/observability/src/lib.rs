use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_LOG_FILTER: &str = "info,hyper=warn,sqlx=warn";

pub struct Observability {
    pub metrics: Arc<IndexerMetrics>,
    pub prometheus_handle: PrometheusHandle,
}

#[derive(Debug, Clone)]
pub struct BatchMetrics {
    pub lane: &'static str,
    pub start_block: u64,
    pub end_block: u64,
    pub latest_block: u64,
    pub checkpoint: u64,
    pub blocks: u64,
    pub events: u64,
    pub tx_count: u64,
    pub duplicate_events: u64,
    pub tip_lag_blocks: u64,
    pub avg_ingestion_delay_secs: f64,
    pub max_ingestion_delay_secs: f64,
    pub block_fetch_duration: Duration,
    pub log_fetch_duration: Duration,
    pub decode_duration: Duration,
    pub db_commit_duration: Duration,
    pub total_duration: Duration,
}

impl BatchMetrics {
    pub fn slowest_stage(&self) -> (&'static str, Duration, f64) {
        let stages = [
            ("block_fetch", self.block_fetch_duration),
            ("log_fetch", self.log_fetch_duration),
            ("decode", self.decode_duration),
            ("db_commit", self.db_commit_duration),
        ];

        let (stage, duration) = stages
            .into_iter()
            .max_by_key(|(_, duration)| *duration)
            .unwrap_or(("unknown", Duration::ZERO));
        let ratio = if self.total_duration.is_zero() {
            0.0
        } else {
            duration.as_secs_f64() / self.total_duration.as_secs_f64()
        };

        (stage, duration, ratio)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub rolling_window_secs: u64,
    pub blocks_per_second: f64,
    pub events_per_second: f64,
    pub successful_blocks: u64,
    pub failed_blocks: u64,
    pub reorg_events: u64,
    pub rollbacks: u64,
    pub rpc_failures: u64,
    pub rpc_retries: u64,
    pub checkpoint_recoveries: u64,
    pub duplicate_inserts: u64,
    pub finalized_tip_lag_blocks: u64,
    pub hot_tip_lag_blocks: u64,
    pub last_ingestion_delay_secs: u64,
    pub finalized_checkpoint: u64,
    pub hot_checkpoint: u64,
}

pub struct IndexerMetrics {
    rolling_window_secs: u64,
    throughput: Mutex<RollingThroughput>,
    successful_blocks: AtomicU64,
    failed_blocks: AtomicU64,
    reorg_events: AtomicU64,
    rollbacks: AtomicU64,
    rpc_failures: AtomicU64,
    rpc_retries: AtomicU64,
    checkpoint_recoveries: AtomicU64,
    duplicate_inserts: AtomicU64,
    finalized_tip_lag_blocks: AtomicU64,
    hot_tip_lag_blocks: AtomicU64,
    last_ingestion_delay_secs: AtomicU64,
    finalized_checkpoint: AtomicU64,
    hot_checkpoint: AtomicU64,
}

impl IndexerMetrics {
    pub fn new(rolling_window_secs: u64) -> Self {
        Self {
            rolling_window_secs: rolling_window_secs.max(1),
            throughput: Mutex::new(RollingThroughput::new(rolling_window_secs.max(1))),
            successful_blocks: AtomicU64::new(0),
            failed_blocks: AtomicU64::new(0),
            reorg_events: AtomicU64::new(0),
            rollbacks: AtomicU64::new(0),
            rpc_failures: AtomicU64::new(0),
            rpc_retries: AtomicU64::new(0),
            checkpoint_recoveries: AtomicU64::new(0),
            duplicate_inserts: AtomicU64::new(0),
            finalized_tip_lag_blocks: AtomicU64::new(0),
            hot_tip_lag_blocks: AtomicU64::new(0),
            last_ingestion_delay_secs: AtomicU64::new(0),
            finalized_checkpoint: AtomicU64::new(0),
            hot_checkpoint: AtomicU64::new(0),
        }
    }

    pub fn record_rpc_result(&self, method: &'static str, duration: Duration, success: bool) {
        let outcome = if success { "success" } else { "failure" };
        counter!("indexer_rpc_requests_total", "method" => method, "outcome" => outcome)
            .increment(1);
        histogram!(
            "indexer_rpc_request_duration_seconds",
            "method" => method,
            "outcome" => outcome
        )
        .record(duration.as_secs_f64());

        if !success {
            self.rpc_failures.fetch_add(1, Ordering::Relaxed);
            counter!("indexer_rpc_failures_total", "method" => method).increment(1);
        }
    }

    pub fn record_rpc_retry(&self, method: &'static str, attempt: usize, backoff: Duration) {
        self.rpc_retries.fetch_add(1, Ordering::Relaxed);
        counter!("indexer_rpc_retries_total", "method" => method).increment(1);
        warn!(
            method,
            attempt,
            backoff_ms = backoff.as_millis() as u64,
            "retrying rpc request"
        );
    }

    pub fn record_checkpoint_resume(
        &self,
        worker: &'static str,
        checkpoint: u64,
        start_block: u64,
    ) {
        self.set_checkpoint(worker, checkpoint);

        if checkpoint > 0 && checkpoint >= start_block {
            self.checkpoint_recoveries.fetch_add(1, Ordering::Relaxed);
            counter!("indexer_checkpoint_recoveries_total", "worker" => worker).increment(1);
            info!(
                worker,
                checkpoint, start_block, "resuming from persisted checkpoint"
            );
        } else {
            info!(
                worker,
                checkpoint, start_block, "starting from configured block"
            );
        }
    }

    pub fn set_checkpoint(&self, worker: &'static str, checkpoint: u64) {
        gauge!("indexer_checkpoint_block", "worker" => worker).set(checkpoint as f64);

        match worker {
            "finalized" => self
                .finalized_checkpoint
                .store(checkpoint, Ordering::Relaxed),
            "hot" => self.hot_checkpoint.store(checkpoint, Ordering::Relaxed),
            _ => {}
        }
    }

    pub fn record_tip_lag(&self, lane: &'static str, lag_blocks: u64) {
        gauge!("indexer_tip_lag_blocks", "lane" => lane).set(lag_blocks as f64);

        match lane {
            "finalized" => self
                .finalized_tip_lag_blocks
                .store(lag_blocks, Ordering::Relaxed),
            "hot" => self.hot_tip_lag_blocks.store(lag_blocks, Ordering::Relaxed),
            _ => {}
        }
    }

    pub fn record_reorg_detected(
        &self,
        lane: &'static str,
        source: &'static str,
        block_number: u64,
    ) {
        self.reorg_events.fetch_add(1, Ordering::Relaxed);
        counter!("indexer_reorg_events_total", "lane" => lane, "source" => source).increment(1);
        warn!(lane, source, block_number, "reorg detected");
    }

    pub fn record_rollback(&self, lane: &'static str, block_number: u64) {
        self.rollbacks.fetch_add(1, Ordering::Relaxed);
        counter!("indexer_rollbacks_total", "lane" => lane).increment(1);
        warn!(lane, block_number, "rollback executed");
    }

    pub fn record_duplicate_inserts(&self, lane: &'static str, duplicates: u64) {
        if duplicates == 0 {
            return;
        }

        self.duplicate_inserts
            .fetch_add(duplicates, Ordering::Relaxed);
        counter!("indexer_duplicate_inserts_total", "lane" => lane).increment(duplicates);
    }

    pub fn record_block_ingestion_delay(&self, lane: &'static str, delay: Duration) {
        self.last_ingestion_delay_secs
            .store(delay.as_secs(), Ordering::Relaxed);
        histogram!("indexer_block_ingestion_delay_seconds", "lane" => lane)
            .record(delay.as_secs_f64());
    }

    pub fn record_batch_success(&self, batch: BatchMetrics) {
        self.successful_blocks
            .fetch_add(batch.blocks, Ordering::Relaxed);
        self.record_tip_lag(batch.lane, batch.tip_lag_blocks);
        self.set_checkpoint(batch.lane, batch.checkpoint);
        self.record_duplicate_inserts(batch.lane, batch.duplicate_events);

        counter!("indexer_blocks_processed_total", "lane" => batch.lane).increment(batch.blocks);
        counter!("indexer_events_processed_total", "lane" => batch.lane).increment(batch.events);
        counter!(
            "indexer_block_processing_total",
            "lane" => batch.lane,
            "status" => "success"
        )
        .increment(batch.blocks);

        histogram!("indexer_stage_duration_seconds", "lane" => batch.lane, "stage" => "block_fetch")
            .record(batch.block_fetch_duration.as_secs_f64());
        histogram!("indexer_stage_duration_seconds", "lane" => batch.lane, "stage" => "log_fetch")
            .record(batch.log_fetch_duration.as_secs_f64());
        histogram!("indexer_stage_duration_seconds", "lane" => batch.lane, "stage" => "decode")
            .record(batch.decode_duration.as_secs_f64());
        histogram!("indexer_stage_duration_seconds", "lane" => batch.lane, "stage" => "db_commit")
            .record(batch.db_commit_duration.as_secs_f64());
        histogram!("indexer_batch_processing_duration_seconds", "lane" => batch.lane)
            .record(batch.total_duration.as_secs_f64());

        let rolling = self
            .throughput
            .lock()
            .expect("rolling throughput lock poisoned")
            .record(batch.blocks, batch.events);

        gauge!("indexer_blocks_processed_per_second_rolling").set(rolling.blocks_per_second);
        gauge!("indexer_events_processed_per_second_rolling").set(rolling.events_per_second);

        let processing_ms = batch.total_duration.as_millis() as u64;
        info!(
            lane = batch.lane,
            start_block = batch.start_block,
            end_block = batch.end_block,
            latest_block = batch.latest_block,
            checkpoint = batch.checkpoint,
            blocks = batch.blocks,
            events = batch.events,
            tx_count = batch.tx_count,
            duplicate_events = batch.duplicate_events,
            tip_lag_blocks = batch.tip_lag_blocks,
            avg_ingestion_delay_secs = batch.avg_ingestion_delay_secs,
            max_ingestion_delay_secs = batch.max_ingestion_delay_secs,
            rolling_blocks_per_second = rolling.blocks_per_second,
            rolling_events_per_second = rolling.events_per_second,
            block_fetch_ms = batch.block_fetch_duration.as_millis() as u64,
            log_fetch_ms = batch.log_fetch_duration.as_millis() as u64,
            decode_ms = batch.decode_duration.as_millis() as u64,
            db_commit_ms = batch.db_commit_duration.as_millis() as u64,
            processing_ms,
            "processed block batch"
        );

        let (slowest_stage, slowest_duration, ratio) = batch.slowest_stage();
        if ratio >= 0.6 {
            warn!(
                lane = batch.lane,
                start_block = batch.start_block,
                end_block = batch.end_block,
                slowest_stage,
                slowest_stage_ms = slowest_duration.as_millis() as u64,
                stage_ratio = ratio,
                processing_ms,
                "batch bottleneck candidate"
            );
        }
    }

    pub fn record_batch_failure(
        &self,
        lane: &'static str,
        start_block: u64,
        end_block: u64,
        blocks: u64,
        error: &dyn std::fmt::Display,
    ) {
        self.failed_blocks.fetch_add(blocks, Ordering::Relaxed);
        counter!(
            "indexer_block_processing_total",
            "lane" => lane,
            "status" => "failure"
        )
        .increment(blocks);
        warn!(lane, start_block, end_block, blocks, error = %error, "block batch failed");
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let rolling = self
            .throughput
            .lock()
            .expect("rolling throughput lock poisoned")
            .snapshot();

        MetricsSnapshot {
            rolling_window_secs: self.rolling_window_secs,
            blocks_per_second: rolling.blocks_per_second,
            events_per_second: rolling.events_per_second,
            successful_blocks: self.successful_blocks.load(Ordering::Relaxed),
            failed_blocks: self.failed_blocks.load(Ordering::Relaxed),
            reorg_events: self.reorg_events.load(Ordering::Relaxed),
            rollbacks: self.rollbacks.load(Ordering::Relaxed),
            rpc_failures: self.rpc_failures.load(Ordering::Relaxed),
            rpc_retries: self.rpc_retries.load(Ordering::Relaxed),
            checkpoint_recoveries: self.checkpoint_recoveries.load(Ordering::Relaxed),
            duplicate_inserts: self.duplicate_inserts.load(Ordering::Relaxed),
            finalized_tip_lag_blocks: self.finalized_tip_lag_blocks.load(Ordering::Relaxed),
            hot_tip_lag_blocks: self.hot_tip_lag_blocks.load(Ordering::Relaxed),
            last_ingestion_delay_secs: self.last_ingestion_delay_secs.load(Ordering::Relaxed),
            finalized_checkpoint: self.finalized_checkpoint.load(Ordering::Relaxed),
            hot_checkpoint: self.hot_checkpoint.load(Ordering::Relaxed),
        }
    }

    pub fn spawn_snapshot_logger(self: &Arc<Self>, every: Duration) -> JoinHandle<()> {
        let metrics = Arc::clone(self);

        tokio::spawn(async move {
            let mut ticker = interval(every.max(Duration::from_secs(1)));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                ticker.tick().await;
                let snapshot = metrics.snapshot();
                info!(
                    rolling_window_secs = snapshot.rolling_window_secs,
                    rolling_blocks_per_second = snapshot.blocks_per_second,
                    rolling_events_per_second = snapshot.events_per_second,
                    successful_blocks = snapshot.successful_blocks,
                    failed_blocks = snapshot.failed_blocks,
                    reorg_events = snapshot.reorg_events,
                    rollbacks = snapshot.rollbacks,
                    rpc_failures = snapshot.rpc_failures,
                    rpc_retries = snapshot.rpc_retries,
                    checkpoint_recoveries = snapshot.checkpoint_recoveries,
                    duplicate_inserts = snapshot.duplicate_inserts,
                    finalized_tip_lag_blocks = snapshot.finalized_tip_lag_blocks,
                    hot_tip_lag_blocks = snapshot.hot_tip_lag_blocks,
                    last_ingestion_delay_secs = snapshot.last_ingestion_delay_secs,
                    finalized_checkpoint = snapshot.finalized_checkpoint,
                    hot_checkpoint = snapshot.hot_checkpoint,
                    "indexer metrics snapshot"
                );
            }
        })
    }
}

pub fn init(
    service_name: &'static str,
    rolling_window_secs: u64,
) -> Result<Observability, Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();
    describe_metrics();

    let prometheus_handle = PrometheusBuilder::new()
        .add_global_label("service", service_name)
        .install_recorder()?;
    let metrics = Arc::new(IndexerMetrics::new(rolling_window_secs.max(1)));

    info!(
        service_name,
        rolling_window_secs, "observability initialized"
    );

    Ok(Observability {
        metrics,
        prometheus_handle,
    })
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    let log_format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "json".to_owned());

    if log_format.eq_ignore_ascii_case("pretty") {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .compact()
            .init();
    } else {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_target(false)
            .with_current_span(false)
            .with_span_list(false)
            .init();
    }
}

fn describe_metrics() {
    describe_counter!(
        "indexer_blocks_processed_total",
        "Total number of successfully processed blocks."
    );
    describe_counter!(
        "indexer_events_processed_total",
        "Total number of decoded ERC-20 Transfer events."
    );
    describe_counter!(
        "indexer_block_processing_total",
        "Block processing outcome counts."
    );
    describe_counter!("indexer_reorg_events_total", "Number of reorg detections.");
    describe_counter!("indexer_rollbacks_total", "Number of rollback operations.");
    describe_counter!("indexer_rpc_requests_total", "RPC request outcomes.");
    describe_counter!(
        "indexer_rpc_failures_total",
        "RPC failures before success or exhaustion."
    );
    describe_counter!("indexer_rpc_retries_total", "RPC retry attempts.");
    describe_counter!(
        "indexer_checkpoint_recoveries_total",
        "Checkpoint-based crash recoveries."
    );
    describe_counter!(
        "indexer_duplicate_inserts_total",
        "Duplicate transfer inserts skipped by idempotent writes."
    );
    describe_gauge!(
        "indexer_blocks_processed_per_second_rolling",
        "Rolling average blocks processed per second."
    );
    describe_gauge!(
        "indexer_events_processed_per_second_rolling",
        "Rolling average events processed per second."
    );
    describe_gauge!(
        "indexer_tip_lag_blocks",
        "Distance from the chain head in blocks."
    );
    describe_gauge!(
        "indexer_checkpoint_block",
        "Latest persisted checkpoint per worker."
    );
    describe_histogram!(
        "indexer_rpc_request_duration_seconds",
        "RPC request latency."
    );
    describe_histogram!(
        "indexer_block_ingestion_delay_seconds",
        "Delay between block timestamp and ingestion."
    );
    describe_histogram!(
        "indexer_stage_duration_seconds",
        "Batch stage timing by lane and stage."
    );
    describe_histogram!(
        "indexer_batch_processing_duration_seconds",
        "End-to-end batch processing latency."
    );
}

#[derive(Default)]
struct RollingSnapshot {
    blocks_per_second: f64,
    events_per_second: f64,
}

struct RollingThroughput {
    window_secs: u64,
    started_at_sec: Option<u64>,
    buckets: VecDeque<Bucket>,
}

#[derive(Clone, Copy)]
struct Bucket {
    second: u64,
    blocks: u64,
    events: u64,
}

impl RollingThroughput {
    fn new(window_secs: u64) -> Self {
        Self {
            window_secs: window_secs.max(1),
            started_at_sec: None,
            buckets: VecDeque::new(),
        }
    }

    fn record(&mut self, blocks: u64, events: u64) -> RollingSnapshot {
        let now = unix_timestamp_secs();
        self.started_at_sec.get_or_insert(now);

        match self.buckets.back_mut() {
            Some(bucket) if bucket.second == now => {
                bucket.blocks += blocks;
                bucket.events += events;
            }
            _ => self.buckets.push_back(Bucket {
                second: now,
                blocks,
                events,
            }),
        }

        self.prune(now);
        self.snapshot_for(now)
    }

    fn snapshot(&mut self) -> RollingSnapshot {
        let now = unix_timestamp_secs();
        self.prune(now);
        self.snapshot_for(now)
    }

    fn prune(&mut self, now: u64) {
        let oldest_second = now.saturating_sub(self.window_secs.saturating_sub(1));
        while matches!(self.buckets.front(), Some(bucket) if bucket.second < oldest_second) {
            self.buckets.pop_front();
        }
    }

    fn snapshot_for(&self, now: u64) -> RollingSnapshot {
        let total_blocks: u64 = self.buckets.iter().map(|bucket| bucket.blocks).sum();
        let total_events: u64 = self.buckets.iter().map(|bucket| bucket.events).sum();
        let denominator = self
            .started_at_sec
            .map(|started| now.saturating_sub(started).saturating_add(1))
            .unwrap_or(1)
            .min(self.window_secs)
            .max(1);

        RollingSnapshot {
            blocks_per_second: total_blocks as f64 / denominator as f64,
            events_per_second: total_events as f64 / denominator as f64,
        }
    }
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::RollingThroughput;

    #[test]
    fn rolling_throughput_counts_recent_buckets() {
        let mut rolling = RollingThroughput::new(5);
        let snapshot = rolling.record(10, 40);

        assert!(snapshot.blocks_per_second >= 10.0);
        assert!(snapshot.events_per_second >= 40.0);
    }
}
