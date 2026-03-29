use alloy::primitives::Address;
use api::start_server;
use ingestion::BackfillEngine;
use observability::init as init_observability;
use rpc::RpcClient;
use sqlx::migrate::Migrator;
use std::sync::Arc;
use storage::create_db_pool;
use tokio::time::Duration;
use tracing::info;

static MIGRATOR: Migrator = sqlx::migrate!("../migrations");

enum IndexerMode {
    Backfill,
    Live,
    Hybrid,
}

impl IndexerMode {
    fn from_env(value: Option<String>) -> Self {
        match value
            .unwrap_or_else(|| "hybrid".to_owned())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "backfill" => Self::Backfill,
            "live" => Self::Live,
            _ => Self::Hybrid,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();

    let rolling_window_secs = std::env::var("METRICS_ROLLING_WINDOW_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60);
    let metrics_log_interval_secs = std::env::var("METRICS_LOG_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(15);
    let rpc_max_retries = std::env::var("RPC_MAX_RETRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);
    let rpc_retry_backoff_ms = std::env::var("RPC_RETRY_BACKOFF_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(500);

    let observability = init_observability("rust-ethereum-indexer", rolling_window_secs)?;
    observability
        .metrics
        .spawn_snapshot_logger(Duration::from_secs(metrics_log_interval_secs));

    let rpc_url = std::env::var("SEPOLIA_RPC_URL").expect("RPC_URL env-var not set!");
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL env-var not set!");

    let start_block = std::env::var("START_BLOCK")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10_463_971);
    let confirmation_depth = std::env::var("CONFIRMATION_DEPTH")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(12);
    let reorg_lookback = std::env::var("REORG_LOOKBACK")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(24);
    let live_poll_interval_secs = std::env::var("LIVE_POLL_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5);
    let live_start_depth = std::env::var("LIVE_START_DEPTH")
        .ok()
        .or_else(|| std::env::var("HOT_WINDOW_SIZE").ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(256);
    let contract_address = std::env::var("CONTRACT_ADDRESS")
        .ok()
        .map(|value| value.parse::<Address>())
        .transpose()?
        .map(|address| address.as_slice().to_vec())
        .unwrap_or_default();
    let indexer_mode = IndexerMode::from_env(std::env::var("INDEXER_MODE").ok());

    let rpc_client = RpcClient::new(
        &rpc_url,
        Arc::clone(&observability.metrics),
        rpc_max_retries,
        Duration::from_millis(rpc_retry_backoff_ms),
    );
    let db_pool = create_db_pool(&db_url).await;

    MIGRATOR
        .run(&db_pool)
        .await
        .expect("Failed to run database migrations");

    info!(
        mode = match indexer_mode {
            IndexerMode::Backfill => "backfill",
            IndexerMode::Live => "live",
            IndexerMode::Hybrid => "hybrid",
        },
        start_block,
        confirmation_depth,
        reorg_lookback,
        live_poll_interval_secs,
        live_start_depth,
        rpc_max_retries,
        rolling_window_secs,
        "indexer runtime configured"
    );

    let indexer_engine = Arc::new(BackfillEngine {
        rpc_client,
        db_pool: db_pool.clone(),
        metrics: Arc::clone(&observability.metrics),
        contract_address,
        confirmation_depth,
        reorg_lookback,
        hot_window_size: live_start_depth,
        live_start_depth,
        live_poll_interval_secs,
    });

    match indexer_mode {
        IndexerMode::Backfill => {
            tokio::try_join!(
                Arc::clone(&indexer_engine).run_backfill(start_block),
                start_server(db_pool, observability.prometheus_handle.clone())
            )?;
        }
        IndexerMode::Live => {
            tokio::try_join!(
                Arc::clone(&indexer_engine).run_live(start_block),
                start_server(db_pool, observability.prometheus_handle.clone())
            )?;
        }
        IndexerMode::Hybrid => {
            tokio::try_join!(
                Arc::clone(&indexer_engine).run_backfill(start_block),
                Arc::clone(&indexer_engine).run_live(start_block),
                start_server(db_pool, observability.prometheus_handle.clone())
            )?;
        }
    }

    Ok(())
}
