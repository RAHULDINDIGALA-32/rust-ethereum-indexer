use alloy::primitives::Address;
use api::start_server;
use ingestion::BackfillEngine;
use rpc::RpcClient;
use sqlx::migrate::Migrator;
use std::sync::Arc;
use storage::create_db_pool;

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

    let rpc_client = RpcClient::new(&rpc_url).await;
    let db_pool = create_db_pool(&db_url).await;

    MIGRATOR
        .run(&db_pool)
        .await
        .expect("Failed to run database migrations");

    let indexer_engine = Arc::new(BackfillEngine {
        rpc_client,
        db_pool: db_pool.clone(),
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
                start_server(db_pool)
            )?;
        }
        IndexerMode::Live => {
            tokio::try_join!(
                Arc::clone(&indexer_engine).run_live(start_block),
                start_server(db_pool)
            )?;
        }
        IndexerMode::Hybrid => {
            tokio::try_join!(
                Arc::clone(&indexer_engine).run_backfill(start_block),
                Arc::clone(&indexer_engine).run_live(start_block),
                start_server(db_pool)
            )?;
        }
    }

    Ok(())
}
