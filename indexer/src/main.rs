use ingestion::BackfillEngine;
use rpc::RpcClient;
use sqlx::migrate::Migrator;
use std::sync::Arc;
use storage::create_db_pool;

static MIGRATOR: Migrator = sqlx::migrate!("../migrations");

#[tokio::main]
async fn main() {
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

    let rpc_client = RpcClient::new(&rpc_url).await;
    let db_pool = create_db_pool(&db_url).await;

    MIGRATOR
        .run(&db_pool)
        .await
        .expect("Failed to run database migrations");

    let backfill_engine = Arc::new(BackfillEngine {
        rpc_client,
        db_pool,
        contract_address: vec![],
        confirmation_depth,
        reorg_lookback,
        live_poll_interval_secs,
    });

    backfill_engine.run(start_block).await.unwrap();
}
