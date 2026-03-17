use rpc::RpcClient;
use std::sync::Arc;
use storage::create_db_pool;
use ingestion::BackfillEngine;

#[tokio::main]
async fn main() {

    dotenvy::dotenv().ok(); // Load environment variables from .env file

    // let rpc_url = std::env::var("SEPOLIA_RPC_URL").
    //     .unwrap_or_else(|_| {
    //         eprintln!("SEPOLIA_RPC_URL not set. using default local url");
    //         "http://localhost:8545".to_string();
    //     });

    let rpc_url = std::env::var("SEPOLIA_RPC_URL").expect("RPC_URL env-var not set!");

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL env-var not set!");
    
    let rpc_client = RpcClient::new(&rpc_url).await;

    let db_pool = create_db_pool(&db_url).await;

    let start_block = 10463971;
    let latest_block = rpc_client
        .get_latest_block_number()
        .await
        .expect("Failed to fetch latest block number");

    let backfill_engine = Arc::new(BackfillEngine {
        rpc_client,
        db_pool,
        contract_address: vec![],
    });

    backfill_engine.run(start_block, latest_block).await.unwrap();
}
