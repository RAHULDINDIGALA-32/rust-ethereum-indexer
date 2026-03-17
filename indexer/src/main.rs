use alloy::primitives::address;
use rpc::RpcClient;
use decoder::decode_erc20_transfer;
use storage::create_db_pool;
use storage::models::Erc20TransferRecord;
use ingestion::BackfillEngine;

#[tokio::main]
async fn main() {

    // let rpc_url = std::env::var("SEPOLIA_RPC_URL").
    //     .unwrap_or_else(|_| {
    //         eprintln!("SEPOLIA_RPC_URL not set. using default local url");
    //         "http://localhost:8545".to_string();
    //     });

    let rpc_url = std::env::var("SEPOLIA_RPC_URL").expect("RPC_URL env-var not set!");

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL env-var not set!");
    
    let rpc_client = RpcClient::new(rpc_url).await;

    let db_pool = create_db_pool(@db_url).await;

    let backfill_engine = BackfillEngine {
        rpc_client,
        db_pool,
        contract_address: vec![],
    };

    let start_block = 10463970;
    let latest_block = rpc_client.provider.get_block_number().await.unwrap().as_u64();

    backfill_engine.run(start_block, latest_block).await.unwrap();
}