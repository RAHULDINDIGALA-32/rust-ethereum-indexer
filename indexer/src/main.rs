use alloy::primitives::address;
use rpc::RpcClient;
use decoder::decode_erc20_transfer;
use storage::models::Erc20TransferRecord;

#[tokio::main]
async fn main() {

    let rpc_url = "https://eth-sepolia.g.alchemy.com/v2/Z9YPZSpaYutwn9JJeJBjB";
    
    let client = RpcClient::new(rpc_url).await;

    let contract = address!("0x1c7d4b196cb0c7b01d743fbc6116a902379c7238"); // Sepolia USDC contract address

    let logs = client
        .fetch_logs(10457555, 10457558, contract)
        .await
        .expect("Failed to fetch logs");
        
    println!("logs fetched: {}", logs.len());

    for log in logs {

        if let Some(event) = decode_erc20_transfer(&log) {
            println!("ERC20 TRANSFER EVENT");
            println!("From: {:?}", event.from);
            println!("To: {:?}", event.to);
            println!("Value: {:?}", event.value);
             println!("-----------------------------");
        }
       
    }

    let erc20_transfer_record = Erc20TransferRecord {
        block_number: log.block_number.unwrap().as_u64() as i64,
        block_hash: log.block_hash.unwrap().0.to_vec(),
        txn_hash: log.transaction_hash.unwrap().0.to_vec(),
        log_index: log.log_index.as_u64() as i32,
        contract_address: log.address.0.to_vec(),
        from_address: event.from.0.to_vec(),
        to_address: event.to.0.to_vec(),
        value: event.value.to_string(),
        timestamp: 0, 
    };
}