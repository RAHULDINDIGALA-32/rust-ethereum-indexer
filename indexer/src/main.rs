use rpc::RpcClient;
use alloy::primitives::address;


#[tokio::main]
async fn main() {

    let rpc_url = "https://eth-sepolia.g.alchemy.com/v2/<YOUR_ALCHEMY_API_KEY>";
    
    let client = RpcClient::new(rpc_url).await;

    let contract = address!("0x1c7d4b196cb0c7b01d743fbc6116a902379c7238"); // Sepolia USDC contract address

    let logs = client
        .fetch_logs(10457550, 10457558, contract)
        .await
        .expect("Failed to fetch logs");
        
    println!("logs fetched: {}", logs.len());

    for log in logs {
        println!("Txn: {:?}", log.transaction_hash);
        println!("Topics: {:?}", log.topics());
        println!("Data: {:?}", log.data());
        println!("-----------------------------");
    }
}