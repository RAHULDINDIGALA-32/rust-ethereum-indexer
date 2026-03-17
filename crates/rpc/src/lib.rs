use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::{Filter, Log};
use alloy::primitives::{Address};
use alloy::transports::http::{Http, Client};

use std::sync::Arc;

pub struct RpcClient {
    provider: Arc<RootProvider<Http<Client>>>,
}


impl RpcClient {
    pub async fn new(rpc_url: &str) -> Self {

        let provider = ProviderBuilder::new()
            .on_http(rpc_url.parse().unwrap());

        Self {
            provider: Arc::new(provider),
        }
    }

    pub async fn fetch_logs(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, Box<dyn std::error::Error>> {

        let filter = Filter::new()
            .from_block(from_block)
            .to_block(to_block);

        let logs = self.provider.get_logs(&filter).await?;

        Ok(logs)
    }
}