use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::{Filter, Log, BlockNumberOrTag, BlockTransactionsKind};
//use alloy::primitives::{Address};
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
    ) -> Result<Vec<Log>, Box<dyn std::error::Error + Send + Sync>> {

        let filter = Filter::new()
            .from_block(from_block)
            .to_block(to_block);

        let logs = self.provider.get_logs(&filter).await?;

        Ok(logs)
    }

    pub async fn get_latest_block_number(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.provider.get_block_number().await?)
    }

    pub async fn get_block_timestamp(
        &self,
        block_number: u64,
    ) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {

        let block = self.provider
            .get_block_by_number(
                BlockNumberOrTag::Number(block_number.into()),
                BlockTransactionsKind::Hashes,
        ).await?;

        Ok(block.map(|b| b.header.timestamp))
    }

   
}
