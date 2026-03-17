use alloy::primitives::{B256};
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::{Filter, Log, BlockNumberOrTag, BlockTransactionsKind};
//use alloy::primitives::{Address};
use alloy::transports::http::{Http, Client};

use std::sync::Arc;


const ERC20_TRANSFER_EVENT_SIGNATURE: B256 = B256::new([ // pre-compute to reduce runtime cost
    0xdd, 0xf2, 0x52, 0xad,
    0x1b, 0xe2, 0xc8, 0x9b,
    0x69, 0xc2, 0xb0, 0x68,
    0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1,
    0x63, 0xc4, 0xa1, 0x16,
    0x28, 0xf5, 0x5a, 0x4d,
    0xf5, 0x23, 0xb3, 0xef,
]);

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

    pub async fn fetch_erc20_transfer_logs(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, Box<dyn std::error::Error + Send + Sync>> {

        let filter = Filter::new()
            .event_signature(ERC20_TRANSFER_EVENT_SIGNATURE)
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
