use alloy::primitives::{Address, B256};
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::{BlockNumberOrTag, BlockTransactionsKind, Filter, Log};
use alloy::transports::http::{Client, Http};
use observability::IndexerMetrics;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{error, warn};

const ERC20_TRANSFER_EVENT_SIGNATURE: B256 = B256::new([
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
]);

#[derive(Clone, Debug)]
pub struct BlockMetadata {
    pub number: u64,
    pub hash: Vec<u8>,
    pub timestamp: u64,
}

pub struct RpcClient {
    provider: Arc<RootProvider<Http<Client>>>,
    metrics: Arc<IndexerMetrics>,
    max_retries: usize,
    retry_backoff: Duration,
}

impl RpcClient {
    pub fn new(
        rpc_url: &str,
        metrics: Arc<IndexerMetrics>,
        max_retries: usize,
        retry_backoff: Duration,
    ) -> Self {
        let provider = ProviderBuilder::new().on_http(rpc_url.parse().unwrap());

        Self {
            provider: Arc::new(provider),
            metrics,
            max_retries,
            retry_backoff,
        }
    }

    pub async fn fetch_erc20_transfer_logs(
        &self,
        from_block: u64,
        to_block: u64,
        contract_address: Option<&[u8]>,
    ) -> Result<Vec<Log>, Box<dyn std::error::Error + Send + Sync>> {
        let mut filter = Filter::new()
            .event_signature(ERC20_TRANSFER_EVENT_SIGNATURE)
            .from_block(from_block)
            .to_block(to_block);

        if let Some(contract_address) = contract_address {
            filter = filter.address(Address::from_slice(contract_address));
        }

        self.with_retry("eth_getLogs", || {
            let filter = filter.clone();
            async move { self.provider.get_logs(&filter).await.map_err(Into::into) }
        })
        .await
    }

    pub async fn get_latest_block_number(
        &self,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        self.with_retry("eth_blockNumber", || async move {
            self.provider.get_block_number().await.map_err(Into::into)
        })
        .await
    }

    pub async fn get_block_metadata(
        &self,
        block_number: u64,
    ) -> Result<Option<BlockMetadata>, Box<dyn std::error::Error + Send + Sync>> {
        let block = self
            .with_retry("eth_getBlockByNumber", || async move {
                self.provider
                    .get_block_by_number(
                        BlockNumberOrTag::Number(block_number.into()),
                        BlockTransactionsKind::Hashes,
                    )
                    .await
                    .map_err(Into::into)
            })
            .await?;

        Ok(block.map(|block| BlockMetadata {
            number: block_number,
            hash: block.header.hash.to_vec(),
            timestamp: block.header.timestamp,
        }))
    }

    async fn with_retry<T, F, Fut>(
        &self,
        method: &'static str,
        mut operation: F,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>,
    {
        let mut attempt = 0usize;

        loop {
            attempt += 1;
            let started_at = Instant::now();

            match operation().await {
                Ok(result) => {
                    self.metrics
                        .record_rpc_result(method, started_at.elapsed(), true);
                    return Ok(result);
                }
                Err(error) => {
                    let elapsed = started_at.elapsed();
                    self.metrics.record_rpc_result(method, elapsed, false);

                    if attempt > self.max_retries {
                        error!(
                            method,
                            attempt,
                            max_retries = self.max_retries,
                            duration_ms = elapsed.as_millis() as u64,
                            error = %error,
                            "rpc request failed after retries"
                        );
                        return Err(error);
                    }

                    let backoff = self.retry_backoff.saturating_mul(attempt as u32);
                    self.metrics.record_rpc_retry(method, attempt, backoff);
                    warn!(
                        method,
                        attempt,
                        max_retries = self.max_retries,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %error,
                        "rpc request failed; backing off before retry"
                    );
                    sleep(backoff).await;
                }
            }
        }
    }
}
