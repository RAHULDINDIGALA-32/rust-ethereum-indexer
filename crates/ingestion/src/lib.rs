use rpc::RpcClient;
use decoder::{Erc20TransferEvent, decode_erc20_transfer};
use stoarge::{PgPool, insert_erc20_transfer};
use storage::models::Erc20TransferRecord;

pun struct BackfillEngine {
    pub rpc_client: RpcClient,
    pub db_pool: PgPool,
    pub contract_address: Vec<u8>,
};

const BLOCK_BATCH_SIZE: u64 = 10; // Number of blocks to process in each batch (currently limited to 10 as alchemy free tier supports only 10 block range)

impl BackfillEngine {

    pub async fn run(
        &self,
        start_block: u64,
        latest_block: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        
        let mut current_block = start_block;

        while current_block <= latest_block {

            let end_block = std::cmp::min(current_block + BLOCK_BATCH_SIZE, latest_block);

            println!("Indexing Blocks: {} -> {}", current_block, end_block);

            let logs = self.rpc_client
                .fetch_logs(current_block, end_block)
                .await:;

            println!("Fetched {} logs", logs.len());

            for log in logs {

                if let Some(erc20_transfer_event) = decode_erc20_transfer(&log) {

                    let row = Erc20TransferRecord {
                        block_number: log.block_number().unwrap().as_u64() as i64,
                        block_hash: log.block_hash().unwrap().to_vec(),

                        transaction_hash: log.transaction_hash().unwrap().to_vec(),\
                        log_index: log.log_index().unwrap().as_u64() as i32,

                        contract_address: log.address().to_vec(),
                        from_address: erc20_transfer_event.from.to_vec(),
                        to_address: erc20_transfer_event.to.to_vec(),
                        value: erc20_transfer_event.value.to_string(),

                        timestamp: 0 
                    };

                insert_erc20_transfer(&self.db_pool, row).await?;

                }
            }

            current_block = end_block + 1;  
        }

        Ok(())
    }
}



