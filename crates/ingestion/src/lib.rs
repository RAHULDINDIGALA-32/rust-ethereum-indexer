use bigdecimal::BigDecimal;
use std::str::FromStr;
use rpc::RpcClient;
use decoder::{decode_erc20_transfer};
use storage::{Erc20TransferRecord, PgPool, insert_batch_erc20_transfers};

pub struct BackfillEngine {
    pub rpc_client: RpcClient,
    pub db_pool: PgPool,
    pub contract_address: Vec<u8>,
}

const BLOCK_BATCH_SIZE: u64 = 9; // Number of blocks to process in each batch (currently limited to 10 as alchemy free tier supports only 10 block range)
const RECORD_BATCH_SIZE: usize = 100; // Number of records to insert in each batch

impl BackfillEngine {

    pub async fn run(
        &self,
        start_block: u64,
        latest_block: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        
        let mut current_block = start_block;

        while current_block <= latest_block {

            let end_block = std::cmp::min(current_block + BLOCK_BATCH_SIZE, latest_block);

            let mut records_batch : Vec<Erc20TransferRecord> = Vec::new();

            println!("Indexing Blocks: {} -> {}", current_block, end_block);

            let logs = self.rpc_client
                .fetch_logs(current_block, end_block)
                .await?;

            println!("Fetched {} logs", logs.len());

            for log in logs {

                let log_block_timestamp =  self.rpc_client.get_block_timestamp(log.block_number.unwrap() as u64).await?;

                if let Some(erc20_transfer_event) = decode_erc20_transfer(&log) {

                    let record = Erc20TransferRecord {
                        block_number: log.block_number.unwrap() as i64,
                        block_hash: log.block_hash.unwrap().to_vec(),

                        txn_hash: log.transaction_hash.unwrap().to_vec(),
                        log_index: log.log_index.unwrap() as i32,

                        contract_address: log.address().to_vec(),
                        from_address: erc20_transfer_event.from.to_vec(),
                        to_address: erc20_transfer_event.to.to_vec(),
                        value: BigDecimal::from_str(&erc20_transfer_event.value.to_string()).unwrap(),

                        timestamp: log_block_timestamp.unwrap() as i64,
                    };

                   // insert_one_erc20_transfer(&self.db_pool, &record).await?;

                   records_batch.push(record);

                   if records_batch.len()  >= RECORD_BATCH_SIZE {
                    insert_batch_erc20_transfers(&self.db_pool, &records_batch).await?;
                    records_batch.clear();
                   }

                }
            }

            if !records_batch.is_empty() {
                insert_batch_erc20_transfers(&self.db_pool, &records_batch).await?;
                records_batch.clear();
            }

            current_block = end_block + 1;  
        }

        Ok(())
    }
}

