use bigdecimal::BigDecimal;
use decoder::decode_erc20_transfer;
use futures::future::join_all;
use std::sync::Arc;
use rpc::RpcClient;
use std::str::FromStr;
use storage::{Erc20TransferRecord, PgPool, insert_batch_erc20_transfers};
//use tokio::task;
use tokio;

pub struct BackfillEngine {
    pub rpc_client: RpcClient,
    pub db_pool: PgPool,
    pub contract_address: Vec<u8>,
}

const BLOCK_BATCH_SIZE: u64 = 9; // Number of blocks to process in each batch (currently limited to 10 as alchemy free tier supports only 10 block range)
const RECORD_BATCH_SIZE: usize = 100; // Number of records to insert in each batch

impl BackfillEngine {
    pub async fn run(
        self: Arc<Self>,
        start_block: u64,
        latest_block: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // let mut current_block = start_block;

        // while current_block <= latest_block {

        //     let end_block = std::cmp::min(current_block + BLOCK_BATCH_SIZE, latest_block);

        //     let mut records_batch : Vec<Erc20TransferRecord> = Vec::new();

        //     println!("Indexing Blocks: {} -> {}", current_block, end_block);

        //     let logs = self.rpc_client
        //         .fetch_logs(current_block, end_block)
        //         .await?;

        //     println!("Fetched {} logs", logs.len());

        //     for log in logs {

        //         let log_block_timestamp =  self.rpc_client.get_block_timestamp(log.block_number.unwrap() as u64).await?;

        //         if let Some(erc20_transfer_event) = decode_erc20_transfer(&log) {

        //             let record = Erc20TransferRecord {
        //                 block_number: log.block_number.unwrap() as i64,
        //                 block_hash: log.block_hash.unwrap().to_vec(),

        //                 txn_hash: log.transaction_hash.unwrap().to_vec(),
        //                 log_index: log.log_index.unwrap() as i32,

        //                 contract_address: log.address().to_vec(),
        //                 from_address: erc20_transfer_event.from.to_vec(),
        //                 to_address: erc20_transfer_event.to.to_vec(),
        //                 value: BigDecimal::from_str(&erc20_transfer_event.value.to_string()).unwrap(),

        //                 timestamp: log_block_timestamp.unwrap() as i64,
        //             };

        //            // insert_one_erc20_transfer(&self.db_pool, &record).await?;

        //            records_batch.push(record);

        //            if records_batch.len()  >= RECORD_BATCH_SIZE {
        //             insert_batch_erc20_transfers(&self.db_pool, &records_batch).await?;
        //             records_batch.clear();
        //            }

        //         }
        //     }

        //     if !records_batch.is_empty() {
        //         insert_batch_erc20_transfers(&self.db_pool, &records_batch).await?;
        //         records_batch.clear();
        //     }

        //     current_block = end_block + 1;
        // }


        let semaphore = Arc::new(tokio::sync::Semaphore::new(5)); // Limit concurrent tasks

       let tasks: Vec<_> = (start_block..=latest_block)
            .step_by(BLOCK_BATCH_SIZE as usize)
            .map(|start| {
                let end = std::cmp::min(start + BLOCK_BATCH_SIZE, latest_block);
                let backfill_engine = Arc::clone(&self);

                tokio::spawn({
                    let value = semaphore.clone();
                    async move { 
                    let _permit = value.clone().acquire_owned().await.unwrap();
                    backfill_engine.process_range(start, end).await }
                })
            })
            .collect();

        join_all(tasks).await;

        Ok(())
    }

    pub async fn process_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let logs = self.rpc_client.fetch_logs(start_block, end_block).await?;

        let mut records_batch: Vec<Erc20TransferRecord> = Vec::new();

        for log in logs {
            let log_block_timestamp = self
                .rpc_client
                .get_block_timestamp(log.block_number.unwrap() as u64)
                .await?;

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

                if records_batch.len() >= RECORD_BATCH_SIZE {
                    insert_batch_erc20_transfers(&self.db_pool, &records_batch).await?;
                    records_batch.clear();
                }
            }
        }

        if !records_batch.is_empty() {
            insert_batch_erc20_transfers(&self.db_pool, &records_batch).await?;
            records_batch.clear();
        }

        Ok(())
    }
}
