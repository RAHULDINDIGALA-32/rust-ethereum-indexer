use serde::{Serialize, Deserialize};
use bigdecimal::BigDecimal;

#[derive(Debug, Serialize, Deserialize)]
pub struct Erc20TransferRecord {

    pub block_number: i64,
    pub block_hash: Vec<u8>,

    pub txn_hash: Vec<u8>,
    pub log_index: i32,

    pub contract_address: Vec<u8>,
    pub from_address: Vec<u8>,
    pub to_address: Vec<u8>,
    pub value: BigDecimal,

    pub timestamp: i64,
}