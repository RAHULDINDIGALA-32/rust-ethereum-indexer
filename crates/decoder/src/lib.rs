use alloy::primitives::{Address, B256, U256, keccak256};
use alloy::rpc::types::Log;

#[derive(Debug)]
pub struct Erc20TransferEvent {
    pub from: Address,
    pub to: Address,
    pub value: U256,
}

// pub fn transfer_event_signature() -> [u8; 32] {
//     keccak256("Transfer(address,address,uint256)").into()
// }

pub fn erc20_transfer_event_signature() -> B256 {
    keccak256("Transfer(address,address,uint256)")
}

pub fn decode_erc20_transfer(log: &Log) -> Option<Erc20TransferEvent> {
    if log.topics().len() < 3 {
        return None;
    }

    let signature = erc20_transfer_event_signature();
    if log.topics()[0] != signature {
        return None;
    }

    let from = Address::from_slice(&log.topics()[1].0[12..]);
    let to = Address::from_slice(&log.topics()[2].0[12..]);

    let value = U256::from_be_slice(&log.data().data);

    Some(Erc20TransferEvent { from, to, value })
}
