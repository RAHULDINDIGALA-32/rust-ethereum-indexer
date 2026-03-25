use alloy::primitives::{Address, B256, U256};
//use alloy::primitives::{keccak256};
use alloy::rpc::types::Log;

#[derive(Debug)]
pub struct Erc20TransferEvent {
    pub from: Address,
    pub to: Address,
    pub value: U256,
}

// pub fn erc20_transfer_event_signature() -> B256 {
//     keccak256("Transfer(address,address,uint256)")
// }
const ERC20_TRANSFER_EVENT_SIGNATURE: B256 = B256::new([
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
]);

pub fn decode_erc20_transfer(log: &Log) -> Option<Erc20TransferEvent> {
    if log.topics().len() != 3 {
        return None;
    }

    if log.data().data.len() != 32 {
        return None;
    }

    //let signature = erc20_transfer_event_signature();
    let signature = ERC20_TRANSFER_EVENT_SIGNATURE;
    if log.topics()[0] != signature {
        return None;
    }

    let from = Address::from_slice(&log.topics()[1].0[12..]);
    let to = Address::from_slice(&log.topics()[2].0[12..]);
    let value = U256::from_be_slice(&log.data().data);

    Some(Erc20TransferEvent { from, to, value })
}
