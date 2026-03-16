CREATE TABLE erc20_transfers (
   
    block_number BIGINT NOT NULL,
    block_hash BYTEA NOT NULL,

    txn_hash BYTEA NOT NULL,
    log_index INTEGER NOT NULL,

    contract_address BYTEA NOT NULL,
    from_address BYTEA NOT NULL,
    to_address BYTEA NOT NULL,
    value NUMERIC(78,0) NOT NULL,

    timestamp BIGINT NOT NULL,
    
    PRIMARY KEY (txn_hash, log_index)
    
) PARTITION BY RANGE (block_number);


CREATE INDEX idx_from_address 
ON erc20_transfers(from_address);

CREATE INDEX idx_to_address
ON erc20_transfers(to_address);

CREATE INDEX idx_contract_address
ON erc20_transfers(contract_address);

CREATE INDEX idx_block_number
ON erc20_transfers(block_number);

CREATE INDEX idx_wallet_from
ON erc20_transfers(from_address, block_number);

CREATE INDEX idx_wallet_to
ON erc20_transfers(to_address, block_number);

CREATE INDEX idx_contract_block
ON erc20_transfers(contract_address, block_number);