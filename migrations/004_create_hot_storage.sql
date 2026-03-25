CREATE TABLE hot_erc20_transfers (
    block_number BIGINT NOT NULL,
    block_hash BYTEA NOT NULL,

    txn_hash BYTEA NOT NULL,
    log_index INTEGER NOT NULL,

    contract_address BYTEA NOT NULL,
    from_address BYTEA NOT NULL,
    to_address BYTEA NOT NULL,
    value NUMERIC(78,0) NOT NULL,

    timestamp BIGINT NOT NULL,

    PRIMARY KEY (txn_hash, log_index, block_number)
) PARTITION BY RANGE (block_number);

CREATE INDEX idx_hot_from_address
ON hot_erc20_transfers(from_address);

CREATE INDEX idx_hot_to_address
ON hot_erc20_transfers(to_address);

CREATE INDEX idx_hot_contract_address
ON hot_erc20_transfers(contract_address);

CREATE INDEX idx_hot_block_number
ON hot_erc20_transfers(block_number);

CREATE INDEX idx_hot_wallet_from
ON hot_erc20_transfers(from_address, block_number);

CREATE INDEX idx_hot_wallet_to
ON hot_erc20_transfers(to_address, block_number);

CREATE INDEX idx_hot_contract_block
ON hot_erc20_transfers(contract_address, block_number);

CREATE TABLE hot_indexed_blocks (
    block_number BIGINT PRIMARY KEY,
    block_hash BYTEA NOT NULL
);
