CREATE TABLE indexer_checkpoint (
    id INTEGER PRIMARY KEY,
    last_processed_block BIGINT NOT NULL
);

INSERT INTO indexer_checkpoint (id, last_processed_block)
VALUES (1, 0)
ON CONFLICT DO NOTHING;