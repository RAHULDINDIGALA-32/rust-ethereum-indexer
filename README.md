<div align="center">

# Rust Ethereum Indexer

</div>

Ethereum event indexer built in Rust with reorg-safe ingestion, partitioned PostgreSQL storage, GraphQL querying, and hybrid backfill + live indexing over hot/cold lanes.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85+-000000)](https://www.rust-lang.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Storage-336791)](https://www.postgresql.org/)
[![GraphQL](https://img.shields.io/badge/GraphQL-API-E10098)](https://graphql.org/)
[![Network](https://img.shields.io/badge/Network-Ethereum-orange)](https://ethereum.org/)

## Overview

Rust Ethereum Indexer is a modular blockchain data pipeline for indexing `ERC-20 Transfer` events from Ethereum-compatible networks into PostgreSQL. The project is designed around real indexer concerns rather than a toy one-shot script: chain reorgs, hot vs finalized data, transactional writes, recent-block live indexing, resumable checkpoints, and a query layer for consumers.

The repository is best described as a production-oriented indexer foundation. The architecture is serious, the storage model is deliberate, and the ingestion path is reorg-aware and crash-conscious, while still leaving room for future upgrades such as websocket head tracking, observability, benchmarking, and distributed workers.

## What This Project Demonstrates

- Modular Rust system design across `rpc`, `decoder`, `ingestion`, `storage`, `api`, and binary runtime crates.
- Reorg-safe blockchain indexing using stored block hashes and rollback from the first invalid block.
- Hybrid indexing strategy with historical backfill into finalized storage and near-tip live indexing into hot storage.
- Atomic transaction-per-range writes to eliminate partial-write crash windows across transfers, indexed block metadata, and checkpoints.
- Partitioned PostgreSQL storage for scalable block-range writes and range-pruned reads.
- GraphQL querying over both finalized and hot near-real-time data.

## Core Features

### Indexing Engine

| Feature | Description |
|---|---|
| Historical backfill | Indexes from a configurable `START_BLOCK` and resumes from persisted checkpoints. |
| Live indexing mode | Tracks recent blocks near the latest head and stores them in hot storage for low-latency UI access. |
| Hybrid mode | Runs backfill and live workers in parallel so historical sync and near-tip freshness happen together. |
| Contract filtering | Supports indexing all ERC-20 transfers or restricting logs to a configured `CONTRACT_ADDRESS`. |
| Batched processing | Processes blocks in bounded ranges for predictable RPC and database behavior. |

### Correctness And Reorg Safety

| Feature | Description |
|---|---|
| Confirmation-depth finalization | Only promotes blocks older than `latest - CONFIRMATION_DEPTH` into cold finalized storage. |
| Hot/cold architecture | Recent reorg-prone blocks live in hot storage; matured blocks are promoted automatically into cold storage. |
| Block hash revalidation | Compares stored hashes against current chain state to detect reorgs deterministically. |
| Rollback support | Removes invalid data from the first bad block onward and rebuilds from canonical chain state. |
| Atomic range commits | Writes transfers, indexed block metadata, and checkpoint updates in a single transaction per range. |

### Storage And Query Layer

| Feature | Description |
|---|---|
| Partitioned transfer tables | `erc20_transfers` and `hot_erc20_transfers` are range-partitioned by `block_number`. |
| Indexed block tracking | Stores canonical block hashes in `indexed_blocks` and `hot_indexed_blocks`. |
| Dedicated checkpoints | Uses separate finalized and live checkpoints for independent backfill/live progress tracking. |
| GraphQL API | Exposes transfer queries and indexer status over finalized + hot data. |
| Health endpoint | Provides a simple `/health` endpoint for service monitoring. |

## Architecture

### High-Level Flow

```text
+------------------------------+
|      Ethereum RPC Node       |
|  Blocks | Logs | Metadata    |
+--------------+---------------+
               |
+--------------v---------------+
|         RPC Crate            |
| Fetch logs and block data    |
+--------------+---------------+
               |
+--------------v---------------+
|       Decoder Crate          |
| Decode ERC-20 Transfer       |
+--------------+---------------+
               |
      +--------+--------+
      |                 |
+-----v-----+   +-------v-------+
| Backfill  |   | Live Worker   |
| Worker    |   | Recent hot    |
| Finalized |   | ranges        |
+-----+-----+   +-------+-------+
      |                 |
      +--------+--------+
               |
+--------------v---------------+
|      Storage Crate           |
| Tx commits and rollback      |
+--------------+---------------+
               |
      +--------+-------------------+
      |                             |
+-----v------+-----+       +----------v----------+
| Cold Storage     |       | Hot Storage         |
| erc20_transfers  |       | hot_erc20_transfers |
| indexed_blocks   |       | hot_indexed_ blocks |
+-----+------------+       +------+--------------+
   
      |                             |
      +--------+--------------------+
                      |
      +--------------v---------------+
      |         API Crate            |
      | GraphQL and health routes    |
      +------------------------------+
```

### Storage Model

The project uses two data lanes:

- `cold` lane: finalized data in `erc20_transfers` and `indexed_blocks`
- `hot` lane: near-tip, reorg-prone data in `hot_erc20_transfers` and `hot_indexed_blocks`

Cold data is the long-lived canonical store. Hot data exists to give applications fresh near-tip state without forcing the entire system to trust the absolute latest head immediately.

### Reorg Strategy

The indexer stores a hash for every indexed block and revalidates recent history against the chain:

1. Fetch the current block metadata from RPC.
2. Compare on-chain block hash with stored block hash.
3. If mismatch, treat it as a reorg.
4. Roll back from the first bad block onward.
5. Re-index the canonical chain.

Hot and cold lanes are handled differently:

- Hot reorgs roll back only hot storage.
- Finalized reorgs roll back both hot and cold storage from the mismatch point.

## Indexing Modes

The runtime supports three modes:

| Mode | Description |
|---|---|
| `backfill` | Only runs the finalized historical worker. |
| `live` | Only runs the recent live worker and stores data in hot storage. |
| `hybrid` | Runs both workers in parallel. Recommended for UI-facing systems. |

### What "Live Indexing" Means Here

Live indexing in this project does not mean only subscribing from the exact latest block onward. Instead, it uses a recent replay window:

- start from `latest - LIVE_START_DEPTH + 1`
- index forward into hot storage
- keep polling and following the head

This is a practical industry-style approach because it:

- tolerates process restarts
- covers missed polls
- handles shallow reorgs better
- works even when indexing speed is faster than block production

## GraphQL API

The API server exposes:

- `GET /graphql` for GraphQL Playground
- `POST /graphql` for GraphQL queries
- `GET /health` for health checks

### Available Queries

#### `erc20Transfers`

Supports:

- `fromAddress`
- `toAddress`
- `contractAddress`
- `minBlock`
- `maxBlock`
- `limit`
- `offset`

The query reads from both hot and cold storage so clients can see finalized and near-real-time data in one endpoint.

#### `indexerStatus`

Returns:

- `lastProcessedBlock`
- `latestFinalizedBlock`
- `latestHotBlock`
- `latestAvailableBlock`
- `finalizedBlockCount`
- `hotBlockCount`

### Example Query

```graphql
query Example {
  indexerStatus {
    lastProcessedBlock
    latestFinalizedBlock
    latestHotBlock
    latestAvailableBlock
    finalizedBlockCount
    hotBlockCount
  }

  erc20Transfers(limit: 5) {
    blockNumber
    txnHash
    fromAddress
    toAddress
    value
    timestamp
  }
}
```

## Tech Stack

### Core

- Rust
- Tokio
- Alloy
- SQLx
- PostgreSQL
- Async-GraphQL
- Axum

### Data And Runtime

- Range-partitioned PostgreSQL tables
- Hybrid backfill/live workers
- Transactional ingestion
- GraphQL query service

## Repository Structure

```text
.
|-- crates/
|   |-- api/          # GraphQL schema and Axum server
|   |-- decoder/      # ERC-20 Transfer log decoder
|   |-- ingestion/    # Backfill + live indexing orchestration
|   |-- rpc/          # Ethereum RPC client wrapper
|   `-- storage/      # PostgreSQL persistence, partitions, checkpoints, rollback
|-- indexer/          # Main binary runtime
|-- migrations/       # PostgreSQL schema migrations
|-- README.md
`-- LICENSE
```

## Database Schema

Primary tables:

- `erc20_transfers`: finalized transfer data
- `indexed_blocks`: finalized block hash metadata
- `indexer_checkpoint`: finalized worker checkpoint
- `hot_erc20_transfers`: near-tip hot transfer data
- `hot_indexed_blocks`: near-tip hot block hash metadata
- `live_indexer_checkpoint`: live worker checkpoint

Both transfer tables are partitioned by `block_number` for efficient range operations.

## Getting Started

### Prerequisites

- Rust `1.85+`
- PostgreSQL
- An Ethereum RPC endpoint
- Git

### 1. Clone the repository

```bash
git clone https://github.com/RAHULDINDIGALA-32/rust-ethereum-indexer.git
cd rust-ethereum-indexer
```

### 2. Configure environment variables

Create a `.env` file in the project root.

```env
SEPOLIA_RPC_URL=https://eth-sepolia.g.alchemy.com/v2/your-key
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/indexer

START_BLOCK=10463971
INDEXER_MODE=hybrid
CONFIRMATION_DEPTH=12
REORG_LOOKBACK=24
LIVE_START_DEPTH=256
LIVE_POLL_INTERVAL_SECS=5
HOT_WINDOW_SIZE=64

API_BIND_ADDR=0.0.0.0:8000

# Optional: restrict to one ERC-20 contract
CONTRACT_ADDRESS=0x0000000000000000000000000000000000000000
```

### 3. Run the indexer and API

```bash
cargo run -p indexer
```

This starts:

- the indexer runtime
- database migrations
- the GraphQL API server

Then open:

- `http://localhost:8000/graphql`
- `http://localhost:8000/health`

## Useful Commands

```bash
# Build workspace
cargo build

# Run the main binary
cargo run -p indexer

# Run checks
cargo check
cargo test

# Format code
cargo fmt
```

## Configuration Reference

| Variable | Description | Default |
|---|---|---|
| `SEPOLIA_RPC_URL` | Ethereum RPC endpoint | required |
| `DATABASE_URL` | PostgreSQL connection string | required |
| `START_BLOCK` | Historical indexing start block | `10463971` |
| `INDEXER_MODE` | `backfill`, `live`, or `hybrid` | `hybrid` |
| `CONFIRMATION_DEPTH` | Blocks treated as unfinalized near the tip | `12` |
| `REORG_LOOKBACK` | Recent finalized/history revalidation depth | `24` |
| `LIVE_START_DEPTH` | Recent replay depth for live worker bootstrap | `256` |
| `LIVE_POLL_INTERVAL_SECS` | Poll interval for workers | `5` |
| `API_BIND_ADDR` | GraphQL server bind address | `0.0.0.0:8000` |
| `CONTRACT_ADDRESS` | Optional ERC-20 contract filter | unset |

## Design Decisions

### Why hot/cold storage?

- Hot storage gives applications low-latency access to recent data.
- Cold storage provides a safer finalized history.
- Promotion from hot to cold happens automatically as blocks mature past the confirmation threshold.

### Why transaction-per-range commits?

Without atomic range commits, the indexer can crash after writing transfers but before updating block metadata or checkpoints. This project commits each indexed range as one database transaction to avoid partial-write inconsistencies.

### Why partition by block number?

- Writes naturally follow block order.
- Reads with block predicates benefit from partition pruning.
- Rollback and range maintenance stay manageable.

## Current Strengths

- Clean modular crate boundaries.
- Reorg-aware indexing logic with deterministic rollback.
- Hybrid backfill + live indexing model.
- Hot/cold storage separation for correctness and freshness.
- Transactional ingestion path.
- GraphQL API for downstream consumers.

## Current Gaps Before Full Production Hardening

- No websocket head subscription yet; live mode currently polls.
- Limited automated test coverage for complex reorg/live edge cases.
- No metrics, tracing, dashboards, or alerting yet.
- No benchmark numbers for RPC throughput or DB write throughput.
- No horizontal scaling or distributed worker coordination story yet.

## Contributing

Contributions are welcome. Good areas to extend include:

- websocket-based live head tracking
- broader event support beyond ERC-20 transfers
- richer GraphQL queries and pagination
- observability and structured tracing
- performance tuning and benchmarks
- reorg/live integration tests

## License

This project is licensed under the **MIT License**. See [LICENSE](./LICENSE).

---
## Developer

Built with ❤️ by Rahul Dindigala

GitHub: https://github.com/RAHULDINDIGALA-32 

Mail: rahul.dindigala.dev@gmail.com
