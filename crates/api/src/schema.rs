use alloy::primitives::{Address, B256};
use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Error, Object, Schema, SimpleObject,
};
use sqlx::{FromRow, Postgres, QueryBuilder};
use storage::PgPool;

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 500;

#[derive(SimpleObject)]
#[graphql(rename_fields = "camelCase")]
pub struct Erc20TransferEventRecord {
    pub block_number: i64,
    pub block_hash: String,
    pub txn_hash: String,
    pub log_index: i32,
    pub contract_address: String,
    pub from_address: String,
    pub to_address: String,
    pub value: String,
    pub timestamp: i64,
}

#[derive(SimpleObject)]
#[graphql(rename_fields = "camelCase")]
pub struct IndexerStatus {
    pub last_processed_block: i64,
    pub latest_indexed_block: Option<i64>,
    pub indexed_block_count: i64,
}

#[derive(FromRow)]
struct Erc20TransferRow {
    block_number: i64,
    block_hash: Vec<u8>,
    txn_hash: Vec<u8>,
    log_index: i32,
    contract_address: Vec<u8>,
    from_address: Vec<u8>,
    to_address: Vec<u8>,
    value: String,
    timestamp: i64,
}

pub type AppSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub struct QueryRoot;

#[Object(rename_fields = "camelCase")]
impl QueryRoot {
    async fn erc20_transfers(
        &self,
        ctx: &Context<'_>,
        from_address: Option<String>,
        to_address: Option<String>,
        contract_address: Option<String>,
        min_block: Option<i64>,
        max_block: Option<i64>,
        limit: Option<i32>,
        offset: Option<i32>,
    ) -> async_graphql::Result<Vec<Erc20TransferEventRecord>> {
        if let (Some(min_block), Some(max_block)) = (min_block, max_block) {
            if min_block > max_block {
                return Err(Error::new("minBlock cannot be greater than maxBlock"));
            }
        }

        let from_address = from_address.as_deref().map(parse_address).transpose()?;
        let to_address = to_address.as_deref().map(parse_address).transpose()?;
        let contract_address = contract_address.as_deref().map(parse_address).transpose()?;
        let limit = normalize_limit(limit)?;
        let offset = normalize_offset(offset)?;

        let pool = ctx.data::<PgPool>()?;
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "SELECT
                block_number,
                block_hash,
                txn_hash,
                log_index,
                contract_address,
                from_address,
                to_address,
                value::text AS value,
                timestamp
             FROM erc20_transfers",
        );

        let has_filters = from_address.is_some()
            || to_address.is_some()
            || contract_address.is_some()
            || min_block.is_some()
            || max_block.is_some();

        if has_filters {
            query_builder.push(" WHERE ");
            let mut filters = query_builder.separated(" AND ");

            if let Some(from_address) = from_address {
                filters.push("from_address = ").push_bind(from_address);
            }

            if let Some(to_address) = to_address {
                filters.push("to_address = ").push_bind(to_address);
            }

            if let Some(contract_address) = contract_address {
                filters
                    .push("contract_address = ")
                    .push_bind(contract_address);
            }

            if let Some(min_block) = min_block {
                filters.push("block_number >= ").push_bind(min_block);
            }

            if let Some(max_block) = max_block {
                filters.push("block_number <= ").push_bind(max_block);
            }
        }

        query_builder.push(" ORDER BY block_number DESC, log_index DESC");
        query_builder.push(" LIMIT ").push_bind(limit);
        query_builder.push(" OFFSET ").push_bind(offset);

        let records = query_builder
            .build_query_as::<Erc20TransferRow>()
            .fetch_all(pool)
            .await?;

        Ok(records.into_iter().map(Into::into).collect())
    }

    async fn indexer_status(&self, ctx: &Context<'_>) -> async_graphql::Result<IndexerStatus> {
        let pool = ctx.data::<PgPool>()?;

        let last_processed_block = sqlx::query_scalar::<_, i64>(
            "SELECT last_processed_block
             FROM indexer_checkpoint
             WHERE id = 1",
        )
        .fetch_one(pool)
        .await?;

        let latest_indexed_block = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(block_number)
             FROM indexed_blocks",
        )
        .fetch_one(pool)
        .await?;

        let indexed_block_count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM indexed_blocks")
                .fetch_one(pool)
                .await?;

        Ok(IndexerStatus {
            last_processed_block,
            latest_indexed_block,
            indexed_block_count,
        })
    }
}

impl From<Erc20TransferRow> for Erc20TransferEventRecord {
    fn from(row: Erc20TransferRow) -> Self {
        Self {
            block_number: row.block_number,
            block_hash: format_hash(&row.block_hash),
            txn_hash: format_hash(&row.txn_hash),
            log_index: row.log_index,
            contract_address: format_address(&row.contract_address),
            from_address: format_address(&row.from_address),
            to_address: format_address(&row.to_address),
            value: row.value,
            timestamp: row.timestamp,
        }
    }
}

pub fn create_schema(db_pool: PgPool) -> AppSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(db_pool)
        .finish()
}

fn parse_address(value: &str) -> Result<Vec<u8>, Error> {
    let trimmed = value.trim();
    let trimmed = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);

    if trimmed.len() != 40 || !trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(Error::new(format!(
            "Invalid Ethereum address '{value}'. Expected a 20-byte hex string."
        )));
    }

    let address = format!("0x{trimmed}")
        .parse::<Address>()
        .map_err(|_| Error::new(format!("Invalid Ethereum address '{value}'.")))?;

    Ok(address.as_slice().to_vec())
}

fn normalize_limit(limit: Option<i32>) -> Result<i64, Error> {
    match limit {
        None => Ok(DEFAULT_LIMIT),
        Some(limit) if limit <= 0 => Err(Error::new("limit must be greater than 0")),
        Some(limit) => Ok(i64::from(limit).min(MAX_LIMIT)),
    }
}

fn normalize_offset(offset: Option<i32>) -> Result<i64, Error> {
    match offset {
        None => Ok(0),
        Some(offset) if offset < 0 => Err(Error::new("offset cannot be negative")),
        Some(offset) => Ok(i64::from(offset)),
    }
}

fn format_address(bytes: &[u8]) -> String {
    format!("{:#x}", Address::from_slice(bytes))
}

fn format_hash(bytes: &[u8]) -> String {
    if bytes.len() == 32 {
        format!("{:#x}", B256::from_slice(bytes))
    } else {
        let mut encoded = String::with_capacity(bytes.len() * 2 + 2);
        encoded.push_str("0x");
        for byte in bytes {
            use std::fmt::Write;
            let _ = write!(&mut encoded, "{byte:02x}");
        }
        encoded
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_LIMIT, normalize_limit, normalize_offset, parse_address};

    #[test]
    fn parse_address_accepts_prefixed_and_unprefixed_input() {
        let prefixed = parse_address("0x1111111111111111111111111111111111111111").unwrap();
        let unprefixed = parse_address("1111111111111111111111111111111111111111").unwrap();

        assert_eq!(prefixed, unprefixed);
        assert_eq!(prefixed.len(), 20);
    }

    #[test]
    fn pagination_helpers_validate_bounds() {
        assert_eq!(normalize_limit(None).unwrap(), 50);
        assert_eq!(normalize_limit(Some(999)).unwrap(), MAX_LIMIT);
        assert!(normalize_limit(Some(0)).is_err());

        assert_eq!(normalize_offset(None).unwrap(), 0);
        assert!(normalize_offset(Some(-1)).is_err());
    }
}
