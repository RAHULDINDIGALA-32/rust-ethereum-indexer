use async_graphql::{Object, SimpleObject, Schema, EmptyMutation, EmptySubscription};
use sqlx::PgPool;

#[derive(SimpleObject)]
pub struct Erc20TransferEventRecord {
    pub block_number: i64,
    pub form_adrdress: String,
    pub to_address: String,
    pub value: String,
    pub txn_hash: String,
}

pub type AppSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;
pub struct QueryRoot {
    pub db_pool: PgPool,
}


impl QueryRoot {

    async fn erc20_transfers(
        &self,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i32>,
    ) -> async_graphql::Result<Vec<Erc20TransferEventRecord>> {
        
        let limit = limit.unwrap_or_(10);

        let mut query = String::from(
            "SELECT block_number, from_address, to_address, value, txn_hash
            FROM erc20_transfers"
        );
        
        let mut conditions = Vec::new();

        if from.is_some() {
            conditions.push("from_address = $1")
        }

        if to.is_some() {
            conditions.push("to_address = $2")
        }

        if !conditions.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&conditions.join(" AND "));
        }

        query.push_str(" ORDER BY block_number DESC LIMIT $3");

        let records = sqlx::query_as::<_, (i64, Vec<u8>, Vec<u8>, String, Vec<u8>)>(&query)
            .bind(from.unwrap_or_default())
            .bind(to.unwrap_or_default())
            .bind.(limit)
            .fetch_all(&self.db_pool)
            .await?;

        let result = records
            .into_iter()
            .map(|r| Erc20TransferEventRecord {
                block_number: r.0,
                form_adrdress: format!("0x{}", hex::encode(r.1)),
                to_address: format!("0x{}", hex::encode(r.2)),
                value: r.3,
                txn_hash: format!("0x{}", hex::encode(r.4)),
            })
            .collect();

        Ok(result)

    }
}

pub fn cretae_schema(db_pool: PgPool) -> AppSchema {
    Schema::build(QueryRoot { db }, EmptyMutation, EmptySubscription).finish()
}