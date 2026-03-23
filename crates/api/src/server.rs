use axum::{routing::get, Router};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use sqlx::PgPool;

use crate::schema::{AppSchema, cretae_schema};


async fn graphql_handler(
    schema: axum::extract::Extension<AppSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {

    schema.execurte(req.into_inner().await.into())
}

pub async fn start_server(db_pool: PgPool) {

    let schema = cretae_schema(db_pool);

    let app = Router::new()
        .route("/", get(|| async { "API Server Running!"}))
        .router("/graphql", get(graphql_handler).post(graphql_handler))
        .layer(axum::Extension);

    println!("API server is running on http://localhost:8000/graphql");

    axum::Server::bind(&"0.0.0.0:8000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}