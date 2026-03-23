use async_graphql::http::{GraphQLPlaygroundConfig, playground_source};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    Router,
    extract::State,
    response::{Html, IntoResponse, Redirect},
    routing::get,
};
use storage::PgPool;
use tokio::net::TcpListener;

use crate::schema::{AppSchema, create_schema};

async fn graphql_handler(State(schema): State<AppSchema>, req: GraphQLRequest) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn graphql_playground() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/graphql")))
}

async fn root_redirect() -> Redirect {
    Redirect::temporary("/graphql")
}

async fn health_handler() -> &'static str {
    "ok"
}

pub async fn start_server(db_pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bind_address = std::env::var("API_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".to_owned());
    let schema = create_schema(db_pool);

    let app = Router::new()
        .route("/", get(root_redirect))
        .route("/health", get(health_handler))
        .route("/graphql", get(graphql_playground).post(graphql_handler))
        .with_state(schema);

    let listener = TcpListener::bind(&bind_address).await?;

    println!("API server is running on http://{bind_address}/graphql");

    axum::serve(listener, app).await?;
    Ok(())
}
