use async_graphql::http::{GraphQLPlaygroundConfig, playground_source};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    Router,
    extract::State,
    http::header,
    response::{Html, IntoResponse, Redirect},
    routing::get,
};
use metrics_exporter_prometheus::PrometheusHandle;
use storage::PgPool;
use tokio::net::TcpListener;
use tracing::info;

use crate::schema::{AppSchema, create_schema};

#[derive(Clone)]
struct AppState {
    schema: AppSchema,
    prometheus_handle: PrometheusHandle,
}

async fn graphql_handler(State(state): State<AppState>, req: GraphQLRequest) -> GraphQLResponse {
    state.schema.execute(req.into_inner()).await.into()
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

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.prometheus_handle.render(),
    )
}

pub async fn start_server(
    db_pool: PgPool,
    prometheus_handle: PrometheusHandle,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bind_address = std::env::var("API_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".to_owned());
    let state = AppState {
        schema: create_schema(db_pool),
        prometheus_handle,
    };

    let app = Router::new()
        .route("/", get(root_redirect))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/graphql", get(graphql_playground).post(graphql_handler))
        .with_state(state);

    let listener = TcpListener::bind(&bind_address).await?;

    info!(
        graphql_url = format!("http://{bind_address}/graphql"),
        metrics_url = format!("http://{bind_address}/metrics"),
        "api server listening"
    );

    axum::serve(listener, app).await?;
    Ok(())
}
