use crate::consumer::{create_consumer, start_consumer, ConsumeArgs};
use axum::{extract::State, response::IntoResponse, routing::any, Json, Router};
use axum_streams::*;
use clap::Parser;
use futures::stream::StreamExt;
use log::{error, warn};
use prost_reflect::DescriptorPool;
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug, Serialize, Deserialize)]
pub struct ServerArgs {
    /// Port to run the server on
    #[arg(short, long)]
    pub port: Option<u16>,
}

const DEFAULT_PORT: u16 = 29999;

#[derive(Clone)]
pub struct ServerState {
    pub client_config: ClientConfig,
    pub descriptor_pool: DescriptorPool,
}

pub async fn start_server(
    server_state: ServerState,
    server_args: ServerArgs,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", any(handler))
        .with_state(server_state);

    let listener = tokio::net::TcpListener::bind(format!(
        "0.0.0.0:{}",
        server_args.port.unwrap_or(DEFAULT_PORT)
    ))
    .await?;

    Ok(axum::serve(listener, app).await?)
}

#[derive(Serialize, Deserialize)]
struct ErrorResponse {
    message: String,
}

impl From<anyhow::Error> for ErrorResponse {
    fn from(value: anyhow::Error) -> Self {
        Self {
            message: value.to_string(),
        }
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> axum::response::Response {
        axum::response::Json(self).into_response()
    }
}

async fn handler(
    State(state): State<ServerState>,
    Json(args): Json<ConsumeArgs>,
) -> axum::response::Result<impl IntoResponse> {
    let client_config = state.client_config.clone();
    let consumer = create_consumer(client_config, &args.group_id, args.exit_on_last)
        .map_err(ErrorResponse::from)?;
    let stream = start_consumer(consumer, state.descriptor_pool, args)
        .await
        .map_err(ErrorResponse::from)?;

    // not all errors are fatal, log errors and keep forwarding messages until the stream is
    // exhausted
    let stream = stream.map(|res| match res {
        Ok(Some(msg)) => Some(msg),
        Ok(None) => {
            warn!("Empty message received");
            None
        }
        Err(err) => {
            error!("Failed to consume message: {err}");
            None
        }
    });

    Ok(StreamBodyAs::json_nl(stream))
}
