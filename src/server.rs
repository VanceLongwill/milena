use crate::{
    codec::LinesCodec,
    consumer::{start_consumer, ConsumeArgs},
};
use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use futures::stream::StreamExt;
use log::{error, info};
use prost_reflect::DescriptorPool;
use rdkafka::ClientConfig;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    select, signal,
};
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::FramedWrite;

#[derive(Clone)]
pub struct ServerState {
    pub client_config: ClientConfig,
    pub descriptor_pool: DescriptorPool,
}

pub async fn start_server(server_state: ServerState) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(handler))
        .with_state(server_state);

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    Ok(axum::serve(listener, app).await?)
}

async fn handler(
    //Path(topic): Path<String>,
    State(state): State<ServerState>,
    Json(args): Json<ConsumeArgs>,
) -> impl IntoResponse {
    let output = tokio::io::sink();
    let mut client_config = state.client_config.clone();
    let codec = LinesCodec::from(tokio_util::codec::LinesCodec::new());
    let writer = FramedWrite::new(output, codec);
    let rdr = tokio_util::io::ReaderStream::new(output);
    let serialized = tokio_serde::SymmetricallyFramed::new(writer, SymmetricalJson::default());
    tokio::spawn(async move {
        select! {
            res = start_consumer(
                &mut client_config,
                state.descriptor_pool,
                args,
                serialized,
            ) => {
                match res {
                    Ok(()) => info!("Consumed exited successfully"),
                    Err(err) => error!("Consumer exited with an error: {err}"),
                }
            },
            shutdown = signal::ctrl_c() => {
                match shutdown {
                    Ok(()) => {
                        info!("Received shutdown signal");
                    }
                    Err(err) => {
                        error!("Failed to listen for shutdown signal: {err}");
                    }
                }
            }
        }
    });

    Body::from(output)
}