use bytes::Bytes;
use clap::{Parser, Subcommand};
use log::{error, info};
use milena::decoder::{DecodeArgs, ProtoDecoder};
use milena::encoder::{EncodeArgs, ProtoEncoder};
use prost_reflect::DescriptorPool;
use rdkafka::config::ClientConfig;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::{read, read_to_string};
use tokio::signal;

use milena::consumer::{start_consumer, ConsumeArgs};
use milena::producer::{ProduceArgs, ProtoProducer};

#[derive(Debug, Default)]
struct KafkaConfig(HashMap<String, String>);

impl From<KafkaConfig> for ClientConfig {
    fn from(value: KafkaConfig) -> Self {
        let mut client_config = &mut ClientConfig::new();
        for (k, v) in value.0 {
            client_config = client_config.set(k, v);
        }
        client_config.to_owned()
    }
}

impl KafkaConfig {
    fn parse_option(s: impl AsRef<str>) -> anyhow::Result<(String, String)> {
        let mut parts = s.as_ref().splitn(2, '=');
        match (parts.next(), parts.next()) {
            (Some(k), Some(v)) => Ok((k.to_string(), v.to_string())),
            _ => Err(anyhow::anyhow!("failed to parse config")),
        }
    }
    async fn from_file(filename: PathBuf) -> anyhow::Result<KafkaConfig> {
        let mut config = HashMap::new();
        for line in read_to_string(filename).await?.lines() {
            let (k, v) = Self::parse_option(line)?;
            config.insert(k, v);
        }
        Ok(KafkaConfig(config))
    }
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Consume & decode protobuf messages to JSON
    Consume(ConsumeArgs),
    /// Encode JSON messages as protobuf and produce them to a topic
    Produce(ProduceArgs),
    /// Decode an arbitrary protobuf message
    Decode(DecodeArgs),
    /// Encode an arbitrary protobuf message
    Encode(EncodeArgs),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Sets a custom config file
    #[arg(short = 'F', long, value_name = "FILE", global = true)]
    config: Option<PathBuf>,

    /// The path to the protobuf file descriptors
    #[arg(short, long, global = true, default_value = "./descriptors.binpb")]
    file_descriptors: PathBuf,

    /// A catchall for specifying additional librdkafka options
    ///
    #[arg(short = 'X', long, global = true)]
    rdkafka_options: Option<Vec<String>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    let mut config = if let Some(config) = cli.config {
        KafkaConfig::from_file(config).await?
    } else {
        KafkaConfig::default()
    };

    if let Some(opts) = cli.rdkafka_options {
        for raw_option in opts {
            let (k, v) = KafkaConfig::parse_option(raw_option)?;
            config.0.insert(k, v);
        }
    }
    let client_config = ClientConfig::from(config);

    let mut descriptor_pool = DescriptorPool::new();
    let b = Bytes::from(read(cli.file_descriptors).await?);
    descriptor_pool.decode_file_descriptor_set(b)?;

    match cli.command {
        Command::Consume(args) => {
            tokio::spawn(async move {
                info!("Starting consumer");
                start_consumer(&mut client_config.clone(), descriptor_pool, args).await
            });

            match signal::ctrl_c().await {
                Ok(()) => {
                    info!("Received shutdown signal");
                }
                Err(err) => {
                    error!("Failed to listen for shutdown signal: {err}");
                }
            }
        }
        Command::Produce(args) => {
            let producer = ProtoProducer::new(client_config, descriptor_pool);
            producer.produce(args).await?;
        }
        Command::Decode(args) => {
            let output = tokio::io::stdout();
            let mut decoder = ProtoDecoder::new(descriptor_pool, output);
            decoder.decode(args).await?;
        }
        Command::Encode(args) => {
            let output = tokio::io::stdout();
            let mut encoder = ProtoEncoder::new(descriptor_pool, output);
            encoder.encode(args).await?;
        }
    }

    Ok(())
}
