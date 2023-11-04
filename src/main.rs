use bytes::Bytes;
use clap::{Parser, ValueEnum};
use log::{error, info, warn};
use prost_reflect::DescriptorPool;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::topic_partition_list::TopicPartitionList;
use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::{read, read_to_string};
use tokio::signal;
use uuid::Uuid;

use milena::dumper::MessageDumper;

pub struct KafkaConfig(HashMap<String, String>);

impl KafkaConfig {
    fn parse_option(s: &str) -> anyhow::Result<(String, String)> {
        let mut parts = s.splitn(2, '=');
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

fn home_dir() -> String {
    std::env::var("HOME").unwrap_or_default()
}

fn default_config_path() -> PathBuf {
    PathBuf::from(format!("{}/.config/kafka.config", home_dir()).to_string())
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the topic to consume
    #[arg(short, long)]
    topic: String,

    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE", default_value = default_config_path().into_os_string())]
    config: PathBuf,

    /// The path to the protobuf file descriptors
    #[arg(short, long, default_value = "./descriptors.bin")]
    descriptors: PathBuf,

    /// The message header key that contains the message type as the value to enable dynamic
    /// decoding
    #[arg(long, default_value = "message-name")]
    message_name_header: String,

    /// The consumer group id to use, defaults to a v4 uuid
    #[arg(short, long, default_value = Uuid::new_v4().to_string())]
    group_id: String,

    /// The offset for the topic
    #[arg(short, long, require_equals = true, default_value_t = Offset::End)]
    offset: Offset,

    /// A catchall for specifying additional librdkafka options in the format `<k1>=<v1>,<k2>=<v2>,...` (takes precedence over config file)
    ///
    #[arg(short, long)]
    rdkafka_options: Option<String>,

    /// Trim a number of bytes from the start of the payload
    #[arg(short, long)]
    trim_leading_bytes: Option<usize>,
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum Offset {
    /// Start consuming from the beginning of the partition.
    Beginning,
    /// Start consuming from the end of the partition.
    End,
    /// Start consuming from the stored offset.
    Stored,
}

impl Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Offset::Beginning => f.write_str("beginning"),
            Offset::End => f.write_str("end"),
            Offset::Stored => f.write_str("stored"),
        }
    }
}

impl From<Offset> for rdkafka::topic_partition_list::Offset {
    fn from(value: Offset) -> Self {
        match value {
            Offset::Beginning => rdkafka::topic_partition_list::Offset::Beginning,
            Offset::End => rdkafka::topic_partition_list::Offset::End,
            Offset::Stored => rdkafka::topic_partition_list::Offset::Stored,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();

    let config = KafkaConfig::from_file(args.config).await?;

    let mut client_config = &mut ClientConfig::new();

    for (k, v) in config.0 {
        client_config = client_config.set(k, v);
    }

    client_config.set("group.id", args.group_id);

    if let Some(opts) = args.rdkafka_options {
        for raw_option in opts.split(',') {
            let (k, v) = KafkaConfig::parse_option(raw_option)?;
            client_config.set(k, v);
        }
    }

    let consumer: StreamConsumer<DefaultConsumerContext> =
        client_config.create_with_context(DefaultConsumerContext)?;

    let metadata = consumer.fetch_metadata(Some(&args.topic), Duration::from_secs(10))?;

    let partitions = metadata
        .topics()
        .first()
        .map(|t| t.partitions().len())
        .map(i32::try_from)
        .transpose()?
        .ok_or(anyhow::anyhow!("failed to get topic partitions"))?;

    let mut l = TopicPartitionList::new();
    l.add_partition_range(&args.topic, 0, partitions);
    l.set_all_offsets(args.offset.into())?;
    consumer.assign(&l)?;
    info!("Subscribed consumer");

    let mut pool = DescriptorPool::new();

    let b = Bytes::from(read(args.descriptors).await?);

    pool.decode_file_descriptor_set(b)?;

    let output = tokio::io::stdout();

    let mut processor = MessageDumper::new(
        pool,
        output,
        args.message_name_header,
        args.trim_leading_bytes,
    );

    tokio::spawn(async move {
        info!("Starting consumer");
        loop {
            match consumer.recv().await {
                Err(e) => warn!("Kafka error: {e}"),
                Ok(m) => {
                    match processor.process_message(m).await {
                        Ok(()) => info!("processed message"),
                        Err(e) => warn!("failed to process message: {e}"),
                    };
                }
            };
        }
    });

    match signal::ctrl_c().await {
        Ok(()) => {
            info!("Received shutdown signal");
        }
        Err(err) => {
            error!("Failed to listen for shutdown signal: {err}");
        }
    }

    Ok(())
}
