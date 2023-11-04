use bytes::Bytes;
use clap::{Parser, Subcommand, ValueEnum};
use log::{debug, error, info, warn};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;
use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::{read, read_to_string};
use tokio::signal;
use uuid::Uuid;

use milena::consumer::Consumer as MessageDumper;

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

#[derive(Subcommand, Debug)]
enum Command {
    Consume {
        /// Name of the topic to consume
        #[arg(short, long)]
        topic: String,

        /// The path to the protobuf file descriptors
        #[arg(short, long, default_value = "./descriptors.binpb")]
        file_descriptors: PathBuf,

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

        /// Trim a number of bytes from the start of the payload
        #[arg(short = 'T', long)]
        trim_leading_bytes: Option<usize>,
    },
    Produce {
        /// Name of the topic to consume
        #[arg(short, long)]
        topic: String,

        /// Data to send to the topic
        #[arg(short, long)]
        data: String,

        /// Headers in curl format. Can be supplied more than once.
        #[arg(short = 'H', long)]
        headers: Vec<String>,

        /// Key for the message
        #[arg(short, long)]
        key: String,

        /// Message type
        #[arg(short, long)]
        message_type: String,

        /// The path to the protobuf file descriptors
        #[arg(short, long, default_value = "./descriptors.binpb")]
        file_descriptors: PathBuf,
    },
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE", global=true, default_value = default_config_path().into_os_string())]
    config: PathBuf,

    /// A catchall for specifying additional librdkafka options in the format `<k1>=<v1>,<k2>=<v2>,...` (takes precedence over config file)
    ///
    #[arg(short, long, global = true)]
    rdkafka_options: Option<String>,
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

    let cli = Cli::parse();

    let config = KafkaConfig::from_file(cli.config).await?;

    let mut client_config = &mut ClientConfig::new();

    for (k, v) in config.0 {
        client_config = client_config.set(k, v);
    }

    if let Some(opts) = cli.rdkafka_options {
        for raw_option in opts.split(',') {
            let (k, v) = KafkaConfig::parse_option(raw_option)?;
            client_config.set(k, v);
        }
    }

    match cli.command {
        Command::Consume {
            topic,
            file_descriptors,
            message_name_header,
            group_id,
            offset,
            trim_leading_bytes,
        } => {
            client_config.set("group.id", group_id);

            let consumer: StreamConsumer<DefaultConsumerContext> =
                client_config.create_with_context(DefaultConsumerContext)?;

            let metadata = consumer.fetch_metadata(Some(&topic), Duration::from_secs(10))?;

            let partitions = metadata
                .topics()
                .first()
                .map(|t| t.partitions().len())
                .map(i32::try_from)
                .transpose()?
                .ok_or(anyhow::anyhow!("failed to get topic partitions"))?;

            let mut l = TopicPartitionList::new();
            l.add_partition_range(&topic, 0, partitions);
            l.set_all_offsets(offset.into())?;
            consumer.assign(&l)?;
            info!("Subscribed consumer");

            let mut pool = DescriptorPool::new();

            let b = Bytes::from(read(file_descriptors).await?);

            pool.decode_file_descriptor_set(b)?;

            let output = tokio::io::stdout();

            let mut processor =
                MessageDumper::new(pool, output, message_name_header, trim_leading_bytes);

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
        }
        Command::Produce {
            topic,
            data,
            headers,
            key,
            message_type,
            file_descriptors,
        } => {
            debug!("produce");
            let producer: &FutureProducer = &client_config.create()?;

            debug!("created producer");

            let mut pool = DescriptorPool::new();

            let b = Bytes::from(read(file_descriptors).await?);

            pool.decode_file_descriptor_set(b)?;

            let message_descriptor =
                pool.get_message_by_name(&message_type)
                    .ok_or(anyhow::anyhow!(
                        "message name not found in descriptors: {message_type}"
                    ))?;

            debug!("found message");

            let mut deserializer = serde_json::de::Deserializer::from_str(&data);

            let dynamic_message =
                DynamicMessage::deserialize(message_descriptor, &mut deserializer)?;

            debug!("deserialized message: {dynamic_message:?}");

            let payload = dynamic_message.encode_to_vec();

            debug!("sending message: {payload:?}");

            producer
                .send(
                    FutureRecord::to(&topic)
                        .payload(&payload)
                        .key(&key)
                        .headers(parse_headers(headers)?),
                    Duration::from_secs(5),
                )
                .await
                .map_err(|err| anyhow::anyhow!("failed to send message: {err:?}"))?;
        }
    }

    Ok(())
}

fn parse_headers(
    headers: impl IntoIterator<Item = impl AsRef<str>>,
) -> anyhow::Result<OwnedHeaders> {
    Ok(headers
        .into_iter()
        .map(|header| {
            let mut parts = header.as_ref().splitn(2, ':');
            match (parts.next(), parts.next()) {
                (Some(key), Some(value)) => Ok((key.to_string(), value.to_string())),
                _ => Err(anyhow::anyhow!("invalid header")),
            }
        })
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .fold(OwnedHeaders::new(), |acc, (k, v)| acc.add(k, v)))
}
