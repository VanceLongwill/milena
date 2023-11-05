use clap::{ArgGroup, Parser, ValueEnum};
use log::{debug, info, warn};
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Headers, Message, Timestamp};
use rdkafka::topic_partition_list::TopicPartitionList;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[clap(group(
    ArgGroup::new("messagenameextraction")
        .required(true)
        .args(&["message_name", "message_name_from_header"]),
))]
pub struct ConsumeArgs {
    /// Name of the topic to consume
    #[arg(short, long)]
    topic: String,

    /// The protobuf message name itself. Useful when there's only one schema per topic.
    #[arg(short = 'N', long)]
    message_name: Option<String>,

    /// The message name header key that contains the message type as the value to enable dynamic
    /// decoding. Useful when there's more than one message type/schema per topic, but requires
    /// that the protobuf message name is present in the specified header.
    #[arg(short = 'H', long)]
    message_name_from_header: Option<String>,

    /// The consumer group id to use, defaults to a v4 uuid
    #[arg(short, long, default_value = Uuid::new_v4().to_string())]
    group_id: String,

    /// The offset for the topic
    #[arg(short, long, require_equals = true, default_value_t = Offset::End)]
    offset: Offset,

    /// Trim a number of bytes from the start of the payload before attempting to deserialize
    #[arg(short = 'T', long)]
    trim_leading_bytes: Option<usize>,
}

pub struct Payload(DynamicMessage);

impl Serialize for Payload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let options = SerializeOptions::new().skip_default_fields(false);
        self.0.serialize_with_options(serializer, &options)
    }
}

/// Envelope represents a kafka message along with its relevant metadata
#[derive(Serialize)]
pub struct Envelope {
    headers: Option<HashMap<String, String>>,
    payload: Payload,
    offset: i64,
    timestamp: Option<i64>,
    key: Option<String>,
    partition: i32,
    topic: String,
}

fn get_headers(b: &BorrowedHeaders) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    for i in 0..b.count() {
        let Some((k, v)) = b.get(i) else {
            debug!("no more headers");
            break;
        };
        match String::from_utf8(v.into()) {
            Ok(value) => {
                headers.insert(k.to_string(), value);
            }
            Err(err) => {
                warn!("failed to decode header: {err}");
            }
        };
    }
    headers
}

pub struct ProtoConsumer<W: AsyncWrite + Unpin> {
    pool: DescriptorPool,
    message_name_extractor: MessageName,
    output: W,
    trim_leading_bytes: Option<usize>,
}

fn trim_bytes(x: usize, b: &[u8]) -> &[u8] {
    &b[x..]
}

impl<W: AsyncWrite + Unpin> ProtoConsumer<W> {
    pub fn new(
        pool: DescriptorPool,
        output: W,
        message_name_extractor: MessageName,
        trim_leading_bytes: Option<usize>,
    ) -> Self {
        Self {
            pool,
            output,
            message_name_extractor,
            trim_leading_bytes,
        }
    }

    pub async fn process_message(&mut self, m: BorrowedMessage<'_>) -> anyhow::Result<()> {
        if let Some(b) = m.payload() {
            let trimmed = if let Some(trim_leading_bytes) = self.trim_leading_bytes {
                trim_bytes(trim_leading_bytes, b)
            } else {
                b
            };

            let maybe_headers = m.headers().map(get_headers);

            let message_name = match &self.message_name_extractor {
                MessageName::Name(name) => name.to_owned(),
                MessageName::HeaderKey(message_name_header) => {
                    let headers = maybe_headers.clone().ok_or(anyhow::anyhow!(
                        "missing headers, required for protobuf deserialization"
                    ))?;
                    headers
                        .get(message_name_header)
                        .ok_or(anyhow::anyhow!("missing '{message_name_header}' header"))?
                        .to_string()
                }
            };

            let message_descriptor = self
                .pool
                .get_message_by_name(&message_name)
                .ok_or(anyhow::anyhow!("unknown message type: '{message_name}'"))?;

            let dynamic_message = DynamicMessage::decode(message_descriptor, trimmed)?;

            let offset = m.offset();
            let timestamp = match m.timestamp() {
                Timestamp::NotAvailable => None,
                Timestamp::CreateTime(t) => Some(t),
                Timestamp::LogAppendTime(t) => Some(t),
            };
            let key = match m.key().map(Vec::from).map(String::from_utf8).transpose() {
                Ok(k) => k,
                Err(err) => {
                    warn!("failed to parse message key: {err}");
                    None
                }
            };
            let partition = m.partition();
            let topic = m.topic().to_string();

            let out = Envelope {
                headers: maybe_headers,
                offset,
                timestamp,
                key,
                partition,
                topic,
                payload: Payload(dynamic_message),
            };

            let mut out = serde_json::to_vec(&out)?;
            out.push(b'\n');
            self.output.write_all(&out).await?;
        };

        Ok(())
    }
}

pub async fn start_consumer(
    client_config: &mut ClientConfig,
    descriptor_pool: DescriptorPool,
    args: ConsumeArgs,
) -> anyhow::Result<()> {
    let ConsumeArgs {
        topic,
        message_name,
        message_name_from_header,
        group_id,
        offset,
        trim_leading_bytes,
    } = args;
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

    let output = tokio::io::stdout();

    let message_name_extraction = match (message_name, message_name_from_header) {
        (Some(name), _) => Ok(MessageName::Name(name)),
        (_, Some(header)) => Ok(MessageName::HeaderKey(header)),
        (None, None) => Err(anyhow::anyhow!("message name flag required")),
    }?;

    let mut processor = ProtoConsumer::new(
        descriptor_pool,
        output,
        message_name_extraction,
        trim_leading_bytes,
    );

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

pub enum MessageName {
    Name(String),
    HeaderKey(String),
}
