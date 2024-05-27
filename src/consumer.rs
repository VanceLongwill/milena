use clap::{ArgGroup, Parser};
use futures::StreamExt;
use log::{debug, info, trace, warn};
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers, Message, Timestamp};
use rdkafka::topic_partition_list::TopicPartitionList;
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::time::Duration;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[clap(verbatim_doc_comment, group(
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
    #[arg(short = 'H', long, verbatim_doc_comment)]
    message_name_from_header: Option<String>,

    /// The consumer group id to use, defaults to a v4 uuid
    #[arg(short, long, default_value = Uuid::new_v4().to_string())]
    group_id: String,

    /// Offset to start consuming from:
    ///    beginning (default) | end | stored |
    ///    <value>  (absolute offset) |
    ///    -<value> (relative offset from end)
    ///    s@<value> (timestamp in ms to start at)
    ///    e@<value> (timestamp in ms to stop at (not included))
    /// When -o=s@ is used, it may be followed with another -o=e@ in order to consume messages between two
    /// timestamps.
    #[arg(short, long, require_equals = true, verbatim_doc_comment)]
    offsets: Vec<Offset>,

    /// Exit after consuming this many messages
    #[arg(short, long)]
    count: Option<usize>,

    ///  Exit successfully when last message received
    #[arg(short, long)]
    exit_on_last: bool,

    /// Trim a number of bytes from the start of the payload before attempting to deserialize
    #[arg(short = 'T', long)]
    trim_leading_bytes: Option<usize>,

    /// Decode the message key using a protobuf message
    key_message_name: Option<String>,

    /// Decode a header value with the given protobuf message in the format `<header-name>=<message-name>`
    header_message_name: Option<Vec<String>>,

    /// Parition to consume from, can be specified more than once for multiple partitions. Defaults to all partitions.
    #[arg(short = 'p', long)]
    partition: Option<Vec<i32>>,
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
    headers: Option<HashMap<String, Value>>,
    payload: Payload,
    offset: i64,
    timestamp: Option<i64>,
    key: Option<Value>,
    partition: i32,
    topic: String,
}

#[derive(Debug, Clone, Serialize)]
pub enum Value {
    Plaintext(String),
    Protobuf(DynamicMessage),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Plaintext(s) => s.fmt(f),
            Value::Protobuf(p) => p.fmt(f),
        }
    }
}

pub struct ProtoConsumer<W: AsyncWrite + Unpin> {
    pool: DescriptorPool,
    message_name_extractor: MessageName,
    output: W,
    trim_leading_bytes: Option<usize>,
    header_message_names: Option<HashMap<String, String>>,
    key_message_name: Option<String>,
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
        key_message_name: Option<String>,
        header_message_names: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            pool,
            output,
            message_name_extractor,
            trim_leading_bytes,
            key_message_name,
            header_message_names,
        }
    }

    pub async fn process_message(&mut self, m: BorrowedMessage<'_>) -> anyhow::Result<()> {
        if let Some(b) = m.payload() {
            let trimmed = if let Some(trim_leading_bytes) = self.trim_leading_bytes {
                trim_bytes(trim_leading_bytes, b)
            } else {
                b
            };

            let maybe_headers = if let Some(b) = m.headers() {
                let mut headers = HashMap::new();
                for i in 0..b.count() {
                    let Some((k, v)) = b.get(i) else {
                        debug!("no more headers");
                        break;
                    };

                    if let Some(header_message_names) = &self.header_message_names {
                        if let Some(message_name) = header_message_names.get(k) {
                            let message_descriptor = self
                                .pool
                                .get_message_by_name(message_name)
                                .ok_or(anyhow::anyhow!(
                                    "cannot find header message in descriptors: {message_name}"
                                ))?;
                            let dynamic_message = DynamicMessage::decode(message_descriptor, v)?;
                            headers.insert(k.to_string(), Value::Protobuf(dynamic_message));
                        }
                    };

                    match String::from_utf8(v.into()) {
                        Ok(value) => {
                            headers.insert(k.to_string(), Value::Plaintext(value));
                        }
                        Err(err) => {
                            warn!("failed to decode header: {err}");
                        }
                    };
                }
                Some(headers)
            } else {
                None
            };

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

            let key = if let Some(key_data) = m.key() {
                if let Some(key_message_name) = &self.key_message_name {
                    let message_descriptor = self
                        .pool
                        .get_message_by_name(key_message_name)
                        .ok_or(anyhow::anyhow!(
                            "cannot find header message in descriptors: {message_name}"
                        ))?;
                    let dynamic_message = DynamicMessage::decode(message_descriptor, key_data)?;
                    Some(Value::Protobuf(dynamic_message))
                } else {
                    match String::from_utf8(Vec::from(key_data)) {
                        Ok(k) => Some(Value::Plaintext(k)),
                        Err(err) => {
                            warn!("failed to parse message key: {err}");
                            None
                        }
                    }
                }
            } else {
                None
            };

            let offset = m.offset();
            let timestamp = match m.timestamp() {
                Timestamp::NotAvailable => None,
                Timestamp::CreateTime(t) => Some(t),
                Timestamp::LogAppendTime(t) => Some(t),
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

fn get_absolute_offset(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    offset: rdkafka::Offset,
) -> Result<i64, anyhow::Error> {
    match offset {
        rdkafka::Offset::Beginning => Ok(0),
        rdkafka::Offset::Stored => Err(anyhow::anyhow!(
            "absolute offset for stored offset is not supported"
        )),
        rdkafka::Offset::Invalid => Err(anyhow::anyhow!("invalid max offset {:?}", offset)),
        rdkafka::Offset::End => {
            let (_low, high) =
                consumer.fetch_watermarks(topic, partition, Duration::from_secs(10))?;
            Ok(high)
        }
        rdkafka::Offset::OffsetTail(tail) => {
            let (_low, high) =
                consumer.fetch_watermarks(topic, partition, Duration::from_secs(10))?;
            Ok(high - tail)
        }
        rdkafka::Offset::Offset(head) => Ok(head),
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
        offsets,
        trim_leading_bytes,
        key_message_name,
        header_message_name,
        partition,
        count,
        exit_on_last,
    } = args;
    client_config.set("group.id", group_id);

    if exit_on_last {
        if let Some(eof) = client_config.get("enable.partition.eof") {
            let will_err = eof.parse::<bool>()?;
            if !will_err {
                return Err(anyhow::anyhow!("Incompatible config: \"enable.partition.eof\" is enabled in RDKafka config, but must be disabled when used with -e --exit-on-last flag"));
            }
        }
        client_config.set("enable.partition.eof", "true");
    }

    let consumer: StreamConsumer<DefaultConsumerContext> =
        client_config.create_with_context(DefaultConsumerContext)?;

    let partitions: BTreeSet<i32> = if let Some(partition) = partition {
        partition.into_iter().collect()
    } else {
        let metadata = consumer.fetch_metadata(Some(&topic), Duration::from_secs(10))?;
        let partitions_meta = metadata
            .topics()
            .first()
            .map(|t| t.partitions())
            .ok_or(anyhow::anyhow!("failed to get topic partitions"))?;

        partitions_meta.iter().map(|p| p.id()).collect()
    };

    let mut l = TopicPartitionList::new();
    let mut max_offsets = None;

    for p in &partitions {
        debug!("Adding partition {p}");
        // add all topics with the offset defaulted to beginning
        l.add_partition_offset(&topic, *p, rdkafka::Offset::Beginning)?;
    }
    consumer.assign(&l)?;

    let mut offsets = offsets.into_iter();
    let (start_offset, end_offset) = match (offsets.next(), offsets.next(), offsets.next()) {
        (Some(start @ Offset::Start(_)), end @ Some(Offset::Until(_)), None) => (start, end),
        (Some(first), None, None) => (first, None),
        (None, None, None) => (Offset::Beginning, None),
        _ => return Err(anyhow::anyhow!("Invalid offset arg(s)")),
    };

    match start_offset {
        Offset::Start(start) => {
            let tpl = consumer.offsets_for_timestamp(start, Duration::from_secs(1200))?;
            for el in tpl.elements() {
                // Dont care about failing as it only indicates that the given partition has been
                // filtered out already
                let _ = l.set_partition_offset(el.topic(), el.partition(), el.offset());
            }
        }
        Offset::Until(end) => {
            trace!("Querying offset for timestamp {end}");
            let tpl = consumer.offsets_for_timestamp(end, Duration::from_secs(1200))?;
            max_offsets = Some(tpl);
        }
        offset => {
            l.set_all_offsets(offset.try_into()?)?;
        }
    }

    if let Some(Offset::Until(end)) = end_offset {
        trace!("Querying offset for timestamp {end}");
        let tpl = consumer.offsets_for_timestamp(end, Duration::from_secs(1200))?;
        max_offsets = Some(tpl);
    };

    let max_offsets = if let Some(offsets) = max_offsets {
        let mut absolute_offsets = HashMap::new();
        for el in offsets.elements() {
            let absolute = get_absolute_offset(&consumer, el.topic(), el.partition(), el.offset())?;
            absolute_offsets.insert((el.topic().to_string(), el.partition()), absolute);
        }
        debug!("Ending at max topic offsets: {absolute_offsets:?}");
        Some(absolute_offsets)
    } else {
        None
    };

    info!("Subscribed consumer to topic {l:?}");

    let output = tokio::io::stdout();

    let message_name_extraction = match (message_name, message_name_from_header) {
        (Some(name), _) => Ok(MessageName::Name(name)),
        (_, Some(header)) => Ok(MessageName::HeaderKey(header)),
        (None, None) => Err(anyhow::anyhow!("message name flag required")),
    }?;

    let headers_messages = header_message_name.map(|v| {
        v.iter().map(|s| {
            s.split_once('=')
                .ok_or(anyhow::anyhow!("invalid format for header message names, expected <header-name>=<message-name>"))
                .map(|(k, v)| (k.to_string(), v.to_string())) })
                .collect::<anyhow::Result<HashMap<_, _>>>()
    }).transpose()?;

    if let Some(count) = count {
        if count == 0 {
            info!("Message count is zero, exiting");
            return Ok(());
        }
    }

    let mut processor = ProtoConsumer::new(
        descriptor_pool,
        output,
        message_name_extraction,
        trim_leading_bytes,
        key_message_name,
        headers_messages,
    );

    if let Some(count) = count {
        debug!("Exiting after {count:?} messages");
    };

    let mut stream = consumer.stream();
    let mut received = 0;

    let mut partitions_reached_max = BTreeSet::<i32>::new();

    while let Some(message) = stream.next().await {
        match message {
            Err(err) => match err {
                KafkaError::PartitionEOF(current_partition) if exit_on_last => {
                    debug!("Reached end of partition {current_partition}");
                    partitions_reached_max.insert(current_partition);
                    if partitions_reached_max == partitions {
                        debug!("Reached end of all partitions, exiting");
                        return Ok(());
                    }
                }
                _ => {
                    return Err(err.into());
                }
            },
            Ok(m) => {
                let current_offset = m.offset();
                let current_partition = m.partition();
                if let Some(max_offset) = max_offsets
                    .as_ref()
                    .and_then(|offsets| offsets.get(&(m.topic().to_string(), current_partition)))
                {
                    if current_offset >= *max_offset {
                        debug!("Reached specified maximum offset for partition {current_partition}, exiting");
                        partitions_reached_max.insert(current_partition);
                        if partitions_reached_max == partitions {
                            debug!("Reached end of all partitions, exiting");
                            return Ok(());
                        }
                        return Ok(());
                    }
                }

                received += 1;

                match processor.process_message(m).await {
                    Ok(()) => info!("processed message"),
                    Err(e) => warn!("failed to process message: {e}"),
                };

                if let Some(count) = count {
                    if received >= count - 1 {
                        debug!("Exiting after {received} messages processed");
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Offset {
    /// Start consuming from the beginning of the partition.
    Beginning,
    /// Start consuming from the end of the partition.
    End,
    /// Start consuming from the stored offset.
    Stored,
    /// Start consuming from the given offset integer, negative offsets from end
    Numeric(i64),
    /// Start consuming from the given timestamp
    Start(i64),
    /// Stop consuming at the given timestamp
    Until(i64),
}

impl From<&str> for Offset {
    fn from(value: &str) -> Self {
        match value {
            "beginning" => Self::Beginning,
            "end" => Self::End,
            "stored" => Self::Stored,
            other => match other.split_once('@') {
                Some(("s", timestamp)) => {
                    let start = timestamp.parse::<i64>().unwrap_or(0);
                    Self::Start(start)
                }
                Some(("e", timestamp)) => {
                    let end = timestamp.parse::<i64>().unwrap_or(0);
                    Self::Until(end)
                }
                _ => Self::Numeric(other.parse::<i64>().unwrap_or(0)),
            },
        }
    }
}

impl Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Offset::Beginning => f.write_str("beginning"),
            Offset::End => f.write_str("end"),
            Offset::Stored => f.write_str("stored"),
            Offset::Numeric(n) => f.write_fmt(format_args!("numeric {n}")),
            Offset::Start(n) => f.write_fmt(format_args!("start {n}")),
            Offset::Until(n) => f.write_fmt(format_args!("until {n}")),
        }
    }
}

impl TryFrom<Offset> for rdkafka::topic_partition_list::Offset {
    type Error = anyhow::Error;

    fn try_from(value: Offset) -> Result<Self, Self::Error> {
        match value {
            Offset::Beginning => Ok(rdkafka::topic_partition_list::Offset::Beginning),
            Offset::End => Ok(rdkafka::topic_partition_list::Offset::End),
            Offset::Stored => Ok(rdkafka::topic_partition_list::Offset::Stored),
            Offset::Numeric(n) => Ok(if n < 0 {
                rdkafka::topic_partition_list::Offset::OffsetTail(n.abs())
            } else {
                rdkafka::topic_partition_list::Offset::Offset(n)
            }),
            _ => Err(anyhow::anyhow!("invalid offset {value:?}")),
        }
    }
}

pub enum MessageName {
    Name(String),
    HeaderKey(String),
}
