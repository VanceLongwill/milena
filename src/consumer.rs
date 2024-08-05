use clap::{ArgGroup, Parser};
use displaydoc::Display;
use futures::Stream;
use log::{debug, info, trace, warn};
use prost::DecodeError;
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Header, Headers, Message, Timestamp};
use rdkafka::topic_partition_list::TopicPartitionList;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{BTreeSet, HashMap};
use std::string::FromUtf8Error;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

fn uuid() -> String {
    Uuid::new_v4().to_string()
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[clap(verbatim_doc_comment, group(
    ArgGroup::new("messagenameextraction")
        .required(true)
        .args(&["message_name", "message_name_from_header"]),
))]
pub struct ConsumeArgs {
    /// Name of the topic to consume
    #[arg(short, long)]
    pub topic: String,

    /// The protobuf message name itself. Useful when there's only one schema per topic.
    #[arg(short = 'N', long)]
    pub message_name: Option<String>,

    /// The message name header key that contains the message type as the value to enable dynamic
    /// decoding. Useful when there's more than one message type/schema per topic, but requires
    /// that the protobuf message name is present in the specified header.
    #[arg(short = 'H', long, verbatim_doc_comment)]
    pub message_name_from_header: Option<String>,

    /// The consumer group id to use, defaults to a v4 uuid
    #[arg(short, long, default_value = uuid())]
    #[serde(default = "uuid")]
    pub group_id: String,

    /// Offset to start consuming from:
    ///    beginning (default) | end | stored |
    ///    <value>  (absolute offset) |
    ///    -<value> (relative offset from end)
    ///    s@<value> (timestamp in ms to start at)
    ///    e@<value> (timestamp in ms to stop at (not included))
    /// When -o=s@ is used, it may be followed with another -o=e@ in order to consume messages between two
    /// timestamps.
    #[arg(short, long, require_equals = true, verbatim_doc_comment)]
    pub offsets: Option<Vec<Offset>>,

    /// Exit after consuming this many messages
    #[arg(short, long)]
    pub count: Option<usize>,

    ///  Exit successfully when last message received
    #[arg(short, long)]
    #[serde(default)]
    pub exit_on_last: bool,

    /// Trim a number of bytes from the start of the payload before attempting to deserialize
    #[arg(short = 'T', long)]
    pub trim_leading_bytes: Option<usize>,

    /// Decode the message key using a protobuf message
    #[arg(long)]
    pub key_message_name: Option<String>,

    /// Decode a header value with the given protobuf message in the format `<header-name>=<message-name>`
    #[arg(long)]
    pub header_message_name: Option<Vec<String>>,

    /// Parition to consume from, can be specified more than once for multiple partitions. Defaults to all partitions.
    #[arg(short = 'p', long)]
    pub partition: Option<Vec<i32>>,
}

#[derive(Debug, Clone)]
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
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Envelope {
    headers: Option<HashMap<String, Option<Value>>>,
    payload: Option<Payload>,
    offset: i64,
    timestamp: Option<i64>,
    key: Option<Value>,
    partition: i32,
    topic: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum Value {
    Plaintext(String),
    Protobuf(DynamicMessage),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Plaintext(s) => s.fmt(f),
            Value::Protobuf(p) => p.fmt(f),
        }
    }
}

pub struct ProtoConsumer {
    pool: DescriptorPool,
    message_name_extractor: MessageName,
    trim_leading_bytes: Option<usize>,
    header_message_names: Option<HashMap<String, String>>,
    key_message_name: Option<String>,
}

fn trim_bytes(x: usize, b: &[u8]) -> &[u8] {
    &b[x..]
}

#[derive(Debug, Serialize)]
pub struct MessageError {
    #[serde(serialize_with = "serialize_error")]
    pub error: Error,
    pub message: Envelope,
}

fn serialize_error<S>(error: &Error, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&error.to_string())
}

#[derive(Debug, Display, Error)]
pub enum Error {
    /// Failed to decode proto message payload: {0}
    DecodeProtoPayload(DecodeError),
    /// Failed to decode proto message header: {0}
    DecodeProtoHeader(DecodeError),
    /// Failed to decode proto message key: {0}
    DecodeProtoKey(DecodeError),
    /// Failed to decode utf-8 message header: {0}
    DecodeUTF8Header(FromUtf8Error),
    /// Missing headers required for decoding payload by header key
    MissingHeaders,
    /// Missing header: {0}
    MissingHeader(String),
    /// Header key has no value: {0}
    MissingHeaderValue(String),
    /// Payload message name not found in descriptor pool: {0}
    PayloadMessageNameNotFound(String),
    /// Key message name not found in descriptor pool: {0}
    KeyMessageNameNotFound(String),
    /// Header message name not found in descriptor pool: {0}
    HeaderMessageNameNotFound(String),
}

impl ProtoConsumer {
    pub fn new(
        pool: DescriptorPool,
        message_name_extractor: MessageName,
        trim_leading_bytes: Option<usize>,
        key_message_name: Option<String>,
        header_message_names: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            pool,
            message_name_extractor,
            trim_leading_bytes,
            key_message_name,
            header_message_names,
        }
    }

    fn decode_header_kv(&self, header: Header<&[u8]>) -> Result<(String, Option<Value>), Error> {
        let k = header.key;

        let message_name = self
            .header_message_names
            .as_ref()
            .and_then(|names| names.get(k));

        match (header.value, message_name) {
            (Some(v), Some(message_name)) => {
                let message_descriptor = self
                    .pool
                    .get_message_by_name(message_name)
                    .ok_or(Error::HeaderMessageNameNotFound(message_name.to_string()))?;
                let dynamic_message = DynamicMessage::decode(message_descriptor, v)
                    .map_err(Error::DecodeProtoHeader)?;
                Ok((k.to_string(), Some(Value::Protobuf(dynamic_message))))
            }
            (Some(v), None) => match String::from_utf8(v.to_vec()) {
                Ok(value) => Ok((k.to_string(), Some(Value::Plaintext(value)))),
                Err(err) => Err(Error::DecodeUTF8Header(err)),
            },
            _ => Ok((k.to_string(), None)),
        }
    }

    fn get_headers(&self, borrowed_headers: &BorrowedHeaders) -> HashMap<String, Option<Value>> {
        let mut headers = HashMap::with_capacity(borrowed_headers.count());
        for header in borrowed_headers.iter() {
            match self.decode_header_kv(header) {
                Ok((k, v)) => {
                    headers.insert(k, v);
                }
                Err(err) => {
                    warn!("Failed to decode header: {err}");
                }
            };
        }
        headers
    }

    fn decode_payload(
        &self,
        b: &[u8],
        maybe_headers: &Option<HashMap<String, Option<Value>>>,
    ) -> Result<DynamicMessage, Error> {
        let trimmed = if let Some(trim_leading_bytes) = self.trim_leading_bytes {
            trim_bytes(trim_leading_bytes, b)
        } else {
            b
        };

        let message_name = match &self.message_name_extractor {
            MessageName::Name(name) => name.to_string(),
            MessageName::HeaderKey(message_name_header) => {
                let headers = maybe_headers.as_ref().ok_or(Error::MissingHeaders)?;

                headers
                    .get(message_name_header)
                    .ok_or(Error::MissingHeader(message_name_header.to_string()))?
                    .as_ref()
                    .map(|k| k.to_string())
                    .ok_or(Error::MissingHeaderValue(message_name_header.to_string()))?
            }
        };

        let message_descriptor = self
            .pool
            .get_message_by_name(&message_name)
            .ok_or(Error::PayloadMessageNameNotFound(message_name))?;

        DynamicMessage::decode(message_descriptor, trimmed).map_err(Error::DecodeProtoPayload)
    }

    pub async fn process_message(
        &mut self,
        m: BorrowedMessage<'_>,
    ) -> Result<Option<Envelope>, Error> {
        let maybe_headers = m.headers().map(|b| self.get_headers(b));

        let payload = if let Some(b) = m.payload() {
            match self.decode_payload(b, &maybe_headers) {
                Ok(msg) => Some(Payload(msg)),
                Err(err) => {
                    warn!("Failed to decode message payload: {err}");
                    None
                }
            }
        } else {
            None
        };

        let key = match (m.key(), &self.key_message_name) {
            (Some(key_data), Some(key_message_name)) => {
                let message_descriptor = self
                    .pool
                    .get_message_by_name(key_message_name)
                    .ok_or(Error::KeyMessageNameNotFound(key_message_name.to_string()))?;
                let dynamic_message = DynamicMessage::decode(message_descriptor, key_data)
                    .map_err(Error::DecodeProtoKey)?;
                Some(Value::Protobuf(dynamic_message))
            }
            (Some(key_data), None) => match String::from_utf8(Vec::from(key_data)) {
                Ok(k) => Some(Value::Plaintext(k)),
                Err(err) => {
                    warn!("Failed to parse message key as utf-8 string: {err}");
                    None
                }
            },
            _ => None,
        };

        let timestamp = match m.timestamp() {
            Timestamp::NotAvailable => None,
            Timestamp::CreateTime(t) => Some(t),
            Timestamp::LogAppendTime(t) => Some(t),
        };

        Ok(Some(Envelope {
            headers: maybe_headers,
            offset: m.offset(),
            timestamp,
            key,
            partition: m.partition(),
            topic: m.topic().to_string(),
            payload,
        }))
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

pub fn create_consumer(
    client_config: ClientConfig,
    group_id: &str,
    exit_on_last: bool,
) -> anyhow::Result<StreamConsumer> {
    let mut client_config = client_config.clone();
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

    Ok(consumer)
}

struct State {
    consumer: StreamConsumer,
    processor: ProtoConsumer,
    received: usize,
    count: Option<usize>,
    max_offsets: Option<HashMap<(String, i32), i64>>,
    partitions: BTreeSet<i32>,
    partitions_reached_max: BTreeSet<i32>,
    exit_on_last: bool,
}

pub async fn start_consumer(
    consumer: StreamConsumer,
    descriptor_pool: DescriptorPool,
    args: ConsumeArgs,
) -> anyhow::Result<impl Stream<Item = anyhow::Result<Option<Envelope>>>> {
    let ConsumeArgs {
        topic,
        message_name,
        message_name_from_header,
        offsets,
        trim_leading_bytes,
        key_message_name,
        header_message_name,
        partition,
        count,
        ..
    } = args;

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

    let mut offsets = offsets.unwrap_or(vec![Offset::Beginning]).into_iter();
    let (start_offset, end_offset) = match (offsets.next(), offsets.next(), offsets.next()) {
        (Some(start @ Offset::Start(_)), end @ Some(Offset::Until(_)), None) => (start, end),
        (Some(first), None, None) => (first, None),
        (None, None, None) => (Offset::Beginning, None),
        _ => return Err(anyhow::anyhow!("Invalid offset arg(s)")),
    };

    match start_offset {
        Offset::Start(start) => {
            consumer.assign(&l)?;
            let tpl = consumer.offsets_for_timestamp(start, Duration::from_secs(1200))?;
            for el in tpl.elements() {
                // Dont care about failing as it only indicates that the given partition has been
                // filtered out already
                let _ = l.set_partition_offset(el.topic(), el.partition(), el.offset());
            }
        }
        Offset::Until(end) => {
            consumer.assign(&l)?;
            trace!("Querying offset for timestamp {end}");
            let tpl = consumer.offsets_for_timestamp(end, Duration::from_secs(1200))?;
            max_offsets = Some(tpl);
        }
        offset => {
            l.set_all_offsets(rdkafka::Offset::try_from(offset)?)?;
        }
    }

    if let Some(Offset::Until(end)) = end_offset {
        trace!("Querying offset for timestamp {end}");
        consumer.assign(&l)?;
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

    consumer.assign(&l)?;
    info!("Subscribed consumer to topic {l:?}");

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
            return Err(anyhow::anyhow!("Message count is zero, exiting")); // @TODO should this be error?
        }
    }

    let processor = ProtoConsumer::new(
        descriptor_pool,
        message_name_extraction,
        trim_leading_bytes,
        key_message_name,
        headers_messages,
    );

    if let Some(count) = count {
        debug!("Exiting after {count:?} messages");
    };

    let state = State {
        consumer,
        processor,
        received: 0,
        count,
        max_offsets,
        partitions_reached_max: BTreeSet::<i32>::new(),
        partitions,
        exit_on_last: args.exit_on_last,
    };

    let s = futures::stream::unfold(state, |mut s| async {
        match s.consumer.recv().await {
            Ok(m) => {
                debug!("Got message");
                let current_offset = m.offset();
                let current_partition = m.partition();
                if let Some(max_offset) = s
                    .max_offsets
                    .as_ref()
                    .and_then(|offsets| offsets.get(&(m.topic().to_string(), current_partition)))
                {
                    if current_offset >= *max_offset {
                        debug!("Reached specified maximum offset for partition {current_partition}, exiting");
                        s.partitions_reached_max.insert(current_partition);
                        if s.partitions_reached_max == s.partitions {
                            debug!("Reached end of all partitions, exiting");
                            return None;
                        }
                    }
                }

                s.received += 1;
                if let Some(count) = s.count {
                    if s.received >= count - 1 {
                        debug!("Exiting after {} messages processed", s.received);
                        return None;
                    }
                }
                Some((
                    s.processor
                        .process_message(m)
                        .await
                        .map_err(anyhow::Error::from), // @TODO
                    s,
                ))
            }
            Err(err) => match err {
                KafkaError::PartitionEOF(current_partition) if s.exit_on_last => {
                    debug!("Reached end of partition {current_partition}");
                    s.partitions_reached_max.insert(current_partition);
                    if s.partitions_reached_max == s.partitions {
                        debug!("Reached end of all partitions, exiting");
                        None
                    } else {
                        Some((Err(err.into()), s))
                    }
                }
                _ => Some((Err(err.into()), s)),
            },
        }
    });

    Ok(s)
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Offset {
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

impl std::fmt::Display for Offset {
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
