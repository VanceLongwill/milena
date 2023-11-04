use log::{debug, warn};
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Headers, Message, Timestamp};
use serde::Serialize;
use std::collections::HashMap;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub struct Payload(DynamicMessage);

#[derive(Serialize)]
pub struct Envelope {
    headers: HashMap<String, String>,
    payload: Payload,
    offset: i64,
    timestamp: Option<i64>,
    key: Option<String>,
    partition: i32,
    topic: String,
}

impl Serialize for Payload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let options = SerializeOptions::new().skip_default_fields(false);
        self.0.serialize_with_options(serializer, &options)
    }
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

pub struct Consumer<W: AsyncWrite + Unpin> {
    pool: DescriptorPool,
    message_name_header: String,
    output: W,
    trim_leading_bytes: Option<usize>,
}

fn trim_bytes(x: usize, b: &[u8]) -> &[u8] {
    &b[x..]
}

impl<W: AsyncWrite + Unpin> Consumer<W> {
    pub fn new(
        pool: DescriptorPool,
        output: W,
        message_name_header: String,
        trim_leading_bytes: Option<usize>,
    ) -> Self {
        Self {
            pool,
            output,
            message_name_header,
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

            let headers = m
                .headers()
                .map(get_headers)
                .ok_or(anyhow::anyhow!("missing headers"))?;

            let message_name_header = &self.message_name_header;
            let message_name = headers
                .get(message_name_header)
                .ok_or(anyhow::anyhow!("missing '{message_name_header}' header"))?
                .to_string();

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
                headers,
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
