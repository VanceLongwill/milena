use clap::{arg, Parser};
use log::debug;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;

pub struct ProtoProducer {
    client_config: ClientConfig,
    descriptor_pool: DescriptorPool,
}

#[derive(Parser, Debug)]
pub struct ProduceArgs {
    /// Name of the topic to produce to
    #[arg(short, long)]
    topic: String,

    /// Data to send to the topic, curl style
    #[arg(short, long)]
    data: String,

    /// Message headers <header=value>. Can be supplied more than once.
    #[arg(short = 'H', long = "header")]
    headers: Vec<String>,

    /// Key for the message
    #[arg(short, long)]
    key: String,

    /// The fully qualified name of the protobuf message
    #[arg(short = 'N', long)]
    message_name: String,
}

/// Envelope represents a kafka message along with its relevant metadata
#[derive(Serialize)]
pub struct Envelope {
    message_name: String,
    payload: String,
    key: Option<String>,
    topic: String,
    headers: HashMap<String, String>,
}

impl ProtoProducer {
    pub fn new(client_config: ClientConfig, descriptor_pool: DescriptorPool) -> ProtoProducer {
        ProtoProducer {
            client_config,
            descriptor_pool,
        }
    }

    pub async fn produce_from_envelope(&self, envelope: &Envelope) -> anyhow::Result<()> {
        let producer: &FutureProducer = &self.client_config.create()?;
        let message_name = &envelope.message_name;

        let message_descriptor = self
            .descriptor_pool
            .get_message_by_name(message_name)
            .ok_or(anyhow::anyhow!(
                "message name not found in descriptors: {message_name}"
            ))?;

        debug!("found message");

        let mut deserializer = serde_json::de::Deserializer::from_str(&envelope.payload);

        let dynamic_message = DynamicMessage::deserialize(message_descriptor, &mut deserializer)?;

        debug!("deserialized message: {dynamic_message:?}");

        let payload = dynamic_message.encode_to_vec();

        debug!("sending message: {payload:?}");

        let headers = envelope
            .headers
            .iter()
            .fold(OwnedHeaders::new(), |acc, (k, v)| acc.add(k, v));

        let mut msg = FutureRecord::to(&envelope.topic)
            .payload(&envelope.payload)
            .headers(headers);

        if let Some(key) = &envelope.key {
            msg = msg.key(key);
        };

        producer
            .send(msg, Duration::from_secs(5))
            .await
            .map_err(|err| anyhow::anyhow!("failed to send message: {err:?}"))?;

        Ok(())
    }

    pub async fn produce(
        &self,
        ProduceArgs {
            topic,
            data,
            headers,
            key,
            message_name: message_type,
        }: ProduceArgs,
    ) -> anyhow::Result<()> {
        debug!("produce");
        let producer: &FutureProducer = &self.client_config.create()?;

        debug!("created producer");

        let message_descriptor = self
            .descriptor_pool
            .get_message_by_name(&message_type)
            .ok_or(anyhow::anyhow!(
                "message name not found in descriptors: {message_type}"
            ))?;

        debug!("found message");

        let mut deserializer = serde_json::de::Deserializer::from_str(&data);

        let dynamic_message = DynamicMessage::deserialize(message_descriptor, &mut deserializer)?;

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

        Ok(())
    }
}

fn parse_headers(
    headers: impl IntoIterator<Item = impl AsRef<str>>,
) -> anyhow::Result<OwnedHeaders> {
    Ok(headers
        .into_iter()
        .map(|header| {
            let mut parts = header.as_ref().splitn(2, '=');
            match (parts.next(), parts.next()) {
                (Some(key), Some(value)) => Ok((key.to_string(), value.to_string())),
                _ => Err(anyhow::anyhow!("invalid header")),
            }
        })
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .fold(OwnedHeaders::new(), |acc, (k, v)| acc.add(k, v)))
}
