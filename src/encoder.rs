use clap::{arg, Parser};
use log::debug;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[derive(Parser, Debug)]
pub struct EncodeArgs {
    /// Data to send to the topic
    data: String,

    /// The fully qualified name of the protobuf message
    #[arg(short = 'N', long)]
    message_name: String,

    /// Pad left with bytes
    pad_left: Vec<u8>,
}

pub struct ProtoEncoder<W: AsyncWrite + Unpin> {
    pool: DescriptorPool,
    output: W,
}

impl<W: AsyncWrite + Unpin> ProtoEncoder<W> {
    pub fn new(pool: DescriptorPool, output: W) -> ProtoEncoder<W> {
        ProtoEncoder { pool, output }
    }

    pub async fn encode(&mut self, args: EncodeArgs) -> anyhow::Result<()> {
        let EncodeArgs {
            data,
            message_name,
            pad_left,
        } = args;

        let message_descriptor =
            self.pool
                .get_message_by_name(&message_name)
                .ok_or(anyhow::anyhow!(
                    "message name not found in descriptors: {message_name}"
                ))?;

        let mut deserializer = serde_json::de::Deserializer::from_str(&data);

        let dynamic_message = DynamicMessage::deserialize(message_descriptor, &mut deserializer)?;

        debug!("deserialized message: {dynamic_message:?}");

        let payload = dynamic_message.encode_to_vec();

        self.output.write_all(pad_left.as_slice()).await?;
        self.output.write_all(payload.as_slice()).await?;
        Ok(())
    }
}
