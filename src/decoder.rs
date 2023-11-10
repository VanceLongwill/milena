use clap::Parser;
use prost_reflect::{DescriptorPool, DynamicMessage};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

#[derive(Parser, Debug)]
pub struct DecodeArgs {
    /// The protobuf message name itself
    #[arg(short = 'N', long)]
    message_name: String,

    /// Trim a number of bytes from the start of the payload before attempting to deserialize
    #[arg(short = 'T', long)]
    trim_leading_bytes: Option<usize>,

    /// Message to decode, defaults to stdin
    message: Option<Vec<u8>>,
}

pub struct ProtoDecoder<W: AsyncWrite + Unpin> {
    pool: DescriptorPool,
    output: W,
}

fn trim_bytes(x: usize, b: &[u8]) -> &[u8] {
    &b[x..]
}

impl<W: AsyncWrite + Unpin> ProtoDecoder<W> {
    pub fn new(pool: DescriptorPool, output: W) -> ProtoDecoder<W> {
        ProtoDecoder { pool, output }
    }
    pub async fn decode(&mut self, args: DecodeArgs) -> anyhow::Result<()> {
        let DecodeArgs {
            message_name,
            trim_leading_bytes,
            message,
        } = args;

        let buf = if let Some(m) = message {
            m
        } else {
            let mut stdin = BufReader::new(tokio::io::stdin());
            let mut buf = vec![];
            stdin.read_to_end(&mut buf).await?;
            buf
        };

        let data = buf.as_slice();

        let trimmed = trim_leading_bytes
            .map(|l| trim_bytes(l, data))
            .unwrap_or(data);

        self.decode_message(trimmed, message_name).await?;
        Ok(())
    }

    async fn decode_message(&mut self, data: &[u8], message_name: String) -> anyhow::Result<()> {
        let message_descriptor = self
            .pool
            .get_message_by_name(&message_name)
            .ok_or(anyhow::anyhow!("unknown message type: '{message_name}'"))?;

        let dynamic_message = DynamicMessage::decode(message_descriptor, data)?;
        let out = serde_json::to_vec(&dynamic_message)?;
        self.output.write_all(&out).await?;
        Ok(())
    }
}
