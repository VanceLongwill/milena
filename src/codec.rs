use std::io::{Error, ErrorKind};

use tokio_util::codec::{Decoder, Encoder, LinesCodecError};

pub struct LinesCodec(tokio_util::codec::LinesCodec);

impl From<tokio_util::codec::LinesCodec> for LinesCodec {
    fn from(value: tokio_util::codec::LinesCodec) -> Self {
        LinesCodec(value)
    }
}

fn to_io_error(err: LinesCodecError) -> Error {
    match err {
        tokio_util::codec::LinesCodecError::MaxLineLengthExceeded => {
            Error::new(ErrorKind::Other, "max line length exceeded")
        }
        tokio_util::codec::LinesCodecError::Io(err) => err,
    }
}

impl Decoder for LinesCodec {
    type Item = String;
    type Error = Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.0.decode(src).map_err(to_io_error)
    }
}

impl<Item> Encoder<Item> for LinesCodec
where
    Item: AsRef<[u8]>,
{
    type Error = Error;

    fn encode(&mut self, item: Item, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        self.0
            .encode(
                std::str::from_utf8(item.as_ref()).map_err(|_| {
                    Error::new(ErrorKind::Unsupported, "only utf8 strings supported")
                })?,
                dst,
            )
            .map_err(to_io_error)
    }
}
