use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::net::TcpStream;

pub type FramedStream = Framed<TcpStream, LengthDelimitedCodec>;

pub type TcpSender = SplitSink<FramedStream, Bytes>;
pub type TcpReceiver = SplitStream<FramedStream>;

pub fn framed_stream(stream: TcpStream, max_frame_length: Option<usize>) -> FramedStream {
    let mut codec = LengthDelimitedCodec::new();
    if let Some(len) = max_frame_length {
        codec.set_max_frame_length(len);
    }

    Framed::new(stream, codec)
}
