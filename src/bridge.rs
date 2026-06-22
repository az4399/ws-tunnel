use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

use crate::config::DynError;

const BUFFER_SIZE: usize = 16 * 1024;

pub async fn bridge_ws_and_tcp<Ws, Io>(ws: Ws, io: Io) -> Result<(), DynError>
where
    Ws: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin
        + Send
        + 'static,
    Io: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut ws_sink, mut ws_stream) = ws.split();
    let (mut io_reader, mut io_writer) = tokio::io::split(io);

    let tcp_to_ws = async move {
        let mut buf = vec![0_u8; BUFFER_SIZE];
        loop {
            let n = io_reader.read(&mut buf).await?;
            if n == 0 {
                let _ = ws_sink.send(Message::Close(None)).await;
                break;
            }
            ws_sink.send(Message::Binary(buf[..n].to_vec())).await?;
        }
        Ok::<(), DynError>(())
    };

    let ws_to_tcp = async move {
        while let Some(msg) = ws_stream.next().await {
            match msg? {
                Message::Binary(data) => {
                    io_writer.write_all(&data).await?;
                }
                Message::Close(_) => {
                    io_writer.shutdown().await?;
                    break;
                }
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Text(_) => {}
                Message::Frame(_) => {}
            }
        }
        Ok::<(), DynError>(())
    };

    tokio::select! {
        result = tcp_to_ws => result,
        result = ws_to_tcp => result,
    }
}

