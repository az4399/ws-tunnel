use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{interval, Duration, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;

use crate::config::DynError;

const BUFFER_SIZE: usize = 16 * 1024;

pub async fn bridge_ws_and_tcp<Ws, Io>(
    ws: Ws,
    io: Io,
    heartbeat_interval: Option<Duration>,
) -> Result<(), DynError>
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
    let mut buf = vec![0_u8; BUFFER_SIZE];
    let mut heartbeat = heartbeat_interval.map(interval);
    if let Some(ticker) = heartbeat.as_mut() {
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await;
    }

    loop {
        tokio::select! {
            read = io_reader.read(&mut buf) => {
                let n = read?;
                if n == 0 {
                    let _ = ws_sink.send(Message::Close(None)).await;
                    break;
                }
                ws_sink.send(Message::Binary(buf[..n].to_vec().into())).await?;
            }
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(Message::Binary(data))) => {
                        io_writer.write_all(&data).await?;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        ws_sink.send(Message::Pong(data)).await?;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let _ = ws_sink.send(Message::Close(frame)).await;
                        io_writer.shutdown().await?;
                        break;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Text(_))) => {}
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        io_writer.shutdown().await?;
                        break;
                    }
                }
            }
            _ = async {
                if let Some(ticker) = heartbeat.as_mut() {
                    ticker.tick().await;
                }
            }, if heartbeat.is_some() => {
                ws_sink.send(Message::Ping(Vec::new().into())).await?;
            }
        }
    }

    Ok(())
}
