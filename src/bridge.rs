use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;

use crate::config::DynError;

const BUFFER_SIZE: usize = 16 * 1024;
const OUTBOUND_QUEUE_SIZE: usize = 32;

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
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Message>(OUTBOUND_QUEUE_SIZE);

    let writer = tokio::spawn(async move {
        while let Some(message) = outbound_rx.recv().await {
            let is_close = matches!(message, Message::Close(_));
            ws_sink.send(message).await?;
            if is_close {
                break;
            }
        }
        Ok::<(), DynError>(())
    });

    let tcp_tx = outbound_tx.clone();
    let tcp_to_ws = async move {
        let mut buf = vec![0_u8; BUFFER_SIZE];
        loop {
            let n = io_reader.read(&mut buf).await?;
            if n == 0 {
                let _ = tcp_tx.send(Message::Close(None)).await;
                break;
            }
            tcp_tx.send(Message::Binary(buf[..n].to_vec())).await?;
        }
        Ok::<(), DynError>(())
    };

    let control_tx = outbound_tx.clone();
    let ws_to_tcp = async move {
        while let Some(msg) = ws_stream.next().await {
            match msg? {
                Message::Binary(data) => {
                    io_writer.write_all(&data).await?;
                }
                Message::Ping(data) => {
                    let _ = control_tx.send(Message::Pong(data)).await;
                }
                Message::Close(_) => {
                    io_writer.shutdown().await?;
                    break;
                }
                Message::Pong(_) => {}
                Message::Text(_) => {}
                Message::Frame(_) => {}
            }
        }
        Ok::<(), DynError>(())
    };

    let heartbeat_tx = outbound_tx.clone();
    let heartbeat = async move {
        if let Some(heartbeat_interval) = heartbeat_interval {
            let mut ticker = interval(heartbeat_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                heartbeat_tx.send(Message::Ping(Vec::new().into())).await?;
            }
        } else {
            futures_util::future::pending::<()>().await;
            Ok::<(), DynError>(())
        }
    };

    let result = tokio::select! {
        result = tcp_to_ws => result,
        result = ws_to_tcp => result,
        result = heartbeat => result,
    };

    drop(outbound_tx);
    let writer_result = writer.await??;
    result?;
    writer_result
}
