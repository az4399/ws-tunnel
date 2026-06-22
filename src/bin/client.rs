use std::env;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use ws_tunnel::bridge::bridge_ws_and_tcp;
use ws_tunnel::config::{load_client_config, ClientConfig, DynError};
use ws_tunnel::protocol;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/client.toml".to_string());

    let cfg = Arc::new(load_client_config(&config_path).await?);
    let mut handles = Vec::with_capacity(cfg.worker_pool_size);

    for worker_id in 0..cfg.worker_pool_size {
        let cfg = cfg.clone();
        handles.push(tokio::spawn(async move {
            run_worker_loop(worker_id, cfg).await;
        }));
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            eprintln!("client received Ctrl+C, shutting down");
        }
        _ = async {
            for handle in handles {
                let _ = handle.await;
            }
        } => {}
    }

    Ok(())
}

async fn run_worker_loop(worker_id: usize, cfg: Arc<ClientConfig>) {
    loop {
        if let Err(err) = run_worker_once(worker_id, cfg.clone()).await {
            eprintln!("worker {worker_id} reconnecting after error: {err}");
        }
        sleep(Duration::from_secs(cfg.reconnect_delay_secs)).await;
    }
}

async fn run_worker_once(worker_id: usize, cfg: Arc<ClientConfig>) -> Result<(), DynError> {
    let (mut ws, _) = connect_async(cfg.server_url.as_str()).await?;
    ws.send(protocol::hello(&cfg.token, cfg.remote_port)).await?;

    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) if text == protocol::CMD_START => break,
            Some(Ok(Message::Text(text))) => {
                if let Some(err_text) = protocol::parse_err(&text) {
                    return Err(format!("server rejected worker {worker_id}: {err_text}").into());
                }
                return Err(format!("unexpected text command for worker {worker_id}: {text}").into());
            }
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Close(_))) => {
                return Err(format!("server closed worker {worker_id} before START").into());
            }
            Some(Ok(_)) => {
                return Err(format!("unexpected non-text command for worker {worker_id}").into());
            }
            Some(Err(err)) => return Err(err.into()),
            None => return Err(format!("server closed worker {worker_id}").into()),
        }
    }

    let tcp = match timeout(
        Duration::from_secs(cfg.connect_timeout_secs),
        TcpStream::connect(&cfg.local_addr),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        Ok(Err(err)) => {
            let _ = ws.send(protocol::err(&err.to_string())).await;
            return Err(err.into());
        }
        Err(_) => {
            let _ = ws.send(protocol::err("local connect timeout")).await;
            return Err("local connect timeout".into());
        }
    };

    ws.send(protocol::ok()).await?;
    bridge_ws_and_tcp(ws, tcp).await
}
