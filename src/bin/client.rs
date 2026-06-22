use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use http::Uri;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::client_async_tls_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use ws_tunnel::bridge::bridge_ws_and_tcp;
use ws_tunnel::cli::{resolve_config_arg, CliAction};
use ws_tunnel::config::{load_client_config, ClientConfig, DynError};
use ws_tunnel::protocol;
use ws_tunnel::tls::ensure_rustls_crypto_provider;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    ensure_rustls_crypto_provider();

    let config_path = match resolve_config_arg("client", "client.toml", "examples/client.toml") {
        CliAction::RunWithConfig(path) => path,
        CliAction::ExitAfterHelp => return Ok(()),
    };

    let cfg = Arc::new(load_client_config(&config_path)?);
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
            eprintln!(
                "worker {worker_id} reconnecting after error: {err}; retrying in {}s",
                cfg.reconnect_delay_secs
            );
        }
        sleep(reconnect_delay(worker_id, cfg.reconnect_delay_secs)).await;
    }
}

async fn run_worker_once(worker_id: usize, cfg: Arc<ClientConfig>) -> Result<(), DynError> {
    let (mut ws, _) = connect_websocket(&cfg).await?;
    let dial_target = cfg
        .connect_host
        .clone()
        .or_else(|| host_from_server_url(&cfg.server_url).ok())
        .unwrap_or_else(|| "unknown".to_string());
    eprintln!(
        "worker {worker_id} connected to {} using {} for remote port {}",
        cfg.server_url, dial_target, cfg.remote_port
    );
    ws.send(protocol::hello(&cfg.token, cfg.remote_port)).await?;

    let heartbeat_interval = heartbeat_duration(cfg.heartbeat_interval_secs);
    loop {
        match wait_for_start(&mut ws, heartbeat_interval).await? {
            WorkerCommand::Start => break,
            WorkerCommand::Ignore => continue,
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
    tcp.set_nodelay(true)?;
    eprintln!(
        "worker {worker_id} bound remote port {} to local {}",
        cfg.remote_port, cfg.local_addr
    );

    ws.send(protocol::ok()).await?;
    bridge_ws_and_tcp(ws, tcp, heartbeat_interval).await
}

enum WorkerCommand {
    Start,
    Ignore,
}

async fn wait_for_start(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
    heartbeat_interval: Option<Duration>,
) -> Result<WorkerCommand, DynError> {
    if let Some(heartbeat_interval) = heartbeat_interval {
        tokio::select! {
            incoming = ws.next() => handle_worker_message(incoming),
            _ = sleep(heartbeat_interval) => {
                ws.send(Message::Ping(Vec::new().into())).await?;
                Ok(WorkerCommand::Ignore)
            }
        }
    } else {
        handle_worker_message(ws.next().await)
    }
}

fn handle_worker_message(
    message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
) -> Result<WorkerCommand, DynError> {
    match message {
        Some(Ok(Message::Text(text))) if text == protocol::CMD_START => Ok(WorkerCommand::Start),
        Some(Ok(Message::Text(text))) => {
            if let Some(err_text) = protocol::parse_err(&text) {
                return Err(format!("server rejected worker: {err_text}").into());
            }
            Err(format!("unexpected text command from server: {text}").into())
        }
        Some(Ok(Message::Ping(_))) => Ok(WorkerCommand::Ignore),
        Some(Ok(Message::Pong(_))) => Ok(WorkerCommand::Ignore),
        Some(Ok(Message::Close(_))) => Err("server closed worker before START".into()),
        Some(Ok(_)) => Err("unexpected non-text command from server".into()),
        Some(Err(err)) => Err(err.into()),
        None => Err("server closed worker".into()),
    }
}

async fn connect_websocket(
    cfg: &ClientConfig,
) -> Result<
    (
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<TcpStream>,
        >,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    ),
    DynError,
> {
    let uri: Uri = cfg.server_url.parse()?;
    let request = uri.clone().into_client_request()?;
    let host = cfg
        .connect_host
        .as_deref()
        .or_else(|| uri.host())
        .ok_or("server_url is missing a host")?;
    let port = uri
        .port_u16()
        .or_else(|| match uri.scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or("server_url has an unsupported scheme")?;

    let tcp = timeout(
        Duration::from_secs(cfg.connect_timeout_secs),
        TcpStream::connect((host, port)),
    )
    .await??;
    tcp.set_nodelay(true)?;

    let connected = timeout(
        Duration::from_secs(cfg.connect_timeout_secs),
        client_async_tls_with_config(request, tcp, None, None),
    )
    .await??;

    Ok(connected)
}

fn heartbeat_duration(secs: u64) -> Option<Duration> {
    if secs == 0 {
        None
    } else {
        Some(Duration::from_secs(secs))
    }
}

fn reconnect_delay(worker_id: usize, base_secs: u64) -> Duration {
    let base = Duration::from_secs(base_secs);
    let jitter_ms = ((worker_id as u64) % 10) * 200;
    base + Duration::from_millis(jitter_ms)
}

fn host_from_server_url(server_url: &str) -> Result<String, DynError> {
    let uri: Uri = server_url.parse()?;
    uri.host()
        .map(str::to_string)
        .ok_or_else(|| "server_url is missing a host".into())
}
