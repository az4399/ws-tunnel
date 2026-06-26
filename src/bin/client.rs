use std::sync::atomic::{AtomicUsize, Ordering};
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

struct WorkerState {
    next_worker_id: AtomicUsize,
    active_workers: AtomicUsize,
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    ensure_rustls_crypto_provider();

    let config_path = match resolve_config_arg("client", "client.toml", "examples/client.toml") {
        CliAction::RunWithConfig(path) => path,
        CliAction::ExitAfterHelp => return Ok(()),
    };

    let cfg = Arc::new(load_client_config(&config_path)?);
    let state = Arc::new(WorkerState {
        next_worker_id: AtomicUsize::new(0),
        active_workers: AtomicUsize::new(0),
    });

    tokio::select! {
        _ = run_control_loop(cfg, state) => {}
        result = tokio::signal::ctrl_c() => {
            result?;
            eprintln!("client received Ctrl+C, shutting down");
        }
    }

    Ok(())
}

async fn run_control_loop(cfg: Arc<ClientConfig>, state: Arc<WorkerState>) {
    loop {
        if let Err(err) = run_control_once(cfg.clone(), state.clone()).await {
            eprintln!(
                "control connection reconnecting after error: {err}; retrying in {}s",
                cfg.reconnect_delay_secs
            );
        }

        sleep(Duration::from_secs(cfg.reconnect_delay_secs)).await;
    }
}

async fn run_control_once(cfg: Arc<ClientConfig>, state: Arc<WorkerState>) -> Result<(), DynError> {
    let (mut ws, _) = connect_websocket(&cfg).await?;
    let dial_target = cfg
        .connect_host
        .clone()
        .or_else(|| host_from_server_url(&cfg.server_url).ok())
        .unwrap_or_else(|| "unknown".to_string());

    ws.send(protocol::control(&cfg.token, cfg.remote_port))
        .await?;
    eprintln!(
        "control connection established to {} using {} for remote port {}",
        cfg.server_url, dial_target, cfg.remote_port
    );

    let heartbeat_interval = heartbeat_duration(cfg.heartbeat_interval_secs);
    loop {
        match wait_for_control_command(&mut ws, heartbeat_interval).await? {
            ControlCommand::Connect => {
                if spawn_data_worker(cfg.clone(), state.clone()).is_none() {
                    eprintln!(
                        "worker cap {} reached; waiting before accepting more remote connections",
                        cfg.max_total_workers.max(1)
                    );
                }
            }
            ControlCommand::Ignore => {}
        }
    }
}

fn spawn_data_worker(
    cfg: Arc<ClientConfig>,
    state: Arc<WorkerState>,
) -> Option<tokio::task::JoinHandle<()>> {
    let worker_id = reserve_worker_slot(&state, cfg.max_total_workers.max(1))?;
    Some(tokio::spawn(async move {
        if let Err(err) = run_data_worker(worker_id, cfg, state.clone()).await {
            eprintln!("worker {worker_id} ended after error: {err}");
        }
        let remaining = state.active_workers.fetch_sub(1, Ordering::SeqCst) - 1;
        eprintln!("worker {worker_id} stopped; active workers now {remaining}");
    }))
}

async fn run_data_worker(
    worker_id: usize,
    cfg: Arc<ClientConfig>,
    state: Arc<WorkerState>,
) -> Result<(), DynError> {
    let (mut ws, _) = connect_websocket(&cfg).await?;
    ws.send(protocol::hello(&cfg.token, cfg.remote_port))
        .await?;

    wait_for_start(&mut ws).await?;

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
            return Err(format!(
                "timed out opening local target {}; check whether the local service is reachable",
                cfg.local_addr
            )
            .into());
        }
    };
    tcp.set_nodelay(true)?;

    let active = state.active_workers.load(Ordering::SeqCst);
    eprintln!(
        "worker {worker_id} bound remote port {} to local {}; active workers now {active}",
        cfg.remote_port, cfg.local_addr
    );

    ws.send(protocol::ok()).await?;
    bridge_ws_and_tcp(ws, tcp, heartbeat_duration(cfg.heartbeat_interval_secs)).await
}

enum ControlCommand {
    Connect,
    Ignore,
}

async fn wait_for_control_command(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
    heartbeat_interval: Option<Duration>,
) -> Result<ControlCommand, DynError> {
    if let Some(heartbeat_interval) = heartbeat_interval {
        tokio::select! {
            incoming = ws.next() => handle_control_message(incoming),
            _ = sleep(heartbeat_interval) => {
                ws.send(Message::Ping(Vec::new().into())).await?;
                Ok(ControlCommand::Ignore)
            }
        }
    } else {
        handle_control_message(ws.next().await)
    }
}

fn handle_control_message(
    message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
) -> Result<ControlCommand, DynError> {
    match message {
        Some(Ok(Message::Text(text))) if text == protocol::CMD_CONNECT => {
            Ok(ControlCommand::Connect)
        }
        Some(Ok(Message::Text(text))) => {
            if let Some(err_text) = protocol::parse_err(&text) {
                return Err(format!("server rejected control connection: {err_text}").into());
            }
            Err(format!("unexpected control command from server: {text}").into())
        }
        Some(Ok(Message::Ping(_))) => Ok(ControlCommand::Ignore),
        Some(Ok(Message::Pong(_))) => Ok(ControlCommand::Ignore),
        Some(Ok(Message::Close(_))) => Err("server closed control connection".into()),
        Some(Ok(_)) => Err("unexpected non-text control command from server".into()),
        Some(Err(err)) => Err(err.into()),
        None => Err("server closed control connection".into()),
    }
}

async fn wait_for_start(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
) -> Result<(), DynError> {
    match ws.next().await {
        Some(Ok(Message::Text(text))) if text == protocol::CMD_START => Ok(()),
        Some(Ok(Message::Text(text))) => {
            if let Some(err_text) = protocol::parse_err(&text) {
                return Err(format!("server rejected worker: {err_text}").into());
            }
            Err(format!("unexpected text command from server: {text}").into())
        }
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
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
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
    .await
    .map_err(|_| {
        format!(
            "timed out connecting to websocket endpoint {} via {}:{}; check whether the server, reverse proxy, or firewall is reachable",
            cfg.server_url, host, port
        )
    })?
    .map_err(|err| {
        format!(
            "failed to connect to websocket endpoint {} via {}:{}: {}; check whether the server, reverse proxy, or firewall is reachable",
            cfg.server_url, host, port, err
        )
    })?;
    tcp.set_nodelay(true)?;

    let connected = timeout(
        Duration::from_secs(cfg.connect_timeout_secs),
        client_async_tls_with_config(request, tcp, None, None),
    )
    .await
    .map_err(|_| {
        format!(
            "timed out completing websocket handshake for {}; check the server path, reverse proxy, and TLS settings",
            cfg.server_url
        )
    })?
    .map_err(|err| describe_handshake_error(&cfg.server_url, err))?;

    Ok(connected)
}

fn heartbeat_duration(secs: u64) -> Option<Duration> {
    if secs == 0 {
        None
    } else {
        Some(Duration::from_secs(secs))
    }
}

fn reserve_worker_slot(state: &WorkerState, max_total_workers: usize) -> Option<usize> {
    loop {
        let total = state.active_workers.load(Ordering::SeqCst);
        if total >= max_total_workers {
            return None;
        }

        if state
            .active_workers
            .compare_exchange(total, total + 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Some(state.next_worker_id.fetch_add(1, Ordering::SeqCst));
        }
    }
}

fn host_from_server_url(server_url: &str) -> Result<String, DynError> {
    let uri: Uri = server_url.parse()?;
    uri.host()
        .map(str::to_string)
        .ok_or_else(|| "server_url is missing a host".into())
}

fn describe_handshake_error(
    server_url: &str,
    err: tokio_tungstenite::tungstenite::Error,
) -> DynError {
    match err {
        tokio_tungstenite::tungstenite::Error::Http(response) => {
            let status = response.status();
            if status == http::StatusCode::NOT_FOUND {
                format!(
                    "websocket handshake for {} returned 404 Not Found; check that the server path matches exactly",
                    server_url
                )
                .into()
            } else {
                format!(
                    "websocket handshake for {} failed with HTTP {}; check the reverse proxy, path, and authentication rules",
                    server_url, status
                )
                .into()
            }
        }
        other => format!(
            "failed to complete websocket handshake for {}: {}; check the server path, reverse proxy, and TLS settings",
            server_url, other
        )
        .into(),
    }
}
