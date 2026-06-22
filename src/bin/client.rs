use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

struct WorkerPoolState {
    next_worker_id: AtomicUsize,
    total_workers: AtomicUsize,
    idle_workers: AtomicUsize,
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    ensure_rustls_crypto_provider();

    let config_path = match resolve_config_arg("client", "client.toml", "examples/client.toml") {
        CliAction::RunWithConfig(path) => path,
        CliAction::ExitAfterHelp => return Ok(()),
    };

    let cfg = Arc::new(load_client_config(&config_path)?);
    let max_total_workers = cfg.max_total_workers.max(1);
    let idle_target = cfg.worker_pool_size.max(1).min(max_total_workers);
    if cfg.worker_pool_size.max(1) > max_total_workers {
        eprintln!(
            "worker_pool_size {} is higher than max_total_workers {}; idle pool will be capped at {}",
            cfg.worker_pool_size.max(1),
            max_total_workers,
            idle_target
        );
    }
    let pool = Arc::new(WorkerPoolState {
        next_worker_id: AtomicUsize::new(0),
        total_workers: AtomicUsize::new(0),
        idle_workers: AtomicUsize::new(0),
    });

    for _ in 0..idle_target {
        let _ = spawn_worker(cfg.clone(), pool.clone());
    }

    tokio::signal::ctrl_c().await?;
    eprintln!("client received Ctrl+C, shutting down");

    Ok(())
}

fn spawn_worker(
    cfg: Arc<ClientConfig>,
    pool: Arc<WorkerPoolState>,
) -> Option<tokio::task::JoinHandle<()>> {
    let worker_id = reserve_worker_slot(&pool, cfg.max_total_workers.max(1))?;
    Some(tokio::spawn(async move {
        run_worker_loop(worker_id, cfg, pool.clone()).await;
        let remaining = pool.total_workers.fetch_sub(1, Ordering::SeqCst) - 1;
        eprintln!("worker {worker_id} stopped; total workers now {remaining}");
    }))
}

async fn run_worker_loop(worker_id: usize, cfg: Arc<ClientConfig>, pool: Arc<WorkerPoolState>) {
    loop {
        if let Err(err) = run_worker_once(worker_id, cfg.clone(), pool.clone()).await {
            eprintln!(
                "worker {worker_id} reconnecting after error: {err}; retrying in {}s",
                cfg.reconnect_delay_secs
            );
        }

        if should_retire_worker(&cfg, &pool) {
            eprintln!("worker {worker_id} retired because idle capacity is already sufficient");
            return;
        }

        sleep(reconnect_delay(worker_id, cfg.reconnect_delay_secs)).await;
    }
}

async fn run_worker_once(
    worker_id: usize,
    cfg: Arc<ClientConfig>,
    pool: Arc<WorkerPoolState>,
) -> Result<(), DynError> {
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
    let mut counted_idle = false;
    mark_idle(&pool, &mut counted_idle, worker_id, cfg.remote_port);
    loop {
        match wait_for_start(&mut ws, heartbeat_interval).await {
            Ok(WorkerCommand::Start) => {
                unmark_idle(&pool, &mut counted_idle);
                ensure_idle_capacity(worker_id, cfg.clone(), pool.clone());
                break;
            }
            Ok(WorkerCommand::Ignore) => continue,
            Err(err) => {
                unmark_idle(&pool, &mut counted_idle);
                return Err(err);
            }
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
            return Err(format!(
                "timed out opening local target {}; check whether the local service is reachable",
                cfg.local_addr
            ).into());
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

fn reconnect_delay(worker_id: usize, base_secs: u64) -> Duration {
    let base = Duration::from_secs(base_secs);
    let jitter_ms = ((worker_id as u64) % 10) * 200;
    base + Duration::from_millis(jitter_ms)
}

fn reserve_worker_slot(pool: &WorkerPoolState, max_total_workers: usize) -> Option<usize> {
    loop {
        let total = pool.total_workers.load(Ordering::SeqCst);
        if total >= max_total_workers {
            return None;
        }

        if pool
            .total_workers
            .compare_exchange(total, total + 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Some(pool.next_worker_id.fetch_add(1, Ordering::SeqCst));
        }
    }
}

fn mark_idle(pool: &WorkerPoolState, counted_idle: &mut bool, worker_id: usize, remote_port: u16) {
    if !*counted_idle {
        let idle = pool.idle_workers.fetch_add(1, Ordering::SeqCst) + 1;
        *counted_idle = true;
        eprintln!(
            "worker {worker_id} is idle for remote port {remote_port}; idle workers now {idle}"
        );
    }
}

fn unmark_idle(pool: &WorkerPoolState, counted_idle: &mut bool) {
    if *counted_idle {
        pool.idle_workers.fetch_sub(1, Ordering::SeqCst);
        *counted_idle = false;
    }
}

fn ensure_idle_capacity(worker_id: usize, cfg: Arc<ClientConfig>, pool: Arc<WorkerPoolState>) {
    let idle_target = cfg.worker_pool_size.max(1).min(cfg.max_total_workers.max(1));
    let idle = pool.idle_workers.load(Ordering::SeqCst);
    let total = pool.total_workers.load(Ordering::SeqCst);

    if idle >= idle_target {
        return;
    }

    if let Some(handle) = spawn_worker(cfg.clone(), pool.clone()) {
        drop(handle);
        eprintln!(
            "worker {worker_id} became busy; spawning replacement worker (idle {idle}, total {total})"
        );
    } else if total < idle_target {
        eprintln!(
            "worker {worker_id} became busy, but worker cap {} prevents keeping {} idle workers ready",
            cfg.max_total_workers.max(1),
            idle_target
        );
    }
}

fn should_retire_worker(cfg: &ClientConfig, pool: &WorkerPoolState) -> bool {
    let idle_target = cfg.worker_pool_size.max(1).min(cfg.max_total_workers.max(1));
    let idle = pool.idle_workers.load(Ordering::SeqCst);
    let total = pool.total_workers.load(Ordering::SeqCst);
    total > idle_target && idle >= idle_target
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
