use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use http::{Response, StatusCode};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::ErrorResponse;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response as WsResponse};
use tokio_tungstenite::tungstenite::Message;
use ws_tunnel::bridge::bridge_ws_and_tcp;
use ws_tunnel::cli::{resolve_config_arg, CliAction};
use ws_tunnel::config::{load_server_config, DynError, ServerConfig};
use ws_tunnel::protocol;

struct WorkerHandle {
    assign: oneshot::Sender<TcpStream>,
}

struct MappingState {
    idle_tx: mpsc::Sender<WorkerHandle>,
    connect_tx: broadcast::Sender<()>,
}

struct ServerState {
    cfg: Arc<ServerConfig>,
    mappings: Mutex<HashMap<u16, Arc<MappingState>>>,
    creation_lock: Mutex<()>,
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let config_path = match resolve_config_arg("server", "server.toml", "examples/server.toml") {
        CliAction::RunWithConfig(path) => path,
        CliAction::ExitAfterHelp => return Ok(()),
    };

    let cfg = Arc::new(load_server_config(&config_path)?);
    let state = Arc::new(ServerState {
        cfg,
        mappings: Mutex::new(HashMap::new()),
        creation_lock: Mutex::new(()),
    });

    tokio::select! {
        result = run_ws_listener(state) => result?,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("server received Ctrl+C, shutting down");
        }
    }

    Ok(())
}

async fn run_ws_listener(state: Arc<ServerState>) -> Result<(), DynError> {
    let listener = TcpListener::bind(&state.cfg.ws_bind).await?;
    eprintln!("ws server listening on {}", state.cfg.ws_bind);

    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                eprintln!("temporary accept error on {}: {err}", state.cfg.ws_bind);
                continue;
            }
        };
        if let Err(err) = stream.set_nodelay(true) {
            eprintln!("failed to enable TCP_NODELAY for worker session from {addr}: {err}");
        }
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_worker(stream, state).await {
                eprintln!(
                    "worker session from {addr} ended: {}",
                    describe_worker_error(&err)
                );
            }
        });
    }
}

async fn dispatch_tcp_to_workers(
    mut tcp_rx: mpsc::Receiver<TcpStream>,
    mut idle_rx: mpsc::Receiver<WorkerHandle>,
    connect_tx: broadcast::Sender<()>,
) -> Result<(), DynError> {
    while let Some(stream) = tcp_rx.recv().await {
        let mut pending = Some(stream);
        loop {
            match idle_rx.try_recv() {
                Ok(worker) => {
                    let stream = pending.take().expect("pending tcp stream missing");
                    match worker.assign.send(stream) {
                        Ok(()) => break,
                        Err(stream) => {
                            pending = Some(stream);
                            continue;
                        }
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err("worker dispatcher lost its idle worker queue".into());
                }
            }

            if connect_tx.send(()).is_err() {
                eprintln!("no control connection is ready; waiting for client to reconnect");
            }

            match timeout(Duration::from_secs(5), idle_rx.recv()).await {
                Ok(Some(worker)) => {
                    let stream = pending.take().expect("pending tcp stream missing");
                    match worker.assign.send(stream) {
                        Ok(()) => break,
                        Err(stream) => {
                            pending = Some(stream);
                            continue;
                        }
                    }
                }
                Ok(None) => {
                    return Err("worker dispatcher lost its idle worker queue".into());
                }
                Err(_) => {
                    eprintln!(
                        "still waiting for an on-demand worker; retrying control notification"
                    );
                }
            }
        }
    }
    Ok(())
}

async fn handle_worker(stream: TcpStream, state: Arc<ServerState>) -> Result<(), DynError> {
    let expected_path = state.cfg.path.clone();
    let ws = accept_hdr_async(stream, move |req: &Request, response: WsResponse| {
        validate_path(req, response, &expected_path)
    })
    .await?;

    let mut ws = ws;
    let first = timeout(
        Duration::from_secs(state.cfg.handshake_timeout_secs),
        ws.next(),
    )
    .await?;

    let text = match first {
        Some(Ok(Message::Text(text))) => text,
        Some(Ok(_)) => return Err("first worker frame must be text HELLO".into()),
        Some(Err(err)) => return Err(err.into()),
        None => return Err("worker closed before authentication frame".into()),
    };

    if let Some((token, remote_port)) = protocol::parse_control(&text) {
        return handle_control(ws, state, token, remote_port).await;
    }

    let (token, remote_port) = protocol::parse_hello(&text)
        .map(|(token, remote_port)| (token.to_string(), remote_port))
        .ok_or_else(|| "invalid HELLO frame".to_string())?;

    if token != state.cfg.token {
        let _ = ws.send(protocol::err("invalid token")).await;
        return Err("worker token mismatch".into());
    }

    let mapping = ensure_mapping(state.clone(), remote_port).await?;
    eprintln!("worker authenticated and waiting for remote port {remote_port}");
    let (assign, assigned_tcp) = oneshot::channel();
    mapping.idle_tx.send(WorkerHandle { assign }).await?;

    let tcp_stream = wait_for_assignment(&mut ws, assigned_tcp).await?;
    ws.send(protocol::start()).await?;

    let ready = timeout(
        Duration::from_secs(state.cfg.handshake_timeout_secs),
        ws.next(),
    )
    .await?;

    match ready {
        Some(Ok(Message::Text(text))) if text == protocol::CMD_OK => {}
        Some(Ok(Message::Text(text))) => {
            if let Some(err_text) = protocol::parse_err(&text) {
                return Err(format!("client failed to open local target: {err_text}").into());
            }
            return Err(format!("unexpected worker response: {text}").into());
        }
        Some(Ok(_)) => return Err("unexpected non-text worker response".into()),
        Some(Err(err)) => return Err(err.into()),
        None => return Err("worker closed before START confirmation".into()),
    }

    bridge_ws_and_tcp(ws, tcp_stream, None).await
}

async fn handle_control(
    mut ws: tokio_tungstenite::WebSocketStream<TcpStream>,
    state: Arc<ServerState>,
    token: &str,
    remote_port: u16,
) -> Result<(), DynError> {
    if token != state.cfg.token {
        let _ = ws.send(protocol::err("invalid token")).await;
        return Err("control token mismatch".into());
    }

    let mapping = ensure_mapping(state, remote_port).await?;
    let mut connect_rx = mapping.connect_tx.subscribe();
    eprintln!("control connection authenticated for remote port {remote_port}");

    loop {
        tokio::select! {
            signal = connect_rx.recv() => {
                match signal {
                    Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => {
                        ws.send(protocol::connect()).await?;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err("control notification channel closed".into());
                    }
                }
            }
            incoming = ws.next() => {
                match incoming {
                    Some(Ok(Message::Ping(data))) => {
                        ws.send(Message::Pong(data)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => return Err("control connection closed".into()),
                    Some(Ok(Message::Text(text))) => {
                        return Err(format!("unexpected control text frame: {text}").into());
                    }
                    Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {
                        return Err("unexpected control data frame".into());
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => return Err("control connection disconnected".into()),
                }
            }
        }
    }
}

async fn ensure_mapping(
    state: Arc<ServerState>,
    remote_port: u16,
) -> Result<Arc<MappingState>, DynError> {
    if let Some(existing) = state.mappings.lock().await.get(&remote_port).cloned() {
        return Ok(existing);
    }

    let _creation_guard = state.creation_lock.lock().await;
    if let Some(existing) = state.mappings.lock().await.get(&remote_port).cloned() {
        return Ok(existing);
    }

    let listener_addr = format!("{}:{remote_port}", state.cfg.tcp_bind_addr);
    let listener = TcpListener::bind(&listener_addr).await?;
    eprintln!("opened remote tcp listener on {listener_addr}");

    let (idle_tx, idle_rx) = mpsc::channel::<WorkerHandle>(state.cfg.idle_worker_backlog);
    let (tcp_tx, tcp_rx) = mpsc::channel::<TcpStream>(state.cfg.pending_tcp_backlog);

    let (connect_tx, _) = broadcast::channel::<()>(state.cfg.pending_tcp_backlog);
    let mapping = Arc::new(MappingState {
        idle_tx,
        connect_tx,
    });

    {
        let mut guard = state.mappings.lock().await;
        if let Some(existing) = guard.get(&remote_port).cloned() {
            return Ok(existing);
        }
        guard.insert(remote_port, mapping.clone());
    }

    tokio::spawn(async move {
        if let Err(err) = run_dynamic_tcp_listener(listener, remote_port, tcp_tx).await {
            eprintln!("tcp listener for port {remote_port} stopped: {err}");
        }
    });

    tokio::spawn(async move {
        if let Err(err) = dispatch_tcp_to_workers(tcp_rx, idle_rx, mapping.connect_tx.clone()).await
        {
            eprintln!("dispatcher for port {remote_port} stopped: {err}");
        }
    });

    Ok(state
        .mappings
        .lock()
        .await
        .get(&remote_port)
        .cloned()
        .expect("mapping missing after insert"))
}

async fn run_dynamic_tcp_listener(
    listener: TcpListener,
    remote_port: u16,
    tcp_tx: mpsc::Sender<TcpStream>,
) -> Result<(), DynError> {
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                eprintln!("temporary TCP accept error on remote port {remote_port}: {err}");
                continue;
            }
        };
        if let Err(err) = stream.set_nodelay(true) {
            eprintln!("failed to enable TCP_NODELAY for remote client {addr} on port {remote_port}: {err}");
        }
        if tcp_tx.send(stream).await.is_err() {
            eprintln!("dispatcher closed for port {remote_port}, dropping connection from {addr}");
        }
    }
}

async fn wait_for_assignment(
    ws: &mut tokio_tungstenite::WebSocketStream<TcpStream>,
    assigned_tcp: oneshot::Receiver<TcpStream>,
) -> Result<TcpStream, DynError> {
    tokio::pin!(assigned_tcp);

    loop {
        tokio::select! {
            tcp = &mut assigned_tcp => return Ok(tcp?),
            incoming = ws.next() => {
                match incoming {
                    Some(Ok(Message::Ping(data))) => {
                        ws.send(Message::Pong(data)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => return Err("worker closed before assignment".into()),
                    Some(Ok(Message::Text(text))) => {
                        return Err(format!("unexpected text command before assignment: {text}").into());
                    }
                    Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {
                        return Err("unexpected binary frame before assignment".into());
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => return Err("worker disconnected before assignment".into()),
                }
            }
        }
    }
}

fn validate_path(
    request: &Request,
    response: WsResponse,
    expected_path: &str,
) -> Result<WsResponse, ErrorResponse> {
    if request.uri().path() == expected_path {
        return Ok(response);
    }

    eprintln!(
        "rejected websocket request path {:?}, expected {:?}",
        request.uri().path(),
        expected_path
    );

    let response = Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Some("not found".to_string()))
        .expect("failed to build websocket rejection response");
    Err(response)
}

fn describe_worker_error(err: &DynError) -> String {
    let text = err.to_string();
    if text.contains("Connection reset without closing handshake") {
        return "websocket peer reset the connection without a closing handshake; this usually means the reverse proxy or network dropped an idle or unstable connection".to_string();
    }
    if text.contains("Connection reset by peer") {
        return "tcp peer reset the connection; this usually means the client, reverse proxy, or network closed the socket abruptly".to_string();
    }
    if text.contains("invalid token") || text.contains("token mismatch") {
        return "worker authentication failed because the provided token did not match the server token".to_string();
    }
    if text.contains("worker closed before")
        || text.contains("first worker frame must be text HELLO")
    {
        return "worker disconnected before sending a valid authentication frame".to_string();
    }
    text
}
