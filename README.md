# ws-tunnel

A small TCP-over-WebSocket tunnel built for one job:

- server listens on plain `ws`
- client connects with `ws` or `wss`
- client chooses which remote TCP port should be opened on the VPS
- client forwards traffic to a fixed local TCP address

This project intentionally does not implement generic reverse proxy features.

## Layout

- `server`: accepts worker WebSocket connections and opens requested TCP ports on demand
- `client`: keeps a pool of outbound worker connections and forwards them to a local TCP service

## Example

1. Run the server on your VPS:

```toml
# examples/server.toml
ws_bind = "0.0.0.0:8080"
tcp_bind_addr = "0.0.0.0"
path = "/tunnel"
token = "change-me"
pending_tcp_backlog = 128
idle_worker_backlog = 128
handshake_timeout_secs = 10
```

2. Put Cloudflare in front of `http://your-vps:8080` and proxy `/tunnel`.
   The server itself only speaks plain `ws`.
   The client can connect through Cloudflare with `wss://...`.

3. Run the client near your local service:

```toml
# examples/client.toml
server_url = "wss://tunnel.example.com/tunnel"
# Optional: dial this host or IP instead of resolving the host in server_url.
# Host header and TLS SNI still follow server_url.
# connect_host = "1.2.3.4"
token = "change-me"
remote_port = 7000
local_addr = "127.0.0.1:22"
worker_pool_size = 8
reconnect_delay_secs = 3
connect_timeout_secs = 10
heartbeat_interval_secs = 20
```

4. Connect to the VPS public port:

```text
tcp://your-vps:7000 -> wss://tunnel.example.com/tunnel -> 127.0.0.1:22
```

## Build

```bash
cargo build --release --bin server
cargo build --release --bin client
```

## Run

```bash
cargo run --release --bin server -- examples/server.toml
cargo run --release --bin client -- examples/client.toml
```

## Notes

- The server does not terminate TLS.
- `wss` is expected to be terminated by Cloudflare before reaching the VPS origin.
- The client decides `remote_port`, and the server opens that TCP listener on demand.
- You can keep `server_url` as a domain name and set `connect_host` to force the underlying TCP connection to a specific host or IP.
- `heartbeat_interval_secs` controls WebSocket ping keepalive. Set it to `0` to disable heartbeats.
- One incoming TCP connection consumes one idle worker WebSocket from the client pool.
- If all workers are busy, new TCP connections wait in the pending queue.
