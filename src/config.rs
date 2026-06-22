use serde::Deserialize;
use tokio::fs;

pub type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    pub ws_bind: String,
    #[serde(default = "default_tcp_bind_addr")]
    pub tcp_bind_addr: String,
    pub path: String,
    pub token: String,
    #[serde(default = "default_backlog")]
    pub pending_tcp_backlog: usize,
    #[serde(default = "default_backlog")]
    pub idle_worker_backlog: usize,
    #[serde(default = "default_handshake_timeout")]
    pub handshake_timeout_secs: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClientConfig {
    pub server_url: String,
    pub token: String,
    pub remote_port: u16,
    pub local_addr: String,
    #[serde(default = "default_worker_pool")]
    pub worker_pool_size: usize,
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_secs: u64,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
}

fn default_backlog() -> usize {
    128
}

fn default_worker_pool() -> usize {
    8
}

fn default_reconnect_delay() -> u64 {
    3
}

fn default_connect_timeout() -> u64 {
    10
}

fn default_handshake_timeout() -> u64 {
    10
}

fn default_tcp_bind_addr() -> String {
    "0.0.0.0".to_string()
}

pub async fn load_server_config(path: &str) -> Result<ServerConfig, DynError> {
    let text = fs::read_to_string(path).await?;
    let cfg: ServerConfig = toml::from_str(&text)?;
    Ok(cfg)
}

pub async fn load_client_config(path: &str) -> Result<ClientConfig, DynError> {
    let text = fs::read_to_string(path).await?;
    let cfg: ClientConfig = toml::from_str(&text)?;
    Ok(cfg)
}
