use tokio_tungstenite::tungstenite::Message;

pub const CMD_START: &str = "START";
pub const CMD_OK: &str = "OK";
pub const CMD_HELLO_PREFIX: &str = "HELLO ";
pub const CMD_CONTROL_PREFIX: &str = "CONTROL ";
pub const CMD_ERR_PREFIX: &str = "ERR ";
pub const CMD_CONNECT: &str = "CONNECT";

pub fn hello(token: &str, remote_port: u16) -> Message {
    Message::Text(format!("{CMD_HELLO_PREFIX}{token} {remote_port}"))
}

pub fn parse_hello(text: &str) -> Option<(&str, u16)> {
    let body = text.strip_prefix(CMD_HELLO_PREFIX)?;
    let mut parts = body.split_ascii_whitespace();
    let token = parts.next()?;
    let remote_port = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((token, remote_port))
}

pub fn control(token: &str, remote_port: u16) -> Message {
    Message::Text(format!("{CMD_CONTROL_PREFIX}{token} {remote_port}"))
}

pub fn parse_control(text: &str) -> Option<(&str, u16)> {
    let body = text.strip_prefix(CMD_CONTROL_PREFIX)?;
    let mut parts = body.split_ascii_whitespace();
    let token = parts.next()?;
    let remote_port = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((token, remote_port))
}

pub fn start() -> Message {
    Message::Text(CMD_START.to_string())
}

pub fn connect() -> Message {
    Message::Text(CMD_CONNECT.to_string())
}

pub fn ok() -> Message {
    Message::Text(CMD_OK.to_string())
}

pub fn err(message: &str) -> Message {
    Message::Text(format!("{CMD_ERR_PREFIX}{message}"))
}

pub fn parse_err(text: &str) -> Option<&str> {
    text.strip_prefix(CMD_ERR_PREFIX)
}
