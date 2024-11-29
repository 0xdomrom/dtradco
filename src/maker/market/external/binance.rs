use anyhow::{Error, Result};
use bigdecimal::BigDecimal;
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::fmt::Debug;
use std::future::Future;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::{connect_async, tungstenite, MaybeTlsStream};

pub const BINANCE_STREAMS_URL: &str = "wss://dstream.binance.com/stream?streams=";

pub fn get_binance_stream_url(symbols: Vec<String>) -> String {
    let channels: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@bookTicker", s.to_lowercase()))
        .collect();
    let streams_str = channels.join("/");
    format!("{}{}", BINANCE_STREAMS_URL, streams_str)
}

async fn connect_with_retry(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let mut backoff = tokio::time::Duration::from_secs(1);
    let max_wait = tokio::time::Duration::from_secs(32);
    loop {
        let conn_res = connect_async(url).await;
        if let Ok((socket, _)) = conn_res {
            return Ok(socket);
        } else {
            tracing::error!("Error connecting to socket");
            backoff *= 2;
            backoff = std::cmp::min(backoff, max_wait);
            tokio::time::sleep(backoff).await;
        }
    }
}

pub async fn subscribe<Fut, Data>(
    stream_url: &String,
    mut handler: impl FnMut(Data) -> Fut,
) -> Result<()>
where
    Fut: Future<Output = Result<()>>,
    Data: for<'de> Deserialize<'de> + Debug,
{
    let poll_time = tokio::time::Duration::from_micros(10);
    let mut socket = connect_with_retry(stream_url).await?;
    tracing::info!("Connected to websocket: {}", stream_url);
    loop {
        let msg = socket.next().await;
        if let Some(Ok(msg)) = msg {
            let msg_text = msg.to_text()?;
            let mut data = serde_json::from_str::<Data>(msg_text);
            match data {
                Ok(data) => handler(data).await?,
                Err(e) => tracing::error!("Error deserializing data: {:?}", e),
            };
        } else {
            tracing::error!("Error reading message from socket, reconnecting");
            let close_res = socket.close(None).await;
            socket = connect_with_retry(stream_url).await?;
        }
    }
}

/*
{"stream":"<streamName>","data":<rawPayload>}

rawPayload:
{
  "e":"bookTicker",         // event type
  "u":400900217,            // order book updateId
  "E": 1568014460893,       // event time
  "T": 1568014460891,       // transaction time
  "s":"BNBUSDT",            // symbol
  "b":"25.35190000",        // best bid price
  "B":"31.21000000",        // best bid qty
  "a":"25.36520000",        // best ask price
  "A":"40.66000000"         // best ask qty
}
*/

#[derive(Debug, Deserialize, Clone)]
pub struct BookTickerData {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub best_bid_price: BigDecimal,
    #[serde(rename = "a")]
    pub best_ask_price: BigDecimal,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CombinedBookTickerData {
    // pub stream: String, // not needed for now (until we sub to more stream types)
    pub data: BookTickerData,
}
