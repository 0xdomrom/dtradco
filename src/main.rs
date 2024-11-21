mod maker;

use dotenvy::dotenv;
use dotenvy_macro::dotenv;
use dtradco::run;
use lyra_client::json_rpc::WsClientExt;
use anyhow::Result;
use lyra_client::setup::ensure_env;
use serde_json::{json, Value};
use tracing::log::info;

pub async fn setup_env() {
    dotenvy::from_filename(".env").expect("Failed to load .env file");
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    info!("{}", std::env::var("ENV").unwrap());
    ensure_env().await;
    let env_name = std::env::var("ENV").unwrap();
    let env_consts = format!(".env.constants.{env_name}");
    let env_keys = format!(".env.keys.{env_name}");
    dotenvy::from_filename(env_consts).expect("Failed to load .env.constants.{} file");
    let key_loaded = dotenvy::from_filename(env_keys);
    if key_loaded.is_err() {
        info!("No keys file found for env, expecting them to be in AWS");
    }
}

#[tokio::main]
async fn main() -> Result<()>  {
    // let db_uri = format!("{}", dotenv!("DATABASE_URL"));
    // run(db_uri).await;
    setup_env().await;
    let client = lyra_client::json_rpc::WsClient::new_client().await?;
    let time_res = client.send_rpc::<Value, Value>("public/get_time", json!({})).await?;
    tracing::info!("Time: {:?}", time_res);
    Ok(())
}
