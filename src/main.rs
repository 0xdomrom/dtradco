mod maker;

use dotenvy::dotenv;
use dotenvy_macro::dotenv;
use dtradco::run;
use lyra_client::json_rpc::WsClientExt;
use anyhow::Result;
use lyra_client::setup::ensure_env;
use serde_json::{json, Value};
use tracing::log::info;
use crate::maker::market;
use crate::maker::market::data::new_market_state;
use crate::maker::market::tasks::{subscribe_market, subscribe_subaccount, sync_subaccount};

pub async fn setup_env() {
    dotenvy::from_filename(".env").expect("Failed to load .env file");
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    info!("ENV: {}", std::env::var("ENV").unwrap());
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
    let market = new_market_state();
    let client = lyra_client::json_rpc::WsClient::new_client().await?;
    client.login().await?.into_result()?;
    // let client = lyra_client::json_rpc::WsClient::new_client().await?;
    // let time_res = client.send_rpc::<Value, Value>("public/get_time", json!({})).await?;
    // tracing::info!("Time: {:?}", time_res);
    // sleep for 3 sec

    let subacc_id = 103410;
    sync_subaccount(market.clone(), subacc_id, vec!["ETH-PERP".to_string()]).await?;
    tokio::spawn(subscribe_subaccount(market.clone(), subacc_id));
    // tokio::spawn(subscribe_market(market.clone(), vec!["ETH-PERP"]));
    tokio::spawn(market::tasks::log_state(market.clone()));
    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    Ok(())
}
