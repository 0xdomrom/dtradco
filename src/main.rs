mod maker;

use crate::maker::algos::perp_mm::{new_params_ref, PerpMM, PerpMMParams, PerpMMParamsRef};
use crate::maker::market;
use crate::maker::market::data::new_market_state;
use crate::maker::market::tasks::{subscribe_market, subscribe_subaccount, sync_subaccount};
use anyhow::{Error, Result};
use bigdecimal::BigDecimal;
use dotenvy::dotenv;
use dotenvy_macro::dotenv;
use dtradco::run;
use lyra_client::json_rpc::WsClientExt;
use lyra_client::setup::ensure_env;
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::log::info;

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

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    // let db_uri = format!("{}", dotenv!("DATABASE_URL"));
    // run(db_uri).await;
    setup_env().await;

    let args: Vec<String> = std::env::args().collect();
    let json_name = args.get(1).ok_or(Error::msg("No json name provided"))?;
    let params = tokio::fs::read_to_string(format!("./params/{json_name}.json")).await?;
    let params: Vec<PerpMMParams> = serde_json::from_str(&params)?;

    let params_refs = params
        .into_iter()
        .map(|p| new_params_ref(p))
        .collect::<Vec<PerpMMParamsRef>>();

    let client = PerpMM::new_client().await?;
    let market = PerpMM::new_market().await;

    let perp_mms: Vec<Arc<PerpMM>> = params_refs
        .into_iter()
        .map(|params_ref| {
            Arc::new(PerpMM::new_from_state(
                params_ref,
                market.clone(),
                client.clone(),
            ))
        })
        .collect();

    let mut join_set = JoinSet::new();

    for i in 0..perp_mms.len() {
        let perp_mm = Arc::clone(&perp_mms[i]);
        join_set.spawn(async move {
            let res = perp_mm.run().await;
            if let Err(ref e) = res {
                tracing::error!("Perp MM failed: {:?}", e);
            }
            // todo clean restart
            panic!("Perp MM failed: {:?}", res);
        });
    }

    join_set.join_all().await;

    Ok(())
}
