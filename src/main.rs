mod maker;

use crate::maker::algos::perp_mm::{new_params_ref, PerpMM, PerpMMParams, PerpMMParamsRef};
use crate::maker::market;
use crate::maker::market::data::new_market_state;
use crate::maker::market::tasks::{
    add_external_instrument, subscribe_market, subscribe_subaccount, sync_subaccount,
};
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

    let args: Vec<String> = std::env::args().collect();
    let json_name = args.get(1).ok_or(Error::msg("No json name provided"))?;
    let env_name: &String = args.get(2).ok_or(Error::msg("No env name provided"))?;
    let sk_name = args.get(3);

    std::env::set_var("ENV", env_name.clone());
    if let Some(sk_name) = sk_name {
        std::env::set_var("SESSION_KEY_NAME", sk_name.clone());
        std::env::set_var("OWNER_KEY_NAME", sk_name.clone());
    }

    setup_env().await;
    lyra_client::setup::ensure_session_key().await;
    lyra_client::setup::ensure_owner().await;
    lyra_client::setup::setup_ws_endpoint().await;

    let params = tokio::fs::read_to_string(format!("./params/{json_name}.json")).await?;
    let params: Vec<PerpMMParams> = serde_json::from_str(&params)?;
    let subaccs = params.iter().map(|p| p.subaccount_id).collect::<Vec<i64>>();
    let instruments = params
        .iter()
        .map(|p| p.instrument_name.clone())
        .collect::<Vec<String>>();

    if !subaccs.iter().all(|&s| s == subaccs[0]) {
        panic!("All subaccounts in the params array must be the same");
    }
    let subaccount_id = subaccs[0];

    let symbols: Vec<String> = params.iter().map(|p| p.external_symbol.clone()).collect();

    let client = PerpMM::new_client().await?;
    let market = PerpMM::new_market().await;

    for p in params.iter() {
        let mkt = market.clone();
        add_external_instrument(mkt, p.external_symbol.clone(), p.instrument_name.clone()).await;
    }

    let params_refs = params
        .into_iter()
        .map(|p| new_params_ref(p))
        .collect::<Vec<PerpMMParamsRef>>();

    let perp_mms: Vec<Arc<PerpMM>> = params_refs
        .into_iter()
        .enumerate()
        .map(|(i, params_ref)| {
            let subacc = subaccs[i];
            let instr = instruments[i].clone();
            Arc::new(PerpMM::new_from_state(
                params_ref,
                market.clone(),
                client.clone(),
                subacc.clone(),
                instr.clone(),
            ))
        })
        .collect();

    let mut join_set = JoinSet::new();

    let mkt_clone = market.clone();
    let market_handle = tokio::spawn(async move {
        PerpMM::run_market(mkt_clone, subaccount_id, instruments.clone()).await
    });
    let external_handle =
        tokio::spawn(
            async move { market::tasks::subscribe_external(market.clone(), symbols).await },
        );

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

    let mms_handle = join_set.join_all();

    tokio::select! {
        _ = mms_handle => {
            tracing::error!("Perp MM failed");
        }
        _ = market_handle => {
            tracing::error!("Market failed");
        }
        _ = external_handle => {
            tracing::error!("External failed");
        }
    }

    Ok(())
}
