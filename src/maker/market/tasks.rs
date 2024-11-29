use crate::maker::market;
use crate::maker::market::data::{
    Balance, ExternalOrderbook, MarketState, MarketSubscriberData, SubaccountSubscriberData,
};
use crate::maker::market::external::binance::{get_binance_stream_url, CombinedBookTickerData};
use anyhow::{Error, Result};
use bigdecimal::BigDecimal;
use chrono::Utc;
use lyra_client::json_rpc::{http_rpc, Response, WsClient, WsClientExt};
use orderbook_types::generated::private_get_subaccount::{
    PrivateGetSubaccountParamsSchema, PrivateGetSubaccountResponseSchema,
};
use serde_json::Value;
use tracing::log::{error, info};

pub async fn subscribe_market(state: MarketState, instrument_names: Vec<String>) -> Result<()> {
    info!("Starting market subscriber task");
    let channels: Vec<String> = instrument_names
        .iter()
        .flat_map(|instrument_name| {
            vec![
                format!("orderbook.{}.1.10", instrument_name),
                format!("ticker.{}.1000", instrument_name),
            ]
        })
        .collect();

    let client = WsClient::new_client().await?;
    client
        .subscribe(channels, |d: MarketSubscriberData| async {
            match d {
                MarketSubscriberData::OrderbookMsg(msg) => {
                    state.write().await.insert_orderbook(msg.params.data);
                }
                MarketSubscriberData::TickerMsg(msg) => {
                    state
                        .write()
                        .await
                        .insert_ticker(msg.params.data.instrument_ticker);
                }
            }
            Ok(())
        })
        .await?;
    Ok(())
}

/// Syncs the subaccount state with the market struct
/// This includes balances, orders and recent trade history (to check for non-settled trades)
pub async fn sync_subaccount(
    market: MarketState,
    subaccount_id: i64,
    instrument_names: Vec<String>,
) -> Result<()> {
    let headers = lyra_client::auth::get_auth_headers().await;
    let subacc = http_rpc::<_, PrivateGetSubaccountResponseSchema>(
        "private/get_subaccount",
        PrivateGetSubaccountParamsSchema { subaccount_id },
        Some(headers.clone()),
    )
    .await?;
    info!("Subaccount state fetched");
    let mut writer = market.write().await;
    match subacc {
        Response::Error(e) => {
            error!("Failed to get subaccount with {:?}", e);
            return Err(Error::msg("Failed to get subaccount"));
        }
        Response::Success(subacc) => {
            let now = Utc::now().timestamp_millis();
            for position in subacc.result.positions {
                writer.insert_position(Balance {
                    instrument_name: position.instrument_name,
                    amount: position.amount,
                    timestamp: now,
                });
            }
            for collateral in subacc.result.collaterals {
                writer.insert_position(Balance {
                    instrument_name: collateral.asset_name,
                    amount: collateral.amount,
                    timestamp: now,
                });
            }
            for order in subacc.result.open_orders {
                let v = serde_json::to_value(&order).unwrap();
                let order = serde_json::from_value(v).unwrap();
                writer.insert_order(order);
            }
        }
    }

    info!("Subaccount state refreshed");
    for instrument_name in instrument_names {
        let trades = http_rpc::<_, Value>(
            "private/get_trade_history",
            orderbook_types::types::orders::GetTradesParams {
                subaccount_id,
                instrument_name: Some(instrument_name),
                order_id: None,
                quote_id: None,
                from_timestamp: Utc::now().timestamp_millis() - 1000 * 60,
                to_timestamp: Utc::now().timestamp_millis(),
                page: 1,
                page_size: 1000,
            },
            Some(headers.clone()),
        )
        .await?;
        info!("Trades");
        info!("{:?}", trades);
        if let Response::Success(trades) = trades {
            let trades =
                serde_json::from_value::<orderbook_types::types::orders::GetTradesResponse>(trades);
            if let Ok(trades) = trades {
                for trade in trades.result.trades {
                    writer.insert_trade(trade);
                }
            } else {
                error!("Failed to get trades with {:?}", trades);
                return Err(Error::msg("Failed to deserialize trades"));
            }
        } else {
            error!("Failed to get trades with {:?}", trades);
            return Err(Error::msg("Failed to get trades"));
        }
    }
    info!("Trades state refreshed");
    Ok(())
}

pub async fn subscribe_subaccount(market: MarketState, subaccount_id: i64) -> Result<()> {
    let channels: Vec<String> = vec![
        format!("{}.balances", subaccount_id),
        format!("{}.orders", subaccount_id),
        format!("{}.trades.settled", subaccount_id),
        format!("{}.trades.reverted", subaccount_id),
        format!("{}.trades", subaccount_id),
    ];

    let client = WsClient::new_client().await?;
    let login = client.login().await?.into_result()?;
    info!("Login: {:?}", login);
    info!("Subscribing to subaccount: {:?}", channels);
    client
        .subscribe(channels, |d: SubaccountSubscriberData| async {
            match d {
                SubaccountSubscriberData::BalancesMsg(msg) => {
                    let mut writer = market.write().await;
                    for balance in msg.params.data {
                        writer.insert_position(Balance {
                            instrument_name: balance.name.clone(),
                            amount: balance.new_balance.clone(),
                            timestamp: Utc::now().timestamp_millis(),
                        });
                    }
                }
                SubaccountSubscriberData::OrdersMsg(msg) => {
                    let mut writer = market.write().await;
                    for order in msg.params.data {
                        writer.insert_order(order);
                    }
                }
                SubaccountSubscriberData::TradesMsg(msg) => {
                    let mut writer = market.write().await;
                    for trade in msg.params.data {
                        writer.insert_trade(trade);
                    }
                }
            }
            Ok(())
        })
        .await?;
    Ok(())
}

/// just a debugging helper
pub async fn log_state(market: MarketState) {
    loop {
        market.read().await.log_state();
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

pub async fn add_external_instrument(market: MarketState, symbol: String, instrument_name: String) {
    let mut writer = market.write().await;
    writer.add_external_symbol(symbol, instrument_name);
}

pub async fn subscribe_external(market: MarketState, symbols: Vec<String>) -> Result<()> {
    let stream_url = get_binance_stream_url(symbols);
    market::external::binance::subscribe(&stream_url, |d: CombinedBookTickerData| async {
        let mut writer = market.write().await;
        let symbol = d.data.symbol.clone();
        let basis = writer.get_external_basis(&symbol);
        writer.insert_external_orderbook(ExternalOrderbook::from_tickers(d, basis));
        Ok(())
    })
    .await
}
