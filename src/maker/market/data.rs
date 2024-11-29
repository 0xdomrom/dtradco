/*
Defines core shared state of the market.
Public and private modules define logic for ws subscriptions that update the shared state.
*/
use crate::maker::market::external::binance::CombinedBookTickerData;
use bigdecimal::{BigDecimal, FromPrimitive, One, Zero};
use chrono::Utc;
use lyra_client::actions::{Direction, OrderStatus};
use lyra_client::json_rpc::Notification;
use orderbook_types::generated::channel_orderbook_instrument_name_group_depth::OrderbookInstrumentNameGroupDepthPublisherDataSchema;
use orderbook_types::generated::channel_subaccount_id_balances::BalanceUpdateSchema;
use orderbook_types::types::orders::{
    GetTradesParams, GetTradesResponse, OrderNotificationData, OrderResponse,
    TradeNotificationData, TradeResponse, TxStatus,
};
use orderbook_types::types::tickers::result::{InstrumentTicker, TickerNotificationData};
use sea_orm::DeriveDisplay;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

pub type OrderbookData = OrderbookInstrumentNameGroupDepthPublisherDataSchema;

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum MarketSubscriberData {
    OrderbookMsg(Notification<OrderbookData>),
    TickerMsg(Notification<TickerNotificationData>),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum SubaccountSubscriberData {
    BalancesMsg(Notification<Vec<BalanceUpdateSchema>>),
    OrdersMsg(Notification<OrderNotificationData>),
    TradesMsg(Notification<TradeNotificationData>),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Balance {
    pub instrument_name: String,
    pub amount: BigDecimal,
    pub timestamp: i64,
}

#[derive(Clone, Debug)]
pub struct ExternalOrderbook {
    pub symbol: String,
    pub best_bid_price: BigDecimal,
    pub best_ask_price: BigDecimal,
    pub recv_timestamp: i64,
    pub send_timestamp: i64,
    pub basis: BigDecimal,
}

impl ExternalOrderbook {
    pub fn from_tickers(data: CombinedBookTickerData, basis: BigDecimal) -> Self {
        ExternalOrderbook {
            symbol: data.data.symbol,
            best_bid_price: data.data.best_bid_price,
            best_ask_price: data.data.best_ask_price,
            recv_timestamp: Utc::now().timestamp_millis(),
            send_timestamp: data.data.event_time,
            basis,
        }
    }
    pub fn is_different(&self, other: &ExternalOrderbook, replace_tol: &BigDecimal) -> bool {
        let this_mid: BigDecimal = self.mid_price();
        let other_mid: BigDecimal = other.mid_price();
        // magnify diff by 2 since we want to be more reactive than the replace tolerance
        let diff = 2 * (&this_mid - &other_mid).abs() / &other_mid;
        &diff > replace_tol
    }
    pub fn mid_price(&self) -> BigDecimal {
        (&self.best_bid_price + &self.best_ask_price) / 2
    }
    pub fn mid_price_with_basis(&self) -> BigDecimal {
        &self.mid_price() * &self.basis
    }
}

impl Display for Balance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.instrument_name, self.amount)
    }
}

pub type MarketState = Arc<RwLock<MarketData>>;

pub struct MarketData {
    tickers: HashMap<String, InstrumentTicker>,
    orderbooks: HashMap<String, OrderbookData>,
    positions: HashMap<String, Balance>,
    orders: HashMap<String, HashMap<String, OrderResponse>>,
    trades: HashMap<String, HashMap<String, TradeResponse>>,
    external_orderbooks: HashMap<String, ExternalOrderbook>,
    internal_to_external_symbol: HashMap<String, String>,
    external_basis: HashMap<String, (BigDecimal, i64)>,
}

const STALENESS_MS: i64 = 2_000; // todo ideally want to log the staleness
const BASIS_DECAY_RATE: f64 = 0.00006931471; // -ln(0.5) / 10000, i.e. half life of 10 seconds

impl MarketData {
    pub fn new() -> Self {
        MarketData {
            tickers: HashMap::new(),
            orderbooks: HashMap::new(),
            positions: HashMap::new(),
            orders: HashMap::new(),
            trades: HashMap::new(),
            external_orderbooks: HashMap::new(),
            internal_to_external_symbol: HashMap::new(),
            external_basis: HashMap::new(),
        }
    }
    pub fn get_orderbook(&self, instrument_name: &str) -> Option<&OrderbookData> {
        let orderbook = self.orderbooks.get(instrument_name);
        let is_stale = orderbook.map_or(true, |o| {
            Utc::now().timestamp_millis() - o.timestamp > STALENESS_MS
        });
        match is_stale {
            true => None,
            false => orderbook,
        }
    }
    pub fn get_external_basis(&self, instrument_name: &str) -> BigDecimal {
        self.external_basis
            .get(instrument_name)
            .map_or(BigDecimal::one(), |(basis, _)| basis.clone())
    }
    pub fn update_basis(&mut self, symbol: &str, basis: BigDecimal) {
        let basis_now = self.external_basis.get(symbol);
        let ts_now = Utc::now().timestamp_millis();
        let (basis_now, ts) = match basis_now {
            Some((prev_basis, prev_ts)) => {
                let dt = (ts_now - prev_ts) as f64;
                let decay = (-dt * BASIS_DECAY_RATE).exp();
                let decay = BigDecimal::from_f64(decay).unwrap();
                let new_basis = prev_basis * &decay + basis * (1 - &decay);
                (new_basis, ts_now)
            }
            None => (basis, ts_now),
        };
        self.external_basis
            .insert(symbol.to_string(), (basis_now.round(18), ts));
    }
    pub fn insert_orderbook(&mut self, orderbook: OrderbookData) {
        let instrument_name = orderbook.instrument_name.clone();
        self.orderbooks
            .insert(orderbook.instrument_name.clone(), orderbook);
        // if an external orderbook exists for this symbol, calibrate the basis
        if let Some(extern_ob) = self.get_external_orderbook(&instrument_name) {
            let extern_mid = extern_ob.mid_price();
            let ob = self.get_orderbook(&instrument_name).unwrap();
            let best_bid = ob.bids.first();
            let best_ask = ob.asks.first();
            if let (Some(best_bid), Some(best_ask)) = (best_bid, best_ask) {
                let mid: BigDecimal = (&best_bid[0] + &best_ask[0]) / 2;
                let basis = &mid / &extern_mid;
                let symbol = self
                    .internal_to_external_symbol
                    .get(&instrument_name)
                    .expect(format!("No external symbol for {}", instrument_name).as_str())
                    .clone();
                self.update_basis(&symbol, basis);
            }
        }
    }
    pub fn add_external_symbol(&mut self, symbol: String, instrument_name: String) {
        self.internal_to_external_symbol
            .insert(instrument_name, symbol);
    }
    pub fn get_external_orderbook(&self, instrument_name: &str) -> Option<&ExternalOrderbook> {
        let symbol_name = self
            .internal_to_external_symbol
            .get(instrument_name)
            .expect(format!("No external symbol for {}", instrument_name).as_str());
        let orderbook = self.external_orderbooks.get(symbol_name);
        let is_stale = orderbook.map_or(true, |o| {
            chrono::Utc::now().timestamp_millis() - o.recv_timestamp > STALENESS_MS
        });
        match is_stale {
            true => None,
            false => orderbook,
        }
    }
    pub fn insert_external_orderbook(&mut self, orderbook: ExternalOrderbook) {
        if orderbook.recv_timestamp % 20 == 0 {
            let latency = orderbook.recv_timestamp - orderbook.send_timestamp;
            info!("Received external orderbook with latency: {} ms", latency);
        }
        self.external_orderbooks
            .insert(orderbook.symbol.clone(), orderbook);
    }
    pub fn iter_orderbooks(&self) -> impl Iterator<Item = &OrderbookData> {
        self.orderbooks.values()
    }
    pub fn get_tickers(&self) -> &HashMap<String, InstrumentTicker> {
        &self.tickers
    }
    pub fn get_ticker_maybe_stale(&self, instrument_name: &str) -> Option<&InstrumentTicker> {
        self.tickers.get(instrument_name)
    }
    pub fn get_ticker(&self, instrument_name: &str) -> Option<&InstrumentTicker> {
        let ticker = self.tickers.get(instrument_name);
        let is_stale = ticker.map_or(true, |t| {
            chrono::Utc::now().timestamp_millis() - t.timestamp > STALENESS_MS
        });
        match is_stale {
            true => None,
            false => ticker,
        }
    }
    pub fn insert_ticker(&mut self, ticker: InstrumentTicker) {
        self.tickers.insert(ticker.instrument_name.clone(), ticker);
    }
    pub fn iter_tickers(&self) -> impl Iterator<Item = &InstrumentTicker> {
        self.tickers.values()
    }
    pub fn get_position(&self, instrument_name: &str) -> Option<&Balance> {
        self.positions.get(instrument_name)
    }
    pub fn get_amount(&self, instrument_name: &str) -> BigDecimal {
        self.positions
            .get(instrument_name)
            .map_or(BigDecimal::zero(), |p| p.amount.clone())
    }
    pub fn insert_position(&mut self, position: Balance) {
        self.positions
            .insert(position.instrument_name.clone(), position);
    }
    pub fn iter_positions(&self) -> impl Iterator<Item = &Balance> {
        self.positions.values()
    }
    pub fn get_orders(&self, instrument_name: &str) -> Option<&HashMap<String, OrderResponse>> {
        self.orders.get(instrument_name)
    }
    pub fn insert_order(&mut self, order: OrderResponse) {
        let orders = self
            .orders
            .entry(order.instrument_name.clone())
            .or_default();
        let order_id = order.order_id.clone();
        let existing = orders.remove(&order_id);
        if let Some(existing) = existing {
            // insert if new is newer and status is open
            let is_newer = existing.last_update_timestamp < order.last_update_timestamp;
            if (order.order_status == OrderStatus::Open) && is_newer {
                orders.insert(order_id, order);
            } else if is_newer {
                return; // received filled, expired or cancelled - so keep the order removed
            } else {
                orders.insert(order_id, existing);
            }
        } else if order.order_status == OrderStatus::Open {
            orders.insert(order_id, order);
        }
    }
    pub fn iter_orders(&self) -> impl Iterator<Item = &HashMap<String, OrderResponse>> {
        self.orders.values()
    }
    pub fn get_orderbook_exclude_my_orders(&self, instrument_name: &str) -> Option<OrderbookData> {
        let ob = self.get_orderbook(instrument_name)?;
        let orders = self.get_orders(instrument_name);
        let mut ob = ob.clone();
        if orders.is_none() {
            return Some(ob);
        }
        let orders = orders?;
        for order in orders.values() {
            let bids_or_asks = match order.direction {
                Direction::Buy => &mut ob.bids,
                Direction::Sell => &mut ob.asks,
            };
            for level in bids_or_asks.iter_mut() {
                if level[0] != order.limit_price {
                    continue;
                }
                let remain_amount = &order.amount - &order.filled_amount;
                if level[1] > remain_amount {
                    level[1] -= &remain_amount;
                } else {
                    level[1] = BigDecimal::zero();
                }
            }
            bids_or_asks.retain(|x| x[1] > BigDecimal::zero());
        }
        Some(ob)
    }
    pub fn get_trades(&self, instrument_name: &str) -> Option<&HashMap<String, TradeResponse>> {
        self.trades.get(instrument_name)
    }
    pub fn insert_trade(&mut self, trade: TradeResponse) {
        let trades = self
            .trades
            .entry(trade.instrument_name.clone())
            .or_default();
        trades.insert(trade.trade_id.clone(), trade);
    }
    pub fn all_trades_confirmed(&self, instrument_name: &str) -> bool {
        let trades = self.get_trades(instrument_name);
        match trades {
            Some(trades) => trades
                .values()
                .all(|t| t.tx_status == TxStatus::Settled || t.tx_status == TxStatus::Reverted),
            None => true,
        }
    }
    pub fn get_open_orders_summary(&self, instrument_name: &str) -> HashMap<i64, OpenOrderSummary> {
        let orders = self.get_orders(instrument_name);
        if orders.is_none() {
            return HashMap::new();
        }
        let orders = orders.unwrap();
        let summary = HashMap::from_iter(
            orders
                .values()
                .filter(|o| o.order_status == OrderStatus::Open)
                .map(|o| (o.nonce, OpenOrderSummary::new_from_response(o))),
        );
        summary
    }

    pub fn get_direction_summary(
        &self,
        instrument_name: &str,
        direction: Direction,
    ) -> Vec<OpenOrderSummary> {
        if let Some(orders) = self.get_orders(instrument_name) {
            let mut summary: Vec<OpenOrderSummary> = orders
                .values()
                .filter(|o| o.direction == direction)
                .map(|o| OpenOrderSummary::new_from_response(o))
                .collect();
            if direction == Direction::Buy {
                summary.sort_by(|a, b| b.limit_price.partial_cmp(&a.limit_price).unwrap());
            } else {
                summary.sort_by(|a, b| a.limit_price.partial_cmp(&b.limit_price).unwrap());
            }
            summary
        } else {
            Vec::new()
        }
    }
    pub fn log_state(&self) {
        info!("-----------------------------------------------------------------------------");
        info!("Tickers:");
        for ticker in self.iter_tickers() {
            info!("\t{:?}", ticker);
        }
        info!("Orderbooks:");
        for orderbook in self.iter_orderbooks() {
            info!("\t{:?}", orderbook);
        }
        info!("Positions:");
        for position in self.iter_positions() {
            info!("\t{}", position);
        }
        info!("Orders:");
        for orders in self.iter_orders() {
            for order in orders.values() {
                info!("\t{:?}", order);
            }
        }
        info!("Trades:");
        for trades in self.trades.values() {
            for trade in trades.values() {
                info!("\t{:?}", trade);
            }
        }
        info!("-----------------------------------------------------------------------------");
    }
}

#[derive(Clone, Debug)]
pub struct OpenOrderSummary {
    pub nonce: i64,
    pub order_id: String,
    pub direction: Direction,
    pub limit_price: BigDecimal,
    pub remain_amount: BigDecimal,
}

impl OpenOrderSummary {
    pub fn new_from_response(order: &OrderResponse) -> Self {
        OpenOrderSummary {
            nonce: order.nonce,
            order_id: order.order_id.clone(),
            direction: order.direction,
            limit_price: order.limit_price.clone(),
            remain_amount: &order.amount - &order.filled_amount,
        }
    }
}

pub fn new_market_state() -> MarketState {
    Arc::new(RwLock::new(MarketData::new()))
}
