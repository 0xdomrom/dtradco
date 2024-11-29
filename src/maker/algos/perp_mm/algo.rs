use crate::maker::algos::perp_mm::{PerpMMParams, PerpMMParamsRef};
use crate::maker::market::data::{
    new_market_state, ExternalOrderbook, MarketState, OpenOrderSummary, OrderbookData,
};
use crate::maker::market::tasks::{subscribe_market, subscribe_subaccount, sync_subaccount};
use anyhow::{Error, Result};
use bigdecimal::{BigDecimal, One, RoundingMode, Zero};
use lyra_client::actions::OrderArgs;
use lyra_client::json_rpc::{Response, WsClient, WsClientExt, WsClientState};
use orderbook_types::generated::private_cancel_by_instrument::PrivateCancelByInstrumentResponseSchema;
use orderbook_types::types::orders::{Direction, OrderType, TimeInForce};
use orderbook_types::types::tickers::InstrumentTicker;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::Instrument;
use uuid::Uuid;

impl PerpMMParams {
    /// returns (best_bid, best_ask) with "smoothing" applied to handle missing / wide quotes
    fn get_smooth_bbo(
        &self,
        orderbook: &OrderbookData,
        mark: &BigDecimal,
    ) -> (BigDecimal, BigDecimal) {
        let one = BigDecimal::one();
        let best_bid = orderbook.bids.get(0).map(|x| x[0].clone());
        let best_ask = orderbook.asks.get(0).map(|x| x[0].clone());

        let best_bid_max = match best_ask {
            Some(ref best_ask) => best_ask.min(mark),
            None => mark,
        };
        let best_ask_min = match best_bid {
            Some(ref best_bid) => best_bid.max(mark),
            None => mark,
        };

        let best_bid_est = best_bid_max * (&one - &self.max_edge);
        let best_ask_est = best_ask_min * (&one + &self.max_edge);

        let best_bid = best_bid.unwrap_or(best_bid_est.clone()).max(best_bid_est);
        let best_ask = best_ask.unwrap_or(best_ask_est.clone()).min(best_ask_est);

        (best_bid, best_ask)
    }

    fn get_min_max_price(
        &self,
        is_reducing: bool,
        direction: Direction,
        best_bid: &BigDecimal,
        best_ask: &BigDecimal,
        external_mid: &Option<BigDecimal>,
        ticker: &InstrumentTicker,
    ) -> (BigDecimal, BigDecimal) {
        let mid = match external_mid {
            Some(external_mid) => external_mid.clone(),
            None => (best_bid + best_ask) / 2,
        };
        let min_edge = if is_reducing {
            self.reducing_min_edge.clone()
        } else {
            self.increasing_min_edge.clone()
        };
        let spread_to_mid = &min_edge * &mid;

        match direction {
            Direction::Buy => (
                ticker.min_price.clone(),
                (best_ask - &ticker.tick_size).min(&mid - spread_to_mid),
            ),
            Direction::Sell => (
                (best_bid + &ticker.tick_size).max(&mid + spread_to_mid),
                ticker.max_price.clone(),
            ),
        }
    }

    /// returns (price, amount, direction) to quote for the risk reducing level
    pub fn get_reducing_order(
        &self,
        orderbook: &OrderbookData,
        external_orderbook: &Option<ExternalOrderbook>,
        balance: &BigDecimal,
        ticker: &InstrumentTicker,
    ) -> (BigDecimal, BigDecimal, Direction) {
        let zero = BigDecimal::zero();
        let external_mid = match external_orderbook {
            Some(external_orderbook) => Some(external_orderbook.mid_price_with_basis()),
            None => None,
        };
        let mark = match external_mid {
            Some(ref external_mid) => external_mid.clone(),
            None => ticker.mark_price.clone(),
        };
        let exposure = balance * &mark;
        let side = match balance >= &zero {
            true => Direction::Sell,
            false => Direction::Buy,
        };
        let amount = balance.abs().with_scale_round(
            ticker.amount_step.fractional_digit_count(),
            RoundingMode::Down,
        );
        if amount < ticker.minimum_amount {
            return (BigDecimal::zero(), BigDecimal::zero(), side);
        }
        let (best_bid, best_ask) = self.get_smooth_bbo(orderbook, &mark);
        let (min_price, max_price) =
            self.get_min_max_price(true, side, &best_bid, &best_ask, &external_mid, ticker);

        // long means selling to reduce -> price down, short means buying to close -> price up
        let slippage = -&self.reducing_slippage * &exposure;
        // negative reducing_spread & buying -> price up, selling -> price down
        let spread = -&self.reducing_spread * &mark * side.sign();
        let rounding = match side {
            Direction::Buy => RoundingMode::Down,
            Direction::Sell => RoundingMode::Up,
        };
        let this_side_best = match side {
            Direction::Buy => best_bid,
            Direction::Sell => best_ask,
        };
        let price = (this_side_best + slippage + spread)
            .min(max_price)
            .max(min_price)
            .with_scale_round(ticker.tick_size.fractional_digit_count(), rounding);

        (price, amount, side)
    }
    fn get_notional_decay(&self, exposure: &BigDecimal) -> BigDecimal {
        /// desired notional equals current notional if it is below warn_exposure
        /// otherwise it linearly decays to zero as we approach max_exposure
        if exposure.abs() <= self.warn_exposure {
            return BigDecimal::one();
        }
        if exposure.abs() >= self.max_exposure {
            return BigDecimal::zero();
        }
        (&self.max_exposure - exposure.abs()) / (&self.max_exposure - &self.warn_exposure)
    }
    fn get_level_amount(
        &self,
        level: usize,
        direction: &Direction,
        balance: &BigDecimal,
        ticker: &InstrumentTicker,
    ) -> BigDecimal {
        let zero = BigDecimal::zero();
        let is_long = balance >= &zero;
        let is_risk_increasing = match (direction, is_long) {
            (Direction::Buy, true) => true,
            (Direction::Sell, false) => true,
            _ => false,
        };
        let mark = &ticker.index_price;
        let level_notional = &self.level_notionals[level];
        let prev_levels_exposure =
            self.level_notionals.iter().take(level).sum::<BigDecimal>() * direction.sign();

        let notional_decay = match is_risk_increasing {
            true => {
                let exposure = balance * mark + prev_levels_exposure;
                self.get_notional_decay(&exposure)
            }
            false => self.get_notional_decay(&prev_levels_exposure),
        };
        let level_notional = level_notional * notional_decay;
        let level_amount = level_notional / mark;
        let level_amount = level_amount.with_scale_round(
            ticker.amount_step.fractional_digit_count(),
            RoundingMode::Down,
        );
        if level_amount < ticker.minimum_amount {
            return BigDecimal::zero();
        };
        level_amount
    }
    /// returns price to quote for the risk increasing level
    fn get_level_price(
        &self,
        level: usize,
        amount: &BigDecimal,
        direction: &Direction,
        orderbook: &OrderbookData,
        external_orderbook: &Option<ExternalOrderbook>,
        balance: &BigDecimal,
        ticker: &InstrumentTicker,
    ) -> BigDecimal {
        let zero = BigDecimal::zero();
        let extern_mid = match external_orderbook {
            Some(external_orderbook) => Some(external_orderbook.mid_price_with_basis()),
            None => None,
        };
        let mark = match extern_mid {
            Some(ref external_mid) => external_mid.clone(),
            None => ticker.mark_price.clone(),
        };

        let (best_bid, best_ask) = self.get_smooth_bbo(orderbook, &mark);

        let (min_price, max_price) =
            self.get_min_max_price(false, *direction, &best_bid, &best_ask, &extern_mid, ticker);
        let prev_levels_exposure =
            self.level_notionals.iter().take(level).sum::<BigDecimal>() * direction.sign();
        // assume there is an aggressive risk-reducing level that will get filled first
        let reduced_balance = match direction {
            Direction::Buy => balance.max(&zero),
            Direction::Sell => balance.min(&zero),
        };
        let exposure: BigDecimal =
            (reduced_balance + amount * direction.sign() / 2) * &mark + prev_levels_exposure;
        let slippage = -&self.increasing_slippage * &exposure;
        let spread = -&self.increasing_spread * &mark * direction.sign();

        let rounding = match direction {
            Direction::Buy => RoundingMode::Down,
            Direction::Sell => RoundingMode::Up,
        };
        let this_side_best = match direction {
            Direction::Buy => best_bid,
            Direction::Sell => best_ask,
        };

        let price = (this_side_best + slippage + spread)
            .min(max_price)
            .max(min_price)
            .with_scale_round(ticker.tick_size.fractional_digit_count(), rounding);
        price
    }
    pub fn get_increasing_order(
        &self,
        direction: Direction,
        level: usize,
        orderbook: &OrderbookData,
        external_orderbook: &Option<ExternalOrderbook>,
        balance: &BigDecimal,
        ticker: &InstrumentTicker,
    ) -> (BigDecimal, BigDecimal) {
        let amount = self.get_level_amount(level, &direction, balance, ticker);
        let price = self.get_level_price(
            level,
            &amount,
            &direction,
            orderbook,
            external_orderbook,
            balance,
            ticker,
        );
        (price, amount)
    }
}

/*
order gets deleted from the state when the Market state no longer has it
todo edge case: send -> instant cancel AND fail to cancel -> nothing in the state
since send did not arrive yet -> order gets deleted -> order shows up in the state later
we need to have an "error check" function that spots an open order in the Market
for which there is no OMS order at all -> send cancel by instrument in that case
*/

#[derive(Clone, Debug)]
enum OMSOrderState {
    PendingSend,   // initialized as this
    Open,          // updated to open when polled from market and seen as Open
    PendingCancel, // updated to this when cancel is sent (no matter what the current state is)
}

#[derive(Clone, Debug)]
struct OMSOrder {
    nonce: i64,
    order_id: Option<Uuid>,
    label: String,
    direction: Direction,
    limit_price: BigDecimal,
    remain_amount: BigDecimal,
    state: OMSOrderState,
    // if order is meant to be risk reducing
    // when inserting a new reducing order we try to private/replace an existing reducing order
    is_reducing: bool,
    created_at: u64,
}

#[derive(Clone, Debug)]
struct OMS {
    subaccount_id: i64,
    instrument_name: String,
    client: WsClient,
    orders: HashMap<i64, OMSOrder>,
}

impl OMS {
    fn new(client: WsClient, subaccount_id: i64, instrument_name: String) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(OMS {
            subaccount_id,
            instrument_name,
            client: client.clone(),
            orders: HashMap::new(),
        }))
    }

    pub async fn cancel_by_instrument(
        &mut self,
    ) -> Result<Response<PrivateCancelByInstrumentResponseSchema>> {
        for (_, oms_order) in self.orders.iter_mut() {
            oms_order.state = OMSOrderState::PendingCancel;
        }
        self.client
            .cancel_by_instrument(self.subaccount_id, self.instrument_name.clone())
            .await
    }

    pub fn sync_from_open(&mut self, open_orders: HashMap<i64, OpenOrderSummary>) -> Result<()> {
        let mut nonces_to_remove: Vec<i64> = vec![];
        for (nonce, oms_order) in self.orders.iter_mut() {
            let open_order = open_orders.get(nonce);
            match open_order {
                Some(open_order) => {
                    let new_state = match &oms_order.state {
                        OMSOrderState::PendingSend => OMSOrderState::Open,
                        OMSOrderState::PendingCancel => OMSOrderState::PendingCancel,
                        OMSOrderState::Open => OMSOrderState::Open,
                    };
                    oms_order.state = new_state;
                    oms_order.remain_amount = open_order.remain_amount.clone();
                    oms_order.order_id = Some(Uuid::from_str(&open_order.order_id)?);
                }
                None => match oms_order.state {
                    OMSOrderState::PendingSend => {}
                    OMSOrderState::PendingCancel | OMSOrderState::Open => {
                        nonces_to_remove.push(nonce.clone());
                    }
                },
            }
        }
        for nonce in nonces_to_remove {
            self.orders.remove(&nonce);
        }
        for nonce in open_orders.keys() {
            let oms_order = self.orders.get(nonce);
            if oms_order.is_none() {
                return Err(Error::msg("Unexpected open order not present in OMS"));
            }
        }
        Ok(())
    }

    fn insert_from_args(&mut self, nonce: i64, is_reducing: bool, order_args: &OrderArgs) -> i64 {
        self.orders.insert(
            nonce,
            OMSOrder {
                nonce,
                order_id: None,
                label: order_args.label.clone(),
                direction: order_args.direction,
                limit_price: order_args.limit_price.clone(),
                remain_amount: order_args.amount.clone(),
                state: OMSOrderState::PendingSend,
                is_reducing,
                created_at: chrono::Utc::now().timestamp_millis() as u64,
            },
        );
        nonce
    }
    fn log_desired(desired_orders: &Vec<(bool, Direction, BigDecimal, BigDecimal)>) {
        for (is_reduce, dir, price, amount) in desired_orders {
            tracing::info!(
                "Desired: {} {} {} {}",
                if *is_reduce { "Reduce" } else { "Increase" },
                dir,
                price,
                amount
            );
        }
    }
    /// TODO currently doing await when sending to socket, might be better to create tasks and join
    pub async fn execute_desired(
        &mut self,
        state: &PerpMMState,
        desired_orders: &Vec<(bool, Direction, BigDecimal, BigDecimal)>,
    ) -> Result<i64> {
        let start = tokio::time::Instant::now();
        // OMS::log_desired(desired_orders);
        let ticker = &state.ticker;
        let zero = BigDecimal::zero();
        let mut replaceable_orders: HashMap<i64, OMSOrder> = HashMap::from_iter(
            self.orders
                .iter()
                .filter(|(_, oms_order)| {
                    matches!(
                        oms_order.state,
                        OMSOrderState::Open | OMSOrderState::PendingSend
                    )
                })
                .map(|(nonce, oms_order)| (nonce.clone(), oms_order.clone())),
        );
        let mut rpc_ids: Vec<Uuid> = vec![];
        for (is_reduce, dir, price, amount) in desired_orders {
            if amount == &zero || price == &zero {
                continue;
            }

            let order_args = OrderArgs {
                amount: amount.clone(),
                limit_price: price.clone(),
                direction: dir.clone(),
                time_in_force: TimeInForce::PostOnly,
                order_type: OrderType::Limit,
                label: format!("{}-{}", dir, price),
                mmp: true,
            };
            // avoid using up tps if a very similar order is already sent or open
            let similar_already_sent_or_open: Vec<i64> = replaceable_orders
                .iter()
                .filter(|(nonce, oms_order)| {
                    oms_order.direction == *dir
                        && oms_order.is_reducing == *is_reduce
                        && (&oms_order.limit_price - price).abs() / price < state.price_tolerance
                        && (&oms_order.remain_amount - amount).abs() / amount
                            < state.amount_tolerance
                })
                .map(|(nonce, oms_order)| nonce.clone())
                .collect();
            if similar_already_sent_or_open.len() > 0 {
                replaceable_orders.remove(&similar_already_sent_or_open[0]);
                continue;
            }
            // otherwise take an order of the same direction and intention and replace it
            let order_to_replace = replaceable_orders
                .iter()
                .find(|(_, oms_order)| {
                    oms_order.direction == *dir && oms_order.is_reducing == *is_reduce
                })
                .map(|(nonce, o)| (nonce.clone(), o.state.clone()));

            let mut new_sent = false;
            if let Some((nonce_to_replace, state)) = order_to_replace {
                replaceable_orders.remove(&nonce_to_replace);
                let replaced = self.orders.get_mut(&nonce_to_replace).unwrap();
                replaced.state = OMSOrderState::PendingCancel;

                // order that is PendingSend cannot be safely replaced because if it fails,
                // the replace call will start a fail chain (failed cancel fails a replace)
                // so for this case we do manual cancel by nonce + insert later
                match state {
                    OMSOrderState::Open => {
                        let (rpc_id, nonce) = self
                            .client
                            .send_replace_by_nonce_nowait(
                                ticker,
                                self.subaccount_id,
                                nonce_to_replace.clone(),
                                order_args.clone(),
                            )
                            .await?;
                        self.insert_from_args(nonce, *is_reduce, &order_args);
                        rpc_ids.push(rpc_id);
                        new_sent = true;
                    }
                    OMSOrderState::PendingSend => {
                        // send simple cancel by nonce
                        let rpc_id = self
                            .client
                            .cancel_by_nonce_nowait(
                                self.subaccount_id,
                                self.instrument_name.clone(),
                                nonce_to_replace.clone(),
                            )
                            .await?;
                        rpc_ids.push(rpc_id);
                    }
                    OMSOrderState::PendingCancel => {
                        return Err(Error::msg("Unexpected state PendingCancel"));
                    }
                };
            };
            // if state was PendingSend OR there was no order, we still need a regular send
            if !new_sent {
                let (rpc_id, nonce) = self
                    .client
                    .send_order_nowait(ticker, self.subaccount_id, order_args.clone())
                    .await?;
                self.insert_from_args(nonce, *is_reduce, &order_args);
                rpc_ids.push(rpc_id)
            }
        }
        // clean up any remaining replaceable orders
        for (nonce, _) in replaceable_orders {
            let cancelled = self.orders.get_mut(&nonce).unwrap();
            cancelled.state = OMSOrderState::PendingCancel;

            let rpc_id = self
                .client
                .cancel_by_nonce_nowait(
                    self.subaccount_id,
                    self.instrument_name.clone(),
                    nonce.clone(),
                )
                .await?;
            rpc_ids.push(rpc_id);
        }

        let num_rpc = rpc_ids.len();
        let waiter_client = self.client.clone();
        tokio::spawn(async move {
            for rpc_id in rpc_ids {
                let res = waiter_client.await_rpc(rpc_id).await;
                tracing::info!("RPC {:?} finished", rpc_id);
            }
        });

        let elapsed = start.elapsed();
        tracing::info!("execute_desired took {} us", elapsed.as_micros());
        Ok(num_rpc as i64)
    }
}

pub struct PerpMM {
    pub params: PerpMMParamsRef,
    pub market: MarketState,
    pub oms: Arc<Mutex<OMS>>,
}

/// temporary helper state to keep clones of market data during a cycle w/o locking the market
#[derive(Clone, Debug)]
struct PerpMMState {
    pub price_tolerance: BigDecimal,
    pub amount_tolerance: BigDecimal,
    pub ticker: InstrumentTicker,
    pub orderbook: OrderbookData,
    pub external_orderbook: Option<ExternalOrderbook>,
    pub balance: BigDecimal,
    pub open_orders: HashMap<i64, OpenOrderSummary>,
}

impl PerpMM {
    pub async fn new(params: PerpMMParamsRef) -> Result<Self> {
        let client = WsClient::new_client().await?;
        client.login().await?;
        client.enable_cancel_on_disconnect().await?;
        let (subaccount_id, instrument_name) = {
            let params = params.read().await;
            (params.subaccount_id, params.instrument_name.clone())
        };
        Ok(PerpMM {
            params,
            market: new_market_state(),
            oms: OMS::new(client, subaccount_id, instrument_name),
        })
    }

    pub async fn new_client() -> Result<WsClient> {
        let client = WsClient::new_client().await?;
        client.login().await?;
        client.enable_cancel_on_disconnect().await?;
        Ok(client)
    }

    pub async fn new_market() -> MarketState {
        let market = new_market_state();
        market
    }

    pub fn new_from_state(
        params: PerpMMParamsRef,
        market: MarketState,
        client: WsClient,
        subaccount_id: i64,
        instrument_name: String,
    ) -> Self {
        PerpMM {
            params,
            market,
            oms: OMS::new(client, subaccount_id, instrument_name),
        }
    }

    /// Central task for syncing subaccount and instrument state
    /// Can be ran as a single separate task that serves all market maker instances
    pub async fn run_market(
        market: MarketState,
        subaccount_id: i64,
        instrument_names: Vec<String>,
    ) -> Result<()> {
        /// TODO retry / reconnect logic here
        sync_subaccount(market.clone(), subaccount_id, instrument_names.clone()).await?;

        let subacc_sub = subscribe_subaccount(market.clone(), subaccount_id);
        let ticker_sub = subscribe_market(market.clone(), instrument_names.clone());

        let res = tokio::select! {
            _ = ticker_sub => {Err(Error::msg("Market subscription exited early"))},
            _ = subacc_sub => {Err(Error::msg("Subaccount subscription exited early"))},
        };

        tracing::warn!("PerpMM run_market finished with {:?}", res);
        res
    }

    async fn market_ready(&self) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            // checks if ticker is ready and all_trades_confirmed
            let params = self.params.read().await;
            let market = self.market.read().await;
            let ticker = market.get_ticker(&params.instrument_name);
            let ob = market.get_orderbook(&params.instrument_name);
            let external_ob = market.get_external_orderbook(&params.instrument_name);
            let confirmed = market.all_trades_confirmed(&params.instrument_name);
            if ticker.is_some() && ob.is_some() && external_ob.is_some() && confirmed {
                return Ok(());
            }
        }
    }

    async fn get_tps(&self) -> u64 {
        let params = self.params.read().await;
        params.max_tps
    }

    fn random_log_external_book(book: &Option<ExternalOrderbook>) {
        if let Some(book) = book {
            if book.recv_timestamp % 20 == 0 {
                let basis = &book.basis;
                let mid = book.mid_price();
                let mid_with_basis = book.mid_price_with_basis();
                tracing::info!(
                    "External book: mid {} basis {} mid_with_basis {}",
                    mid,
                    basis,
                    mid_with_basis
                );
            }
        }
    }

    pub async fn run_maker(&self) -> Result<()> {
        let max_tps = self.get_tps().await;
        self.market_ready().await?;

        let mut base_wait_time = tokio::time::interval(Duration::from_micros(200));
        let mut last_publish_id = 0;
        let mut tps_left: i64 = max_tps as i64;
        let mut last_refill_us = chrono::Utc::now().timestamp_micros();
        let mut last_external_book: Option<ExternalOrderbook> = None;
        loop {
            base_wait_time.tick().await;

            let now_us = chrono::Utc::now().timestamp_micros();
            let elapsed_since_last_refill = now_us - last_refill_us;
            if elapsed_since_last_refill > 1_000_000 {
                tps_left = max_tps as i64;
                last_refill_us = now_us;
            }
            if tps_left <= 0 {
                continue;
            }

            let params = self.params.read().await;
            let market = self.market.read().await;
            let orderbook = market
                .get_orderbook_exclude_my_orders(&params.instrument_name)
                .ok_or(Error::msg("Orderbook not found"))?;
            let external_book = market
                .get_external_orderbook(&params.instrument_name)
                .map_or(None, |x| Some(x.clone()));
            let is_external_different = match (&external_book, &last_external_book) {
                (Some(external_book), Some(last_external_book)) => {
                    external_book.is_different(last_external_book, &params.price_replace_tol)
                }
                _ => true,
            };
            if (orderbook.publish_id == last_publish_id) && !is_external_different {
                continue;
            } else {
                last_publish_id = orderbook.publish_id;
                last_external_book = external_book.clone();
            }
            if external_book.is_none() {
                tracing::warn!("External book not found");
            }

            Self::random_log_external_book(&external_book);

            let ticker = market
                .get_ticker(&params.instrument_name)
                .ok_or(Error::msg("Ticker not found"))?;
            let balance = market.get_amount(&params.instrument_name);

            let state = {
                PerpMMState {
                    price_tolerance: params.price_replace_tol.clone(),
                    amount_tolerance: params.amount_replace_tol.clone(),
                    ticker: ticker.clone(),
                    orderbook,
                    external_orderbook: external_book,
                    balance,
                    open_orders: market.get_open_orders_summary(&params.instrument_name),
                }
            };
            drop(market);

            let mut oms = self.oms.lock().await;
            let sync_res = oms.sync_from_open(state.open_orders.clone());
            if sync_res.is_err() {
                tracing::warn!("Failed to sync OMS state - cancelling all orders");
                oms.cancel_by_instrument().await?;
                tps_left -= 1;
                continue;
            }

            let (reduce_px, reduce_sz, reduce_dir) = params.get_reducing_order(
                &state.orderbook,
                &state.external_orderbook,
                &state.balance,
                &state.ticker,
            );
            let mut desired_orders: Vec<(bool, Direction, BigDecimal, BigDecimal)> = vec![];
            let reduce_non_zero = reduce_sz > BigDecimal::zero();
            if reduce_non_zero {
                desired_orders.push((true, reduce_dir, reduce_px, reduce_sz));
            }
            for i in 0..params.level_notionals.len() {
                let (bid_px, bid_sz) = params.get_increasing_order(
                    Direction::Buy,
                    i,
                    &state.orderbook,
                    &state.external_orderbook,
                    &state.balance,
                    &state.ticker,
                );
                let (ask_px, ask_sz) = params.get_increasing_order(
                    Direction::Sell,
                    i,
                    &state.orderbook,
                    &state.external_orderbook,
                    &state.balance,
                    &state.ticker,
                );
                desired_orders.push((false, Direction::Buy, bid_px, bid_sz));
                desired_orders.push((false, Direction::Sell, ask_px, ask_sz));
            }
            drop(params);

            let num_rpc = oms.execute_desired(&state, &desired_orders).await?;
            tps_left -= num_rpc;

            if tps_left <= 1 {
                tracing::warn!("TPS limit reached, waiting for refill");
                tps_left -= 1;
                oms.cancel_by_instrument().await?.into_result()?;
            }
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let run_maker = self.run_maker();
        let client = {
            let oms = self.oms.lock().await;
            oms.client.clone()
        };
        let run_ping = client.ping_interval(15);
        let res = tokio::select! {
            _ = run_ping => {Err(Error::msg("Ping interval exited early"))},
            r = run_maker => {
                match r {
                    Ok(_) => tracing::error!("Maker exited early with status ok"),
                    Err(e) => tracing::error!("Maker exited early with error: {:?}", e),
                }
                Err(Error::msg("Maker exited early"))
            },
        };
        res
    }
}
