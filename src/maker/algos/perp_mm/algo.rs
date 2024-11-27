use crate::maker::algos::perp_mm::{PerpMMParams, PerpMMParamsRef};
use crate::maker::market::data::{new_market_state, MarketState, OrderbookData};
use crate::maker::market::tasks::{subscribe_market, subscribe_subaccount, sync_subaccount};
use anyhow::{Error, Result};
use bigdecimal::{BigDecimal, One, RoundingMode, Zero};
use lyra_client::actions::OrderArgs;
use lyra_client::json_rpc::{WsClient, WsClientExt, WsClientState};
use orderbook_types::types::orders::{Direction, OrderType, TimeInForce};
use orderbook_types::types::tickers::InstrumentTicker;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
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

        let best_bid_est = best_bid_max * (&one - &self.max_spread_to_mid);
        let best_ask_est = best_ask_min * (&one + &self.max_spread_to_mid);

        let best_bid = best_bid.unwrap_or(best_bid_est.clone()).max(best_bid_est);
        let best_ask = best_ask.unwrap_or(best_ask_est.clone()).min(best_ask_est);

        (best_bid, best_ask)
    }

    /// returns (price, amount, direction) to quote for the risk reducing level
    pub fn get_reducing_order(
        &self,
        orderbook: &OrderbookData,
        balance: &BigDecimal,
        ticker: &InstrumentTicker,
    ) -> (BigDecimal, BigDecimal, Direction) {
        let zero = BigDecimal::zero();
        let mark = &ticker.mark_price;
        let exposure = balance * mark;
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
        let (best_bid, best_ask) = self.get_smooth_bbo(orderbook, mark);
        let mid: BigDecimal = (&best_bid + &best_ask) / 2;

        let spread_to_best = &self.min_spread_to_best * &mid;
        let (min_price, max_price) = match side {
            Direction::Buy => (ticker.min_price.clone(), &best_ask - spread_to_best),
            Direction::Sell => (&best_bid + spread_to_best, ticker.max_price.clone()),
        };

        // long means selling to reduce -> price down, short means buying to close -> price up
        let slippage = -&self.reducing_slippage * &exposure;
        // negative reducing_spread & buying -> price up, selling -> price down
        let spread = -&self.reducing_spread * &mid * side.sign();
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
        balance: &BigDecimal,
        ticker: &InstrumentTicker,
    ) -> BigDecimal {
        let zero = BigDecimal::zero();
        let mark = &ticker.index_price;
        let (best_bid, best_ask) = self.get_smooth_bbo(orderbook, mark);
        let mid: BigDecimal = (&best_bid + &best_ask) / 2;
        let spread_to_mid = &self.min_spread_to_mid * &mid;
        let (min_price, max_price) = match direction {
            Direction::Buy => (ticker.min_price.clone(), &mid - spread_to_mid),
            Direction::Sell => (&mid + spread_to_mid, ticker.max_price.clone()),
        };
        let prev_levels_exposure =
            self.level_notionals.iter().take(level).sum::<BigDecimal>() * direction.sign();
        // assume there is an aggressive risk-reducing level that will get filled first
        let reduced_balance = match direction {
            Direction::Buy => balance.max(&zero),
            Direction::Sell => balance.min(&zero),
        };
        let exposure: BigDecimal =
            (reduced_balance + amount * direction.sign() / 2) * mark + prev_levels_exposure;
        let slippage = -&self.increasing_slippage * &exposure;
        let spread = -&self.increasing_spread * &mid * direction.sign();

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
        balance: &BigDecimal,
        ticker: &InstrumentTicker,
    ) -> (BigDecimal, BigDecimal) {
        let amount = self.get_level_amount(level, &direction, balance, ticker);
        let price = self.get_level_price(level, &amount, &direction, orderbook, balance, ticker);
        (price, amount)
    }
}

struct PerpMMState {
    pub subaccount_id: i64,
    pub price_tolerance: BigDecimal,
    pub amount_tolerance: BigDecimal,
    pub ticker: InstrumentTicker,
    pub orderbook: OrderbookData,
    pub balance: BigDecimal,
    pub open_bids: Vec<(String, BigDecimal, BigDecimal)>,
    pub open_asks: Vec<(String, BigDecimal, BigDecimal)>,
}

pub struct PerpMM {
    pub params: PerpMMParamsRef,
    pub market: MarketState,
    pub client: WsClient,
}

impl PerpMM {
    pub async fn new(params: PerpMMParamsRef) -> Result<Self> {
        let market = new_market_state();
        let client = WsClient::new_client().await?;
        client.login().await?;
        client.enable_cancel_on_disconnect().await?;
        Ok(PerpMM {
            params,
            market,
            client,
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

    pub fn new_from_state(params: PerpMMParamsRef, market: MarketState, client: WsClient) -> Self {
        PerpMM {
            params,
            market,
            client,
        }
    }

    async fn run_market(&self) -> Result<()> {
        let market = &self.market;
        let params = self.params.read().await;
        let instrument_name = params.instrument_name.clone();
        let subaccount_id = params.subaccount_id;
        drop(params);

        let sync_instruments = vec![instrument_name.clone()];
        sync_subaccount(market.clone(), subaccount_id, sync_instruments).await?;

        let subacc_sub = subscribe_subaccount(market.clone(), subaccount_id);
        let ticker_sub = subscribe_market(market.clone(), vec![&instrument_name]);

        let res = tokio::select! {
            _ = ticker_sub => {Err(Error::msg("Market subscription exited early"))},
            _ = subacc_sub => {Err(Error::msg("Subaccount subscription exited early"))},
        };

        tracing::warn!("LimitOrderAuction run_market finished with {:?}", res);
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
            let confirmed = market.all_trades_confirmed(&params.instrument_name);
            if ticker.is_some() && ob.is_some() && confirmed {
                return Ok(());
            }
        }
    }

    async fn execute_desired(
        &self,
        state: &PerpMMState,
        direction: Direction,
        quotes: &Vec<(BigDecimal, BigDecimal)>,
        open_quotes: &Vec<(String, BigDecimal, BigDecimal)>,
    ) -> Result<()> {
        // first iterate open quotes and use replace endpoint if open limit price exceeds tol
        // if some quotes are still left un-opened, use regular send_order endpoint
        let subaccount_id = state.subaccount_id;
        let ticker = &state.ticker;
        let zero = BigDecimal::zero();
        let mut send_tasks = vec![];
        let mut replace_tasks = vec![];
        let mut cancel_tasks = vec![];

        if open_quotes.len() > quotes.len() {
            tracing::warn!("Open quotes exceed desired quotes");
            // cancel by instrument
            self.client
                .cancel_by_instrument(subaccount_id, ticker.instrument_name.clone())
                .await?;
            return Ok(());
        }

        for i in 0..open_quotes.len() {
            let (desired_price, desired_amount) = &quotes[i];
            let order_args = OrderArgs {
                amount: desired_amount.clone(),
                limit_price: desired_price.clone(),
                direction,
                time_in_force: TimeInForce::PostOnly,
                order_type: OrderType::Limit,
                label: format!("{}-{}", direction, i),
                mmp: true,
            };
            let open_level = &open_quotes[i];
            let open_id = Uuid::from_str(&open_level.0)?;
            let open_price = &open_level.1;
            let open_amount = &open_level.2;
            if desired_amount == &zero {
                let task =
                    self.client
                        .cancel(subaccount_id, ticker.instrument_name.clone(), open_id);
                cancel_tasks.push(task);
            } else {
                let is_price_new = (open_price - desired_price).abs() > state.price_tolerance;
                let is_amount_new = (open_amount - desired_amount).abs() > state.amount_tolerance;
                if !is_price_new && !is_amount_new {
                    continue;
                }
                let task = self
                    .client
                    .send_replace(ticker, subaccount_id, open_id, order_args);
                replace_tasks.push(task);
            }
        }

        // handle remaining quotes
        for i in open_quotes.len()..quotes.len() {
            let (desired_price, desired_amount) = &quotes[i];
            let order_args = OrderArgs {
                amount: desired_amount.clone(),
                limit_price: desired_price.clone(),
                direction,
                time_in_force: TimeInForce::PostOnly,
                order_type: OrderType::Limit,
                label: format!("{}-{}", direction, i),
                mmp: true,
            };
            if desired_amount == &zero {
                continue;
            }
            let task = self.client.send_order(ticker, subaccount_id, order_args);
            send_tasks.push(task);
        }

        let res = tokio::join! {
            futures::future::join_all(send_tasks),
            futures::future::join_all(replace_tasks),
            futures::future::join_all(cancel_tasks),
        };

        Ok(())
    }

    async fn get_tps(&self) -> u64 {
        let params = self.params.read().await;
        params.max_tps
    }

    pub async fn run_maker(&self) -> Result<()> {
        let max_tps = self.get_tps().await;
        self.market_ready().await?;

        let mut base_wait_time = tokio::time::interval(Duration::from_millis(1));
        let mut last_publish_id = 0;
        let mut tps_left: i64 = max_tps as i64;
        let mut last_refill_us = chrono::Utc::now().timestamp_micros();
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

            if orderbook.publish_id == last_publish_id {
                continue;
            } else {
                last_publish_id = orderbook.publish_id;
            }

            let ticker = market
                .get_ticker(&params.instrument_name)
                .ok_or(Error::msg("Ticker not found"))?;
            let balance = market.get_amount(&params.instrument_name);

            let state = {
                PerpMMState {
                    subaccount_id: params.subaccount_id,
                    price_tolerance: params.price_replace_tol.clone(),
                    amount_tolerance: params.amount_replace_tol.clone(),
                    ticker: ticker.clone(),
                    orderbook,
                    balance,
                    open_bids: market
                        .get_open_orders_summary(&params.instrument_name, Direction::Buy),
                    open_asks: market
                        .get_open_orders_summary(&params.instrument_name, Direction::Sell),
                }
            };
            drop(market);

            let (reduce_px, reduce_sz, reduce_dir) =
                params.get_reducing_order(&state.orderbook, &state.balance, &state.ticker);
            let (mut desired_bids, mut desired_asks) = (vec![], vec![]);
            let reduce_non_zero = reduce_sz > BigDecimal::zero();
            match (reduce_dir, reduce_non_zero) {
                (Direction::Buy, true) => {
                    desired_bids.push((reduce_px, reduce_sz));
                }
                (Direction::Sell, true) => {
                    desired_asks.push((reduce_px, reduce_sz));
                }
                _ => {}
            }
            for i in 0..params.level_notionals.len() {
                let (bid_px, bid_sz) = params.get_increasing_order(
                    Direction::Buy,
                    i,
                    &state.orderbook,
                    &state.balance,
                    &state.ticker,
                );
                let (ask_px, ask_sz) = params.get_increasing_order(
                    Direction::Sell,
                    i,
                    &state.orderbook,
                    &state.balance,
                    &state.ticker,
                );
                desired_bids.push((bid_px, bid_sz));
                desired_asks.push((ask_px, ask_sz));
            }

            drop(params);

            let bid_exec =
                self.execute_desired(&state, Direction::Buy, &desired_bids, &state.open_bids);
            let ask_exec =
                self.execute_desired(&state, Direction::Sell, &desired_asks, &state.open_asks);

            let res = tokio::join!(bid_exec, ask_exec);

            tps_left -= (desired_bids.len() + desired_asks.len() + 1) as i64;
            if tps_left <= 1 {
                tracing::warn!("TPS limit reached, waiting for refill");
                tps_left -= 1;
                self.client
                    .cancel_by_instrument(state.subaccount_id, state.ticker.instrument_name.clone())
                    .await?
                    .into_result()?;
            }
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let run_market = self.run_market();
        let run_maker = self.run_maker();
        let run_ping = self.client.ping_interval(15);
        let res = tokio::select! {
            _ = run_ping => {Err(Error::msg("Ping interval exited early"))},
            _ = run_market => {Err(Error::msg("Market subscription exited early"))},
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
