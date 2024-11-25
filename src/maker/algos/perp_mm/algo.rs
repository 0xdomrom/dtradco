use crate::maker::algos::perp_mm::PerpMMParams;
use crate::maker::market::data::OrderbookData;
use bigdecimal::{BigDecimal, One, RoundingMode, Zero};
use orderbook_types::types::orders::Direction;
use orderbook_types::types::tickers::InstrumentTicker;

impl PerpMMParams {
    /// returns (price, amount, direction) to quote for the risk reducing level
    pub fn get_reducing_order(
        &self,
        orderbook: &OrderbookData,
        balance: &BigDecimal,
        ticker: &InstrumentTicker,
    ) -> (BigDecimal, BigDecimal, Direction) {
        let zero = BigDecimal::zero();
        let one = BigDecimal::one();
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

        let best_bid = orderbook.bids.get(0).map(|x| x[0].clone());
        let best_ask = orderbook.asks.get(0).map(|x| x[0].clone());

        // "smooth" the bbo if it is unreasonably wide
        let best_bid_approx = mark * (&one - &self.max_spread_to_mid);
        let best_ask_approx = mark * (&one + &self.max_spread_to_mid);
        let best_bid = best_bid.unwrap_or(BigDecimal::zero()).max(best_bid_approx);
        let best_ask = best_ask
            .unwrap_or(ticker.max_price.clone())
            .min(best_ask_approx);

        let this_side_best = match side {
            Direction::Buy => &best_bid,
            Direction::Sell => &best_ask,
        };
        let other_side_best = match side {
            Direction::Buy => &best_ask,
            Direction::Sell => &best_bid,
        };

        // long means selling to reduce -> price down, short means buying to close -> price up
        let slippage = -&self.reducing_slippage * &exposure;
        // negative reducing_spread & buying -> price up, selling -> price down
        let spread = -&self.reducing_spread * mark * side.sign();
        let rounding = match side {
            Direction::Buy => RoundingMode::Down,
            Direction::Sell => RoundingMode::Up,
        };
        let price = (this_side_best + slippage + spread)
            .min(ticker.max_price.clone())
            .max(ticker.min_price.clone())
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
    pub fn get_level_amount(
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
        let prev_levels_exposure: BigDecimal = match direction {
            Direction::Buy => self.level_notionals.iter().take(level).sum::<BigDecimal>(),
            Direction::Sell => -self.level_notionals.iter().take(level).sum::<BigDecimal>(),
        };
        let notional_decay = match is_risk_increasing {
            true => {
                let exposure = balance * mark + prev_levels_exposure;
                self.get_notional_decay(&exposure)
            }
            false => self.get_notional_decay(&prev_levels_exposure),
        };
        let level_notional = level_notional * notional_decay;
        let level_amount = level_notional / mark;
        level_amount
            .with_scale_round(
                ticker.amount_step.fractional_digit_count(),
                RoundingMode::Down,
            )
            .min(ticker.minimum_amount.clone())
    }
    /// returns price to quote for the risk increasing level
    pub fn get_level_price(&self, level: usize) -> BigDecimal {
        // TODO
        BigDecimal::zero()
    }
}
