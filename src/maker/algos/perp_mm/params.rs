/*
PERP MM ALGO

1. Desired Price Function (aka Marginal Pricing Function)

A: best ask ($)
B: best bid ($)
M = (A + B) / 2: mid market ($)
N: current position (# of contracts)
I: risk increasing slippage (bps per $ exposure, e.g. 0.0001 means $0.60 slippage on $6000 exposure)
R: risk reducing slippage (bps per $ exposure, e.g. 0.0001 means $0.60 slippage on $6000 exposure)

C: fixed spread constant (bps, if negative, will start diming)
IC: max distance to mid when increasing risk (bps)
RC: max distance to Ask (if bidding) or Bid (if asking) when reducing risk (bps)

price_cap_floor =
M * (1 - IC) if (bid && N >= 0)  # don't approach mid too closely when diming
M * (1 + IC) if (ask && N <= 0)  # don't approach mid too closely when diming
A - max(M * C, tick_size) if (bid && N < 0)  # buy up to almost at best ask when reducing risk
B + max(M * C, tick_size) if (ask && N > 0)  # sell down to almost at best bid when reducing risk

P(A, B, N, bid) = min(B - M * C - M * (I * max(N, 0) + R * min(N, 0)), price_cap_floor)
P(A, B, N, ask) = max(A + M * C - M * (I * min(N, 0) + R * max(N, 0)), price_cap_floor)

Multi Level Quoting:
- when quoting multiple levels, we can simply use the same ^ logic except for lower levels
we pretend that X contracts have been filled

i.e., e.g. a level 2 could be:
P(A, B, N + dN, bid)
P(A, B, N - dN, ask)

the values of dN can be set to be the same as the cumulative size of the levels below,
for example if quoting 3 contracts at dN = 0, level 2 can quote as if 3 contracts have been filled.

With this approach, a fill on one level makes it so that level 2 becomes the new level 1
and doesn't need to be updated unless prices move further.

This approach makes order management rather simple: each level can be managed by
a separate task (e.g. for 3 bids and 3 asks: 6 tasks total), and each of them
will auto-update upon seeing changes in the balances.


2. Desired Amount Function.

We would like to quote a certain "minimum" notional on both sides.
To balance the flow a bit better, we could move the size between 3 different levels as
a function of the current inventory N.

We would also have some inventory level Nmax above which we want to start reducing quoted
size due to running into margin issues.

Size quoted goes down linearly between Nmax/2 and Nmax

Define:
Let Vmin = min notional / M (# of contracts), e.g. $30k min notional on $3k mid = 10 contracts

Total size across all levels is then:

Decay(N, bid) =
0 if N < 0
else max(min((Nmax - abs(N)) / (Nmax / 2), 1), 0)

Decay(N, ask) =
0 if N > 0
else max(min((Nmax - abs(N)) / (Nmax / 2), 1), 0)

Vtotal(N, bid) = max(min(-N,0), Vmin * Decay)  # if N is very negative, we want to buy more up to full close
Vtotal(N, ask) = max(max(N,0), Vmin * Decay)  # if N is very positive, we want to sell more down to full close

Furthermore,
if risk-reducing, WE WANT TO SET LEVEL 1 TO BE AT MOST = INVENTORY!!!!
- this way any inventory will get dimed away quickly without overbidding into an opposite position

If risk-increasing, split it equally between the levels.

*/

/*
EDITS
- a single risk reducing task will quote a single level with notional = current inventory
- multiple other tasks will quote risk increasing levels
- on the risk reducing size, level 2 will assume the level 1 (risk reducing level) is fully filled

when risk increasing with a large single level, we should likely incorporate the quoted size
into the pricing

i.e. we would quote the midprice between (price if 0 filled) and (price if fully filled)
equivalently, this is the price s.t. half the size is filled

P(A, B, N + dN[total of prev levels] + dn/2[half size of this level], bid)
P(A, B, N - dN[total of prev levels] - dn/2[half size of this level], ask)

*/
use bigdecimal::BigDecimal;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Deserialize)]
pub struct PerpMMParams {
    pub subaccount_id: i64,
    pub instrument_name: String,

    // risk increasing params
    pub increasing_spread: BigDecimal,
    pub increasing_slippage: BigDecimal,
    /// when quoting risk increasing side and diming, this puts a cap/floor on the price
    pub min_spread_to_mid: BigDecimal,
    /// when markets are super wide, we may want to quote tighter than the current spread
    pub max_spread_to_mid: BigDecimal,

    // risk reducing params
    /// typically negative spread to apply when reducing risk (negative -> bettering the market)
    pub reducing_spread: BigDecimal,
    pub reducing_slippage: BigDecimal,
    /// when quoting risk reducing side and diming, this prevents the price from crossing BBO
    pub min_spread_to_best: BigDecimal,

    /// above this, the quoted size will decay linearly to 0 as it approaches max_exposure
    pub warn_exposure: BigDecimal,
    /// at or > this, the quoted size is 0
    pub max_exposure: BigDecimal,

    /// dollar notionals to quote at each level excluding risk-reducing, e.g. [$3,000,  $27,000]
    pub level_notionals: Vec<BigDecimal>,

    pub max_tps: u64,
    pub price_replace_tol: BigDecimal,  // bps of limit price
    pub amount_replace_tol: BigDecimal, // bps of limit amount
}

pub type PerpMMParamsRef = Arc<RwLock<PerpMMParams>>;

pub fn new_params_ref(params: PerpMMParams) -> PerpMMParamsRef {
    Arc::new(RwLock::new(params))
}
