#!/usr/bin/env python3
"""
Daily buy of the highest buy/sell ratio USD asset on Coinbase.

Flow
----
1) List tradable USD products from Advanced Trade (paginated):
     GET /api/v3/brokerage/products
2) Keep top-N by 24h volume (reduces API calls).
3) For each candidate, fetch most recent public trades from the Exchange API:
     GET https://api.exchange.coinbase.com/products/{product_id}/trades?limit=100
   (Now paginated with `before=<trade_id>` to gather up to TRADES_LIMIT trades.)
4) Compute buy_ratio = buy_base_volume / (buy + sell base volume).
5) Pick the highest buy_ratio (tie-break by 24h volume) and place a BUY using:
   - If BUY_PCT > 0: percent of available quote balance (buying power)
   - Else: fixed BUY_USD
   Market IOC via Advanced Trade, scoped to your portfolio via QUERY PARAM.

Env
---
COINBASE_API_KEY=...
COINBASE_API_SECRET=...

# Choose one:
BUY_PCT=0.05                       # e.g., 0.05 = 5% of available quote balance
BUY_USD=5                          # fallback if BUY_PCT<=0 or unset

QUOTE_CURRENCY=USD
PORTFOLIO_UUID=<uuid>              # preferred (routes order to that portfolio)
PORTFOLIO_NAME=bot                 # used only if UUID not set

MAX_PRODUCTS=60                    # scan top-N by 24h volume
TRADES_LIMIT=1000                  # total recent trades to consider per product (will paginate)
MIN_TRADES=30                      # minimum trades to accept ratio
DENYLIST=USDC,USDT,EURT,WBTC       # skip these tickers by default
ALLOWLIST=                          # if set, only pick from these tickers (comma-separated bases)
TOP_TICKER_OVERRIDE=                # e.g., BTC (forces selection)
DEBUG=1

Requires
--------
pip install coinbase-advanced-py>=1.6.3 requests>=2.32.0
"""

import os
import time
import uuid
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

import requests
from coinbase.rest import RESTClient

# ---------------- Config ----------------
BUY_PCT_STR     = os.getenv("BUY_PCT", "").strip()
BUY_USD_STR     = os.getenv("BUY_USD", "5").strip()
QUOTE           = os.getenv("QUOTE_CURRENCY", "USD").upper().strip()
PORTFOLIO_UUID  = os.getenv("PORTFOLIO_UUID", "").strip()
PORTFOLIO_NAME  = os.getenv("PORTFOLIO_NAME", "bot").strip() if not PORTFOLIO_UUID else ""
MAX_PRODUCTS    = int(os.getenv("MAX_PRODUCTS", "60"))
TRADES_LIMIT    = int(os.getenv("TRADES_LIMIT", "100"))
MIN_TRADES      = int(os.getenv("MIN_TRADES", "30"))
TOP_OVERRIDE    = os.getenv("TOP_TICKER_OVERRIDE", "").strip().upper()
DENYLIST        = {s.strip().upper() for s in os.getenv("DENYLIST", "USDC,USDT,EURT,WBTC").split(",") if s.strip()}
ALLOWLIST_RAW   = os.getenv("ALLOWLIST", "").strip()
ALLOWLIST       = {s.strip().upper() for s in ALLOWLIST_RAW.split(",") if s.strip()} if ALLOWLIST_RAW else set()
DEBUG           = os.getenv("DEBUG", "0").lower() not in ("0", "false", "no", "off", "")

client = RESTClient()  # uses COINBASE_API_KEY / COINBASE_API_SECRET

# ---------------- Logging ----------------
def log(msg: str) -> None:
    print(f"[cb-buyratio] {msg}", flush=True)

def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[cb-buyratio][debug] {msg}", flush=True)

# ---------------- Helpers ----------------
def D(x: str | Decimal | float | int) -> Decimal:
    try:
        return Decimal(str(x))
    except InvalidOperation:
        raise SystemExit(f"Invalid decimal: {x}")

def _get(o: Any, k: str, default=None):
    if isinstance(o, dict):
        return o.get(k, default)
    return getattr(o, k, default)

def ensure_portfolio_uuid() -> Optional[str]:
    """Return portfolio UUID; prefer env, else look up by name."""
    global PORTFOLIO_UUID
    if PORTFOLIO_UUID:
        return PORTFOLIO_UUID
    try:
        res = client.get("/api/v3/brokerage/portfolios")
        ports = _get(res, "portfolios") or _get(res, "data") or []
        for p in ports:
            if str(_get(p, "name") or "").strip().lower() == PORTFOLIO_NAME.lower():
                PORTFOLIO_UUID = _get(p, "uuid") or _get(p, "portfolio_uuid")
                if PORTFOLIO_UUID:
                    log(f"Using portfolio '{PORTFOLIO_NAME}' ({PORTFOLIO_UUID})")
                    return PORTFOLIO_UUID
        log(f"Portfolio named '{PORTFOLIO_NAME}' not found; order will be unscoped (default portfolio).")
    except Exception as e:
        log(f"Error listing portfolios: {e}")
    return None

# --------- Increments / balances for correct notional sizing ----------
def get_product_meta(pid: str) -> Dict[str, Decimal]:
    p = client.get_product(product_id=pid)
    # quote_min_size may be absent on some products; default to 0
    qmin = getattr(p, "quote_min_size", None)
    return {
        "price_inc": D(getattr(p, "price_increment", "0")),
        "base_inc":  D(getattr(p, "base_increment", "0")),
        "quote_inc": D(getattr(p, "quote_increment", "0")),
        "base_ccy":  getattr(p, "base_currency_id"),
        "quote_ccy": getattr(p, "quote_currency_id"),
        "quote_min": D(qmin) if qmin is not None else D("0"),
    }

def round_to_inc(value: Decimal, inc: Decimal) -> Decimal:
    if inc is None or inc <= 0:
        return value
    return (value / inc).to_integral_value(rounding=ROUND_DOWN) * inc

def get_quote_available(quote_ccy: str) -> Decimal:
    accs = client.get_accounts()
    for a in getattr(accs, "accounts", []):
        if getattr(a, "currency", "").upper() == quote_ccy:
            vb = a.available_balance
            return D(vb["value"] if isinstance(vb, dict) else vb)
    return D("0")

# ---------------- Advanced Trade: products (paginated) ----------------
def fetch_all_products() -> List[dict]:
    items: List[dict] = []
    cursor = None
    while True:
        params = {"limit": 250}
        if cursor:
            params["cursor"] = cursor
        res = client.get("/api/v3/brokerage/products", params=params)
        page = _get(res, "products") or _get(res, "data") or []
        items.extend(page)
        cursor = _get(res, "cursor") or _get(res, "next") or _get(res, "next_cursor")
        if not cursor:
            break
    return items

def fetch_usd_products() -> List[dict]:
    """Get tradable online USD products, honoring allow/deny lists, sorted by 24h volume."""
    try:
        prods = fetch_all_products()
    except Exception:
        # Fallback to SDK helper if needed
        res = client.get_products()
        prods = _get(res, "products") or _get(res, "data") or res

    usd_products: List[dict] = []
    for p in prods:
        pid    = str(_get(p, "product_id") or "")
        quote  = str(_get(p, "quote_currency_id") or _get(p, "quote_currency") or "").upper()
        base   = str(_get(p, "base_currency_id")  or _get(p, "base_currency")  or "").upper()
        status = str(_get(p, "status") or _get(p, "status_message") or "online").lower()
        tradable = bool(_get(p, "is_tradable", True))
        if not pid or quote != QUOTE or not tradable or "offline" in status:
            continue
        if ALLOWLIST and base not in ALLOWLIST:
            continue
        if base in DENYLIST:
            continue
        usd_products.append(p)

    def dec(v) -> Decimal:
        try:
            return D(str(v))
        except Exception:
            return Decimal(0)

    usd_products.sort(key=lambda p: dec(_get(p, "volume_24h")), reverse=True)
    if MAX_PRODUCTS and len(usd_products) > MAX_PRODUCTS:
        usd_products = usd_products[:MAX_PRODUCTS]

    dbg(f"USD products considered: {len(usd_products)} (top by 24h volume)")
    return usd_products

# ---------------- Exchange public API: trades (paginated) ----------------
EXCHANGE_BASE = "https://api.exchange.coinbase.com"
EXCHANGE_TRADES_MAX = 100  # per-request cap

def get_trades_exchange(pid: str, total_limit: int) -> List[dict]:
    """
    Fetch up to `total_limit` recent public trades from Coinbase Exchange Market Data API.
    Uses backward pagination with `before=<trade_id>` (newest-first).
    Example page:
      GET /products/BTC-USD/trades?limit=100
      GET /products/BTC-USD/trades?limit=100&before=<last_trade_id_from_previous_page>
    """
    url = f"{EXCHANGE_BASE}/products/{pid}/trades"
    headers = {"User-Agent": "cb-buyratio-bot/1.0", "Accept": "application/json"}

    out: List[dict] = []
    before: Optional[int] = None
    remaining = max(1, int(total_limit))

    while remaining > 0:
        page_limit = min(remaining, EXCHANGE_TRADES_MAX)
        params = {"limit": page_limit}
        if before is not None:
            params["before"] = before
        try:
            r = requests.get(url, headers=headers, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            items = data if isinstance(data, list) else []
            if not items:
                break
            out.extend(items)
            remaining -= len(items)
            # last item is the oldest in this page; page backwards using its trade_id
            last = items[-1]
            before = last.get("trade_id") or last.get("tradeId")
            if before is None or len(items) < page_limit:
                break
            time.sleep(0.05)  # gentle on rate limits
        except Exception as e:
            dbg(f"{pid} | exchange trades fetch error: {e}")
            break

    return out

def compute_buy_ratio_from_trades(trades: List[dict]) -> Tuple[Optional[Decimal], int, Decimal, Decimal]:
    """Return (buy_ratio [0..1], n_trades, buy_vol, sell_vol); None if insufficient trades."""
    buy_vol = Decimal(0)
    sell_vol = Decimal(0)
    n = 0
    for t in trades:
        side = str(_get(t, "side") or "").lower()   # 'buy' / 'sell'
        raw_size = _get(t, "size") or _get(t, "base_size")
        try:
            sz = D(str(raw_size))
        except Exception:
            continue
        if sz <= 0:
            continue
        if side == "buy":
            buy_vol += sz
            n += 1
        elif side == "sell":
            sell_vol += sz
            n += 1

    total = buy_vol + sell_vol
    if n < MIN_TRADES or total <= 0:
        return (None, n, buy_vol, sell_vol)
    return (buy_vol / total, n, buy_vol, sell_vol)

def pick_top_by_ratio(products: List[dict]) -> Optional[Tuple[str, str, Decimal, int]]:
    """
    Compute ratio for each candidate using Exchange trades.
    Returns (base_symbol, product_id, ratio, n_trades) for the best.
    Tie-breaker: 24h volume (desc).
    """
    best: Optional[Tuple[str, str, Decimal, int, Decimal]] = None  # (base, pid, ratio, n_trades, vol24h)

    def dec(v) -> Decimal:
        try:
            return D(str(v))
        except Exception:
            return Decimal(0)

    for p in products:
        pid  = str(_get(p, "product_id") or "")
        base = str(_get(p, "base_currency_id") or _get(p, "base_currency") or "").upper()
        vol24= dec(_get(p, "volume_24h"))
        if not pid or not base:
            continue

        trades = get_trades_exchange(pid, TRADES_LIMIT)
        ratio, n, buy_vol, sell_vol = compute_buy_ratio_from_trades(trades)
        if ratio is None:
            dbg(f"{pid} | insufficient trades (n={n}); skip.")
            continue

        dbg(f"{pid} | ratio={ratio:.4f} n={n} buy_vol={buy_vol} sell_vol={sell_vol}")
        if best is None or (ratio > best[2]) or (ratio == best[2] and vol24 > best[4]):
            best = (base, pid, ratio, n, vol24)

        time.sleep(0.05)  # gentle on rate limits across products

    return (best[0], best[1], best[2], best[3]) if best else None

# ---------------- Trading (Advanced Trade) ----------------
def _make_order_payload(product_id: str, usd_amount: Decimal) -> dict:
    return {
        "product_id": product_id,
        "side": "BUY",
        "client_order_id": str(uuid.uuid4()),  # valid UUIDv4
        "order_configuration": {
            "market_market_ioc": {"quote_size": f"{usd_amount.normalize():f}"}
        },
    }

def place_market_buy(product_id: str, usd_amount: Decimal, portfolio_uuid: Optional[str]) -> None:
    """
    Submit a market IOC BUY using quote_size=$, scoped by portfolio via QUERY PARAM.
    NOTE: 'portfolio_id' must NOT be in the JSON body (it will 400).
    """
    params = {}
    if portfolio_uuid:
        params["portfolio_id"] = portfolio_uuid

    payload = _make_order_payload(product_id, usd_amount)

    try:
        resp = client.post("/api/v3/brokerage/orders", params=params, data=payload)
        oid = (_get(resp, "order_id") or _get(resp, "orderId")
               or _get(_get(resp, "success_response", {}) or {}, "order_id"))
        log(f"{product_id} | BUY ${usd_amount} submitted (order {oid})")
        return
    except Exception as e:
        # If server complains about client_order_id, retry once with a fresh UUID
        msg = str(e)
        if "client_order_id" in msg:
            dbg(f"{product_id} | retrying with fresh client_order_id due to error: {e}")
            payload = _make_order_payload(product_id, usd_amount)
            resp = client.post("/api/v3/brokerage/orders", params=params, data=payload)
            oid = (_get(resp, "order_id") or _get(resp, "orderId")
                   or _get(_get(resp, "success_response", {}) or {}, "order_id"))
            log(f"{product_id} | BUY ${usd_amount} submitted on retry (order {oid})")
            return
        log(f"{product_id} | BUY failed: {type(e).__name__}: {e}")
        raise

# ---------------- Main ----------------
def main():
    # Decide sizing mode
    buy_pct = D(BUY_PCT_STR) if BUY_PCT_STR else D("0")
    usd_amt_fixed = D(BUY_USD_STR)

    eff_per_req = 100  # Exchange per-request max
    log(f"Started | mode={'PCT' if buy_pct > 0 else 'USD'} | buy_pct={buy_pct} | buy_usd={usd_amt_fixed} | quote={QUOTE} | "
        f"max_products={MAX_PRODUCTS} | trades_limit_req={TRADES_LIMIT} | trades_limit_eff_per_req={eff_per_req} | min_trades={MIN_TRADES}")
    pf = ensure_portfolio_uuid()

    if TOP_OVERRIDE:
        forced_pid = f"{TOP_OVERRIDE}-{QUOTE}"
        log(f"Using TOP_TICKER_OVERRIDE={TOP_OVERRIDE} -> {forced_pid}")
        choice = (TOP_OVERRIDE, forced_pid, Decimal("1"), 0)
    else:
        products = fetch_usd_products()
        if not products:
            log("No USD products found; aborting.")
            raise SystemExit(1)

        choice = pick_top_by_ratio(products)
        if not choice:
            log("Could not determine a top symbol (not enough trades or API blocked); aborting.")
            raise SystemExit(1)

    base, pid, ratio, n = choice
    log(f"Selected top: {base} via {pid} (buy_ratio={ratio:.2%}, trades={n})")

    # Build notional based on mode (percent of buying power vs fixed)
    meta = get_product_meta(pid)
    quote_bal = get_quote_available(meta["quote_ccy"])
    if buy_pct > 0:
        usd_amt = round_to_inc(quote_bal * buy_pct, meta["quote_inc"])
        if usd_amt <= 0:
            log(f"{pid} | Computed notional rounds to 0 (quote_bal={quote_bal}, pct={buy_pct}); aborting.")
            raise SystemExit(1)
        if meta["quote_min"] and usd_amt < meta["quote_min"]:
            log(f"{pid} | Notional {usd_amt} < quote_min {meta['quote_min']}; aborting.")
            raise SystemExit(1)
        log(f"{pid} | quote_bal={quote_bal} {meta['quote_ccy']} | pct={buy_pct} -> buy_notional=${usd_amt}")
    else:
        usd_amt = D(usd_amt_fixed)
        if usd_amt <= 0:
            raise SystemExit("BUY_USD must be > 0")
        usd_amt = round_to_inc(usd_amt, meta["quote_inc"])
        if meta["quote_min"] and usd_amt < meta["quote_min"]:
            log(f"{pid} | Notional {usd_amt} < quote_min {meta['quote_min']}; aborting.")
            raise SystemExit(1)
        log(f"{pid} | fixed buy_notional=${usd_amt}")

    place_market_buy(pid, usd_amt, pf)
    log("Done.")

if __name__ == "__main__":
    main()
