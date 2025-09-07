#!/usr/bin/env python3
"""
Daily buy of the highest buy/sell ratio USD asset on Coinbase.

What it does
------------
1) Lists tradable, online USD products from Advanced Trade:
     GET /api/v3/brokerage/products   (with pagination)
2) Keeps the top-N by 24h volume (reduce API calls).
3) For each candidate, fetches most recent public trades from the Exchange API:
     GET https://api.exchange.coinbase.com/products/{product_id}/trades
   (no auth required; pure market data)
4) Computes buy_ratio = buy_base_volume / (buy + sell base volume).
5) Picks the highest buy_ratio (tie-break by 24h volume) and places a $ BUY
   as a market IOC via Advanced Trade.

Env
---
COINBASE_API_KEY=...
COINBASE_API_SECRET=...

BUY_USD=5
QUOTE_CURRENCY=USD
PORTFOLIO_UUID=<uuid>                # preferred
PORTFOLIO_NAME=bot                   # used only if UUID not set

# Universe controls
MAX_PRODUCTS=60                      # scan top-N by 24h volume
TRADES_LIMIT=100                     # Exchange public trades endpoint returns up to ~100 per call
MIN_TRADES=30                        # require at least this many trades for a valid ratio
DENYLIST=USDC,USDT,EURT,WBTC         # skip these tickers
ALLOWLIST=                            # if set, only pick from these tickers
TOP_TICKER_OVERRIDE=                  # e.g., BTC (bypass selection)

DEBUG=1

Requirements
------------
pip install coinbase-advanced-py>=1.6.3 requests>=2.32.0
"""

import os
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import requests
from coinbase.rest import RESTClient

# ---------------- Config ----------------
BUY_USD_STR     = os.getenv("BUY_USD", "5").strip()
QUOTE           = os.getenv("QUOTE_CURRENCY", "USD").upper().strip()
PORTFOLIO_UUID  = os.getenv("PORTFOLIO_UUID", "").strip()
PORTFOLIO_NAME  = os.getenv("PORTFOLIO_NAME", "bot").strip() if not PORTFOLIO_UUID else ""
MAX_PRODUCTS    = int(os.getenv("MAX_PRODUCTS", "60"))
TRADES_LIMIT    = int(os.getenv("TRADES_LIMIT", "100"))   # Exchange API typical max ~100
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
def D(x: str) -> Decimal:
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

# ---------------- Advanced Trade: products (correct path, with pagination) ----------------
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

    usd_products = []
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

# ---------------- Exchange public API: trades ----------------
EXCHANGE_BASE = "https://api.exchange.coinbase.com"

def get_trades_exchange(pid: str, limit: int) -> List[dict]:
    """
    Fetch recent public trades from Coinbase Exchange market data API.
    Example: GET https://api.exchange.coinbase.com/products/BTC-USD/trades?limit=100
    """
    url = f"{EXCHANGE_BASE}/products/{pid}/trades"
    headers = {
        "User-Agent": "cb-buyratio-bot/1.0",
        "Accept": "application/json",
    }
    params = {"limit": max(1, min(limit, 100))}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        return []
    except Exception as e:
        dbg(f"{pid} | exchange trades fetch error: {e}")
        return []

def compute_buy_ratio_from_trades(trades: List[dict]) -> Tuple[Optional[Decimal], int, Decimal, Decimal]:
    """Return (buy_ratio [0..1], n_trades, buy_vol, sell_vol); None if insufficient trades."""
    buy_vol = Decimal(0)
    sell_vol = Decimal(0)
    n = 0
    for t in trades:
        side = str(_get(t, "side") or "").upper()   # 'BUY'/'SELL' on Exchange API is usually lower-case
        if side == "BUY" or side == "buy":
            side = "BUY"
        elif side == "SELL" or side == "sell":
            side = "SELL"
        else:
            continue
        raw_size = _get(t, "size") or _get(t, "base_size")
        try:
            sz = D(str(raw_size))
        except Exception:
            continue
        if sz <= 0:
            continue
        if side == "BUY":
            buy_vol += sz
        else:
            sell_vol += sz
        n += 1

    total = buy_vol + sell_vol
    if n < MIN_TRADES or total <= 0:
        return (None, n, buy_vol, sell_vol)
    return (buy_vol / total, n, buy_vol, sell_vol)

def pick_top_by_ratio(products: List[dict]) -> Optional[Tuple[str, Decimal, int]]:
    """
    Compute ratio for each candidate using Exchange trades, return (symbol, ratio, n_trades).
    Tie-breaker: 24h volume (desc).
    """
    best: Optional[Tuple[str, Decimal, int, Decimal]] = None  # (base, ratio, n_trades, vol24h)

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
        if best is None or (ratio > best[1]) or (ratio == best[1] and vol24 > best[3]):
            best = (base, ratio, n, vol24)

        time.sleep(0.05)  # gentle on rate limits

    return (best[0], best[1], best[2]) if best else None

# ---------------- Trading (Advanced Trade order) ----------------
def place_market_buy(symbol: str, usd_amount: Decimal, portfolio_uuid: Optional[str]) -> None:
    pid = f"{symbol}-{QUOTE}"
    payload = {
        "product_id": pid,
        "side": "BUY",
        "order_configuration": {"market_market_ioc": {"quote_size": f"{usd_amount.normalize():f}"}},
    }
    if portfolio_uuid:
        payload["portfolio_id"] = portfolio_uuid
    try:
        resp = client.post("/api/v3/brokerage/orders", data=payload)
        oid = (_get(resp, "order_id") or _get(resp, "orderId")
               or _get(_get(resp, "success_response", {}) or {}, "order_id"))
        log(f"{pid} | BUY ${usd_amount} submitted (order {oid})")
    except Exception as e:
        log(f"{pid} | BUY failed: {type(e).__name__}: {e}")
        raise

# ---------------- Main ----------------
def main():
    usd_amt = D(BUY_USD_STR)
    if usd_amt <= 0:
        raise SystemExit("BUY_USD must be > 0")

    log(f"Started | buy=${usd_amt} | quote={QUOTE} | max_products={MAX_PRODUCTS} | trades_limit={TRADES_LIMIT} | min_trades={MIN_TRADES}")
    pf = ensure_portfolio_uuid()

    if TOP_OVERRIDE:
        log(f"Using TOP_TICKER_OVERRIDE={TOP_OVERRIDE}")
        sym = TOP_OVERRIDE
    else:
        products = fetch_usd_products()
        if not products:
            log("No USD products found; aborting.")
            raise SystemExit(1)

        choice = pick_top_by_ratio(products)
        if not choice:
            log("Could not determine a top symbol (not enough trades or API blocked); aborting.")
            raise SystemExit(1)

        sym, ratio, n = choice
        log(f"Selected top ticker: {sym} (buy_ratio={ratio:.2%}, trades={n})")

    place_market_buy(sym, usd_amt, pf)
    log("Done.")

if __name__ == "__main__":
    main()
