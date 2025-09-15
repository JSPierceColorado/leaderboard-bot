#!/usr/bin/env python3
"""
Perpetual 15m-bar scanner & buyer for ALL Coinbase USD markets (closed-only candles).

Rules (15m timeframe):
- RSI(14) <= 30
- SMA(60) < SMA(240)

Action:
- Market IOC buy using 5% of CURRENT available USD per qualifying asset.
- No caps/limits on number of products scanned or buys.

Behavior:
- Runs forever. Aligns to each 15m bar close (00, 15, 30, 45 past the hour).
- Scans all USD-quoted products (no MAX cap).
- Intentionally allows repeat buys whenever the signal is met.
- Uses **closed-only** candles to match charting platforms.

Env
---
COINBASE_API_KEY=...
COINBASE_API_SECRET=...

BUY_PCT=0.05                       # 5% of available USD per qualifying asset
BUY_USD=0                          # ignored if BUY_PCT>0
QUOTE_CURRENCY=USD
PORTFOLIO_UUID=<uuid>              # preferred (routes order to that portfolio)
PORTFOLIO_NAME=bot                 # used only if UUID not set

# No cap by default:
MAX_PRODUCTS=0                     # 0 = scan ALL USD products (no limit)

# Lists are optional; defaults let you scan everything:
DENYLIST=
ALLOWLIST=

# How often to refresh product list (seconds):
PRODUCT_REFRESH_SECS=3600

# Set DEBUG=1 to see per-asset metrics each bar:
DEBUG=1

# Force closed-only behavior (recommended: leave ON)
STRICT_CLOSED_ONLY=1

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
BUY_PCT_STR     = os.getenv("BUY_PCT", "0.05").strip()
BUY_USD_STR     = os.getenv("BUY_USD", "0").strip()
QUOTE           = os.getenv("QUOTE_CURRENCY", "USD").upper().strip()
PORTFOLIO_UUID  = os.getenv("PORTFOLIO_UUID", "").strip()
PORTFOLIO_NAME  = os.getenv("PORTFOLIO_NAME", "bot").strip() if not PORTFOLIO_UUID else ""
MAX_PRODUCTS    = int(os.getenv("MAX_PRODUCTS", "0"))  # 0 = no cap
DENYLIST        = {s.strip().upper() for s in os.getenv("DENYLIST", "").split(",") if s.strip()}
ALLOWLIST_RAW   = os.getenv("ALLOWLIST", "").strip()
ALLOWLIST       = {s.strip().upper() for s in ALLOWLIST_RAW.split(",") if s.strip()} if ALLOWLIST_RAW else set()
PRODUCT_REFRESH_SECS = int(os.getenv("PRODUCT_REFRESH_SECS", "3600"))
DEBUG           = os.getenv("DEBUG", "0").lower() not in ("0", "false", "no", "off", "")
STRICT_CLOSED_ONLY = os.getenv("STRICT_CLOSED_ONLY", "1").lower() not in ("0","false","no","off","")

client = RESTClient()  # uses COINBASE_API_KEY / COINBASE_API_SECRET

# ---------------- Logging ----------------
def log(msg: str) -> None:
    print(f"[cb-rsi-buyer-live] {msg}", flush=True)

def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[cb-rsi-buyer-live][debug] {msg}", flush=True)

def fmt_ts(ts: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(ts))

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
    """Get tradable online USD products (optionally honor allow/deny lists), sorted by 24h volume."""
    try:
        prods = fetch_all_products()
    except Exception:
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
    if MAX_PRODUCTS and MAX_PRODUCTS > 0 and len(usd_products) > MAX_PRODUCTS:
        usd_products = usd_products[:MAX_PRODUCTS]

    dbg(f"USD products considered: {len(usd_products)}")
    return usd_products

# ---------------- Exchange public API: 15m candles (CLOSED ONLY) ----------------
EXCHANGE_BASE = "https://api.exchange.coinbase.com"
FIFTEEN_MIN = 900

def last_closed_bar_boundary(ts: Optional[int] = None) -> int:
    """Return the UNIX epoch for the *start* of the most recently CLOSED 15m bar."""
    if ts is None:
        ts = int(time.time())
    current_bucket = (ts // FIFTEEN_MIN) * FIFTEEN_MIN
    return current_bucket - FIFTEEN_MIN

def get_candles_15m(pid: str, bars_needed: int = 300) -> List[Tuple[int, float, float, float, float, float]]:
    """
    Return recent 15m candles for product_id, oldest -> newest.
    Each item: (time, low, high, open, close, volume)
    Uses **closed-only** candles if STRICT_CLOSED_ONLY is True.
    """
    granularity = FIFTEEN_MIN

    if STRICT_CLOSED_ONLY:
        end_ts = last_closed_bar_boundary()
    else:
        end_ts = int(time.time())

    start_ts = end_ts - granularity * max(bars_needed, 300)

    params = {"granularity": granularity, "start": start_ts, "end": end_ts}
    url = f"{EXCHANGE_BASE}/products/{pid}/candles"
    headers = {"User-Agent": "cb-rsi-buyer-live/1.0", "Accept": "application/json"}

    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            return []
        data.sort(key=lambda x: x[0])  # oldest first

        # Extra guard: drop any candle at or after end_ts (shouldn't appear, but be safe)
        if STRICT_CLOSED_ONLY:
            data = [row for row in data if int(row[0]) < end_ts]

        return [(int(t), float(lo), float(hi), float(op), float(cl), float(v)) for t, lo, hi, op, cl, v in data]
    except Exception as e:
        dbg(f"{pid} | candles fetch error: {e}")
        return []

# ---------------- Indicators (RSI, SMA) ----------------
def sma(values: List[float], length: int) -> Optional[float]:
    if len(values) < length:
        return None
    return sum(values[-length:]) / float(length)

def rsi_wilder_14(closes: List[float], length: int = 14) -> Optional[float]:
    """Wilder's RSI(14) on closes; returns latest RSI value."""
    if len(closes) < length + 1:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, length + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains) / length
    avg_loss = sum(losses) / length
    for i in range(length + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gain = max(diff, 0.0)
        loss = max(-diff, 0.0)
        avg_gain = (avg_gain * (length - 1) + gain) / length
        avg_loss = (avg_loss * (length - 1) + loss) / length
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

# ---------------- Trading (Advanced Trade) ----------------
def _make_order_payload(product_id: str, usd_amount: Decimal) -> dict:
    return {
        "product_id": product_id,
        "side": "BUY",
        "client_order_id": str(uuid.uuid4()),
        "order_configuration": {
            "market_market_ioc": {"quote_size": f"{usd_amount.normalize():f}"}
        },
    }

def place_market_buy(product_id: str, usd_amount: Decimal, portfolio_uuid: Optional[str]) -> None:
    params = {}
    if portfolio_uuid:
        params["portfolio_id"] = portfolio_uuid
    payload = _make_order_payload(product_id, usd_amount)
    try:
        resp = client.post("/api/v3/brokerage/orders", params=params, data=payload)
        oid = (_get(resp, "order_id") or _get(resp, "orderId")
               or _get(_get(resp, "success_response", {}) or {}, "order_id"))
        log(f"{product_id} | BUY ${usd_amount} submitted (order {oid})")
    except Exception as e:
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

# ---------------- Scheduling: align to 15m bars ----------------
def next_bar_epoch(now: Optional[int] = None) -> int:
    """Return the UNIX epoch for the *next* 15m bar boundary."""
    FIFTEEN_MIN = 900
    if now is None:
        now = int(time.time())
    return ((now // FIFTEEN_MIN) + 1) * FIFTEEN_MIN

def sleep_until(ts: int, pad_seconds: int = 2) -> None:
    """Sleep until ts + small pad; candles fetch still uses last CLOSED bar."""
    delay = ts - int(time.time()) + pad_seconds
    if delay > 0:
        time.sleep(delay)

# ---------------- Main perpetual loop ----------------
def main():
    buy_pct = D(BUY_PCT_STR) if BUY_PCT_STR else D("0.05")
    usd_amt_fixed = D(BUY_USD_STR)  # ignored if buy_pct > 0

    mode = 'PCT' if buy_pct > 0 else 'USD'
    log(f"Started LIVE | mode={mode} | buy_pct={buy_pct} | quote={QUOTE} | no caps/limits | closed_only={STRICT_CLOSED_ONLY}")
    pf = ensure_portfolio_uuid()

    products: List[dict] = []
    products_last_refresh = 0

    while True:
        # refresh USD products if needed
        now = int(time.time())
        if not products or (now - products_last_refresh) >= PRODUCT_REFRESH_SECS:
            products = fetch_usd_products()
            products_last_refresh = now
            log(f"Products refreshed: {len(products)} USD markets")

        # Wait for the next bar boundary; candles function will use last CLOSED bar
        boundary = next_bar_epoch(now)
        sleep_until(boundary, pad_seconds=3)

        cycle_start = int(time.time())
        total_signals = 0
        total_buys = 0

        for p in products:
            pid  = str(_get(p, "product_id") or "")
            base = str(_get(p, "base_currency_id") or _get(p, "base_currency") or "").upper()
            if not pid or not base:
                continue

            candles = get_candles_15m(pid, bars_needed=300)
            if len(candles) < 240 + 14 + 1:
                dbg(f"{pid} | insufficient candles ({len(candles)})")
                continue

            closes = [c[4] for c in candles]
            latest_bar_ts = candles[-1][0]  # should be the start time of the last CLOSED bar
            latest_close = closes[-1]

            rsi14 = rsi_wilder_14(closes, 14)
            sma60 = sma(closes, 60)
            sma240 = sma(closes, 240)
            if rsi14 is None or sma60 is None or sma240 is None:
                dbg(f"{pid} | metrics unavailable rsi14={rsi14} sma60={sma60} sma240={sma240}")
                continue

            cond = (rsi14 <= 30.0) and (sma60 < sma240)

            # per-asset visibility
            dbg(
                f"{pid} | bar={fmt_ts(latest_bar_ts)} | close={latest_close:.8f} | "
                f"RSI14={rsi14:.2f} | SMA60={sma60:.8f} | SMA240={sma240:.8f} | "
                f"candles={len(candles)} | signal={cond}"
            )

            if not cond:
                continue

            total_signals += 1

            # Size: 5% of *current* available USD per qualifying asset (no global caps)
            meta = get_product_meta(pid)
            quote_bal = get_quote_available(meta["quote_ccy"])
            if buy_pct > 0:
                usd_amt = round_to_inc(quote_bal * buy_pct, meta["quote_inc"])
            else:
                usd_amt = round_to_inc(D(max(usd_amt_fixed, D("0"))), meta["quote_inc"])

            if usd_amt <= 0:
                log(f"{pid} | Notional rounds to 0 (quote_bal={quote_bal}, pct={buy_pct}); skipping.")
                continue
            if meta["quote_min"] and usd_amt < meta["quote_min"]:
                log(f"{pid} | Notional {usd_amt} < quote_min {meta['quote_min']}; skipping.")
                continue

            log(
                f"{pid} | SIGNAL ✅ | {fmt_ts(latest_bar_ts)} | "
                f"RSI14={rsi14:.2f} <= 30 and SMA60<SMA240 | buy_notional=${usd_amt}"
            )
            try:
                place_market_buy(pid, usd_amt, pf)
                total_buys += 1
            except Exception:
                pass

            time.sleep(0.05)  # gentle pacing

        took = int(time.time()) - cycle_start
        log(f"Bar complete | signals={total_signals} | buys={total_buys} | cycle_time={took}s")

if __name__ == "__main__":
    main()
