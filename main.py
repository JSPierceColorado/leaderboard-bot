#!/usr/bin/env python3
"""
Daily buy of the top Coinbase Leaderboard asset — NO Playwright required.

Strategy:
- Fetch the leaderboard HTML with requests.
- Parse the embedded Next.js JSON (__NEXT_DATA__) to find the first row.
- Resolve the ticker (symbol) and place a $ BUY via Coinbase Advanced Trade.
- Scoped to a specific portfolio if PORTFOLIO_UUID is provided (or look up by PORTFOLIO_NAME).

Env:
  COINBASE_API_KEY, COINBASE_API_SECRET
  LEADERBOARD=most-buyers | highest-buy-ratio   (default: most-buyers)
  BUY_USD=5
  QUOTE_CURRENCY=USD
  PORTFOLIO_UUID=<uuid>                         (preferred)
  PORTFOLIO_NAME=bot                            (used only if UUID not set)
  TOP_TICKER_OVERRIDE=                          (e.g., BTC; bypass scraping)
  DEBUG=1
"""

import os
import re
import sys
import json
from typing import Any, Dict, List, Optional
from decimal import Decimal, InvalidOperation

import requests
from coinbase.rest import RESTClient  # pip install coinbase-advanced-py

LEADERBOARD = os.getenv("LEADERBOARD", "most-buyers").strip().lower()
BUY_USD_STR = os.getenv("BUY_USD", "5").strip()
QUOTE       = os.getenv("QUOTE_CURRENCY", "USD").upper().strip()
PORTFOLIO_UUID = os.getenv("PORTFOLIO_UUID", "").strip()
PORTFOLIO_NAME = os.getenv("PORTFOLIO_NAME", "bot").strip() if not PORTFOLIO_UUID else ""
TOP_OVERRIDE   = os.getenv("TOP_TICKER_OVERRIDE", "").strip().upper()
DEBUG          = os.getenv("DEBUG", "0") not in ("0","false","False", "")

URLS = {
    "most-buyers": "https://www.coinbase.com/leaderboards/most-buyers",
    "highest-buy-ratio": "https://www.coinbase.com/leaderboards/highest-buy-ratio",
}

client = RESTClient()  # requires COINBASE_API_KEY / COINBASE_API_SECRET

# ---------------- Logging ----------------
def log(msg: str) -> None:
    print(f"[cb-daily-buy] {msg}", flush=True)

def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[cb-daily-buy][debug] {msg}", flush=True)

# ---------------- Helpers ----------------
def D(x: str) -> Decimal:
    try:
        return Decimal(str(x))
    except InvalidOperation:
        raise SystemExit(f"Invalid decimal: {x}")

def ensure_portfolio_uuid() -> Optional[str]:
    global PORTFOLIO_UUID
    if PORTFOLIO_UUID:
        return PORTFOLIO_UUID
    try:
        res = client.get("/api/v3/brokerage/portfolios")
        ports = res.get("portfolios") or res.get("data") or []
        for p in ports:
            if str(p.get("name","")).strip().lower() == PORTFOLIO_NAME.lower():
                PORTFOLIO_UUID = p.get("uuid") or p.get("portfolio_uuid")
                if PORTFOLIO_UUID:
                    log(f"Using portfolio '{PORTFOLIO_NAME}' ({PORTFOLIO_UUID})")
                    return PORTFOLIO_UUID
        log(f"Portfolio named '{PORTFOLIO_NAME}' not found; order will be unscoped (default portfolio).")
    except Exception as e:
        log(f"Error listing portfolios: {e}")
    return None

def http_get(url: str) -> str:
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text

def extract_nextdata(html: str) -> Optional[dict]:
    """
    Extract Next.js bootstrapped JSON from a <script id="__NEXT_DATA__"> tag.
    """
    # Save debug HTML if requested
    if DEBUG:
        try:
            with open("/tmp/leaderboard.html", "w", encoding="utf-8") as f:
                f.write(html)
            dbg("Saved /tmp/leaderboard.html")
        except Exception as e:
            dbg(f"Saving HTML failed: {e}")

    # Try a precise search first
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, flags=re.DOTALL|re.IGNORECASE)
    if not m:
        # Try generic JSON script (some builds use type="application/json")
        m = re.search(r'<script[^>]+type="application/json"[^>]*>(.*?)</script>', html, flags=re.DOTALL|re.IGNORECASE)
    if not m:
        return None

    raw = m.group(1).strip()
    try:
        data = json.loads(raw)
        # Also dump JSON if debugging
        if DEBUG:
            try:
                with open("/tmp/nextdata.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                dbg("Saved /tmp/nextdata.json")
            except Exception as e:
                dbg(f"Saving JSON failed: {e}")
        return data
    except Exception as e:
        dbg(f"Failed to parse __NEXT_DATA__: {e}")
        return None

def looks_like_row(x: Any) -> bool:
    if not isinstance(x, dict):
        return False
    if x.get("symbol") or x.get("ticker") or x.get("base") or x.get("assetSymbol"):
        return True
    href = str(x.get("href") or x.get("link") or "")
    if href.startswith("/price/"):
        return True
    return False

def find_row_arrays(obj: Any) -> List[List[dict]]:
    found: List[List[dict]] = []
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, list) and any(isinstance(el, dict) and looks_like_row(el) for el in cur):
            found.append(cur)
        if isinstance(cur, dict):
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return found

def map_slug_to_ticker(slug: str) -> Optional[str]:
    """Map a /price/<slug> to a tradable base ticker via products list."""
    try:
        prods = client.get_products()
        items = getattr(prods, "products", None) or getattr(prods, "data", None) or prods
        s = slug.lower()
        best = None
        for p in items:
            base = getattr(p, "base_currency_id", None) or p.get("base_currency_id")
            disp = getattr(p, "display_name", None) or p.get("display_name")
            base_name = getattr(p, "base_display_name", None) or p.get("base_display_name") or ""
            pid = getattr(p, "product_id", None) or p.get("product_id")
            if not base or not pid:
                continue
            tkr = str(base).upper()
            dn  = str(disp or "")
            if s in (tkr.lower(), pid.lower(), pid.split("-")[0].lower(), base_name.lower(), dn.lower()):
                return tkr
            if base_name and s in base_name.lower():
                best = tkr
        return best
    except Exception as e:
        dbg(f"map_slug_to_ticker error: {e}")
        return None

def resolve_row_symbol(row: dict) -> Optional[str]:
    sym = row.get("symbol") or row.get("ticker") or row.get("base") or row.get("assetSymbol")
    if isinstance(sym, str) and 2 <= len(sym) <= 10 and sym.isupper():
        return sym
    href = str(row.get("href") or row.get("link") or "")
    m = re.search(r"/price/([a-z0-9-]+)", href)
    if m:
        return map_slug_to_ticker(m.group(1))
    return None

def parse_nextdata_for_top_symbol(nextdata: dict) -> Optional[str]:
    # Try common roots
    roots = [
        nextdata,
        nextdata.get("props", {}),
        nextdata.get("pageProps", {}),
        (nextdata.get("props", {}) or {}).get("pageProps", {}),
    ]
    for root in roots:
        arrays = find_row_arrays(root)
        for arr in arrays:
            if not arr:
                continue
            row = arr[0]
            if isinstance(row, dict):
                sym = resolve_row_symbol(row)
                if sym:
                    return sym
    return None

def get_top_symbol() -> Optional[str]:
    if TOP_OVERRIDE:
        log(f"Using TOP_TICKER_OVERRIDE={TOP_OVERRIDE}")
        return TOP_OVERRIDE
    url = URLS.get(LEADERBOARD)
    if not url:
        log(f"Unknown LEADERBOARD='{LEADERBOARD}'. Choose one of: {list(URLS)}")
        return None
    log(f"Fetching top asset from: {url}")
    try:
        html = http_get(url)
    except Exception as e:
        log(f"HTTP fetch failed: {type(e).__name__}: {e}")
        return None
    nextdata = extract_nextdata(html)
    if not nextdata:
        log("No __NEXT_DATA__ JSON found.")
        return None
    sym = parse_nextdata_for_top_symbol(nextdata)
    if sym:
        log(f"Detected top ticker: {sym}")
    else:
        log("Could not detect top ticker in embedded JSON.")
    return sym

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
        oid = (resp.get("order_id") or resp.get("orderId")
               or (resp.get("success_response", {}) or {}).get("order_id"))
        log(f"{pid} | BUY ${usd_amount} submitted (order {oid})")
    except Exception as e:
        log(f"{pid} | BUY failed: {type(e).__name__}: {e}")
        sys.exit(1)

# ---------------- Main ----------------
def main():
    usd_amt = D(BUY_USD_STR)
    if usd_amt <= 0:
        raise SystemExit("BUY_USD must be > 0")

    log(f"Started | leaderboard={LEADERBOARD} | buy=${usd_amt} | quote={QUOTE}")
    pf = ensure_portfolio_uuid()

    sym = get_top_symbol()
    if not sym:
        log("No symbol found; aborting.")
        sys.exit(1)

    place_market_buy(sym, usd_amt, pf)
    log("Done.")

if __name__ == "__main__":
    main()
