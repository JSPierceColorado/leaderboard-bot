#!/usr/bin/env python3
"""
Daily buy of the top Coinbase Leaderboard asset.

This version parses the Next.js bootstrapped JSON (__NEXT_DATA__ / similar)
instead of scraping visual elements. If it still can't find data, set
TOP_TICKER_OVERRIDE=BTC for that run.

Env:
  COINBASE_API_KEY, COINBASE_API_SECRET
  LEADERBOARD=most-buyers | highest-buy-ratio
  BUY_USD=5
  QUOTE_CURRENCY=USD
  PORTFOLIO_UUID=<uuid>   # preferred
  PORTFOLIO_NAME=bot      # used only if UUID not set
  TOP_TICKER_OVERRIDE=    # e.g., BTC
  DEBUG=1

Requires:
  pip install coinbase-advanced-py playwright
  python -m playwright install --with-deps chromium
"""

import json
import os
import re
import sys
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

from coinbase.rest import RESTClient

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

client = RESTClient()  # needs COINBASE_API_KEY/SECRET

# -------------- logging --------------
def log(msg: str) -> None:
    print(f"[cb-daily-buy] {msg}", flush=True)

def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[cb-daily-buy][debug] {msg}", flush=True)

# -------------- helpers --------------
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

def save_debug(page_html: str, next_json: Optional[dict]) -> None:
    if not DEBUG:
        return
    try:
        with open("/tmp/leaderboard.html", "w", encoding="utf-8") as f:
            f.write(page_html)
        dbg("Saved /tmp/leaderboard.html")
    except Exception as e:
        dbg(f"Saving HTML failed: {e}")
    if next_json is not None:
        try:
            with open("/tmp/nextdata.json", "w", encoding="utf-8") as f:
                json.dump(next_json, f, indent=2)
            dbg("Saved /tmp/nextdata.json")
        except Exception as e:
            dbg(f"Saving JSON failed: {e}")

def find_in_obj(obj: Any, predicate) -> List[Any]:
    """DFS search returning all values for which predicate(value) is True."""
    found = []
    stack = [obj]
    while stack:
        cur = stack.pop()
        try:
            if predicate(cur):
                found.append(cur)
        except Exception:
            pass
        if isinstance(cur, dict):
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return found

def map_slug_to_ticker(slug: str) -> Optional[str]:
    """Map a /price/<slug> to ticker via products list."""
    try:
        prods = client.get_products()
        items = getattr(prods, "products", None) or getattr(prods, "data", None) or prods
        s = slug.lower()
        best = None
        for p in items:
            base = getattr(p, "base_currency_id", None) or p.get("base_currency_id")
            disp = getattr(p, "display_name", None) or p.get("display_name")  # e.g., BTC-USD
            base_name = getattr(p, "base_display_name", None) or p.get("base_display_name") or ""
            pid = getattr(p, "product_id", None) or p.get("product_id")
            if not base or not pid:
                continue
            tkr = str(base).upper()
            dn  = (disp or "")
            if s in (tkr.lower(), pid.lower(), pid.split("-")[0].lower(), base_name.lower(), dn.lower()):
                return tkr
            if base_name and s in base_name.lower():
                best = tkr
        return best
    except Exception as e:
        dbg(f"map_slug_to_ticker error: {e}")
        return None

def extract_top_from_nextdata(nextdata: dict) -> Optional[str]:
    """
    Try common Next.js shapes. We scan for arrays that look like leaderboard rows,
    then pick the first row and extract symbol or /price/<slug>.
    """
    # Heuristic: find any list of rows where each row has a 'href' to /price/ or has a 'symbol'/'ticker' field.
    def looks_like_row(x):
        if not isinstance(x, dict):
            return False
        href = str(x.get("href") or x.get("link") or "")
        sym  = x.get("symbol") or x.get("ticker") or x.get("base") or x.get("assetSymbol")
        return (href.startswith("/price/") or sym)

    candidates = find_in_obj(nextdata, lambda v: isinstance(v, list) and any(looks_like_row(el) for el in v))
    for arr in candidates:
        # pick the first row in each candidate array
        row = arr[0] if arr else None
        if not isinstance(row, dict):
            continue
        # 1) ticker present directly
        sym = row.get("symbol") or row.get("ticker") or row.get("base") or row.get("assetSymbol")
        if sym and isinstance(sym, str) and 2 <= len(sym) <= 10 and sym.isupper():
            return sym
        # 2) derive from /price/<slug>
        href = str(row.get("href") or row.get("link") or "")
        m = re.search(r"/price/([a-z0-9-]+)", href)
        if m:
            t = map_slug_to_ticker(m.group(1))
            if t:
                return t
    return None

def get_top_symbol_via_nextdata(url: str) -> Optional[str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        dbg(f"Playwright unavailable: {e}")
        return None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
        )
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=90000)

        # Try to read Next data via DOM
        next_json = None
        selectors = [
            "script#__NEXT_DATA__",
            "script[id='__NEXT_DATA__']",
            "script[type='application/json']",
        ]
        for sel in selectors:
            try:
                el = page.query_selector(sel)
                if el:
                    raw = el.inner_text()
                    if raw and len(raw) > 20:
                        next_json = json.loads(raw)
                        break
            except Exception:
                continue

        # Fallback: evaluate window.__NEXT_DATA__ if present
        if next_json is None:
            try:
                next_json = page.evaluate("() => window.__NEXT_DATA__ || null")
            except Exception:
                pass

        html = page.content()
        browser.close()

    save_debug(html, next_json)

    if not next_json:
        return None

    # Try a few common nesting roots
    roots = [
        next_json,
        next_json.get("props", {}),
        next_json.get("pageProps", {}),
        (next_json.get("props", {}) or {}).get("pageProps", {}),
    ]
    for root in roots:
        sym = extract_top_from_nextdata(root)
        if sym:
            return sym
    return None

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

def main():
    usd_amt = D(BUY_USD_STR)
    if usd_amt <= 0:
        raise SystemExit("BUY_USD must be > 0")

    log(f"Started | leaderboard={LEADERBOARD} | buy=${usd_amt} | quote={QUOTE}")
    pf = ensure_portfolio_uuid()

    if TOP_OVERRIDE:
        log(f"Using TOP_TICKER_OVERRIDE={TOP_OVERRIDE}")
        sym = TOP_OVERRIDE
    else:
        url = URLS.get(LEADERBOARD)
        if not url:
            log(f"Unknown LEADERBOARD='{LEADERBOARD}'. Choose one of: {list(URLS)}")
            sys.exit(1)
        log(f"Fetching top asset from: {url}")
        sym = get_top_symbol_via_nextdata(url)

    if not sym:
        log("No symbol found; aborting.")
        sys.exit(1)

    place_market_buy(sym, usd_amt, pf)
    log("Done.")

if __name__ == "__main__":
    main()
