#!/usr/bin/env python3
"""
Daily buy of the top Coinbase Leaderboard asset.

Behavior (one-shot):
- Determines the #1 asset from a leaderboard page (default: "Most buyers").
- Places a market IOC BUY for $BUY_USD of that asset via Coinbase Advanced Trade.
- Scopes the order to a portfolio (PORTFOLIO_UUID preferred; else looks up PORTFOLIO_NAME="bot").

Env vars:
  COINBASE_API_KEY=<...>
  COINBASE_API_SECRET=<...>

  # Which leaderboard:
  #   - "most-buyers"          -> https://www.coinbase.com/leaderboards/most-buyers
  #   - "highest-buy-ratio"    -> https://www.coinbase.com/leaderboards/highest-buy-ratio
  LEADERBOARD=most-buyers

  # Buy parameters:
  BUY_USD=5
  QUOTE_CURRENCY=USD

  # Portfolio scoping:
  PORTFOLIO_UUID=<uuid>          # preferred
  PORTFOLIO_NAME=bot             # used only if UUID not set

  # Scrape fallback:
  TOP_TICKER_OVERRIDE=           # e.g. BTC (bypasses scraping if set)

  # Logging:
  DEBUG=1                        # optional; more verbose logs
"""

import os
import re
import sys
from decimal import Decimal, InvalidOperation
from typing import Optional

from coinbase.rest import RESTClient  # pip install coinbase-advanced-py

TOP_URLS = {
    "most-buyers": "https://www.coinbase.com/leaderboards/most-buyers",
    "highest-buy-ratio": "https://www.coinbase.com/leaderboards/highest-buy-ratio",
}

LEADERBOARD = os.getenv("LEADERBOARD", "most-buyers").strip().lower()
BUY_USD     = os.getenv("BUY_USD", "5").strip()
QUOTE       = os.getenv("QUOTE_CURRENCY", "USD").upper().strip()

PORTFOLIO_UUID = os.getenv("PORTFOLIO_UUID", "").strip()
PORTFOLIO_NAME = os.getenv("PORTFOLIO_NAME", "bot").strip() if not PORTFOLIO_UUID else ""

DEBUG = os.getenv("DEBUG", "0") not in ("0", "false", "False", "")

client = RESTClient()  # uses COINBASE_API_KEY / COINBASE_API_SECRET


# ---------- logging ----------
def log(msg: str) -> None:
    print(f"[cb-daily-buy] {msg}", flush=True)

def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[cb-daily-buy][debug] {msg}", flush=True)


# ---------- helpers ----------
def d(val: str) -> Decimal:
    try:
        return Decimal(str(val))
    except InvalidOperation:
        raise SystemExit(f"Invalid decimal: {val}")

def ensure_portfolio_uuid() -> Optional[str]:
    """Return a portfolio UUID. Prefer env; otherwise find by name."""
    global PORTFOLIO_UUID
    if PORTFOLIO_UUID:
        return PORTFOLIO_UUID
    try:
        res = client.get("/api/v3/brokerage/portfolios")
        ports = res.get("portfolios") or res.get("data") or []
        for p in ports:
            name = str(p.get("name") or "").strip().lower()
            if name == PORTFOLIO_NAME.lower():
                PORTFOLIO_UUID = p.get("uuid") or p.get("portfolio_uuid")
                if PORTFOLIO_UUID:
                    log(f"Using portfolio '{PORTFOLIO_NAME}' ({PORTFOLIO_UUID})")
                    return PORTFOLIO_UUID
        log(f"Portfolio named '{PORTFOLIO_NAME}' not found; order will be unscoped (default portfolio).")
    except Exception as e:
        log(f"Error listing portfolios: {e}")
    return None

def extract_top_ticker_with_playwright(url: str) -> Optional[str]:
    """
    Scrape the leaderboard page and return the top ticker symbol.
    Requires playwright + chromium installed:
      pip install playwright
      python -m playwright install --with-deps chromium
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        dbg(f"Playwright not available: {e}")
        return None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        # Give the site time to hydrate client-side content
        page.wait_for_timeout(2000)

        # Heuristics:
        # - "Most buyers" page: the first row under the section title contains the leader
        # - We'll grab visible text and look for an ALLCAPS ticker (2-10 chars), not ending with USD
        text = page.inner_text("body")
        browser.close()

    tokens = re.findall(r"\b[A-Z0-9]{2,10}\b", text)
    # Filter obvious non-tickers
    blacklist = {
        "USD", "USDT", "USDC", "BUY", "RATIO", "VOLUME", "PRICE", "MARKET", "CAP",
        "NAME", "MOST", "BUYERS", "TRENDING", "LOSERS", "GAINERS",
    }
    for tok in tokens:
        if tok in blacklist or tok.endswith("USD"):
            continue
        # Small sanity: some assets are 2–6 chars typically; keep 2–10 in case (WIF, POPCAT, etc)
        return tok
    return None

def get_top_ticker() -> Optional[str]:
    # Manual override
    override = os.getenv("TOP_TICKER_OVERRIDE", "").strip().upper()
    if override:
        log(f"Using TOP_TICKER_OVERRIDE={override}")
        return override

    url = TOP_URLS.get(LEADERBOARD)
    if not url:
        log(f"Unknown LEADERBOARD='{LEADERBOARD}'. Supported: {list(TOP_URLS)}")
        return None

    log(f"Fetching top asset from: {url}")
    sym = extract_top_ticker_with_playwright(url)
    if sym:
        log(f"Detected top ticker: {sym}")
    else:
        log("Could not detect top ticker via scraping.")
    return sym

def place_market_buy(symbol: str, usd_amount: Decimal, portfolio_uuid: Optional[str]) -> None:
    pid = f"{symbol}-{QUOTE}"
    payload = {
        "product_id": pid,
        "side": "BUY",
        # Use quote_size for $-notional market buy
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

# ---------- main ----------
def main() -> None:
    # parse buy amount early to fail fast if malformed
    usd_amt = d(BUY_USD)
    if usd_amt <= 0:
        raise SystemExit("BUY_USD must be > 0")

    log(f"Started | leaderboard={LEADERBOARD} | buy=${usd_amt} | quote={QUOTE}")
    pf = ensure_portfolio_uuid()

    sym = get_top_ticker()
    if not sym:
        log("No symbol found; aborting.")
        sys.exit(1)

    place_market_buy(sym, usd_amt, pf)
    log("Done.")

if __name__ == "__main__":
    main()
