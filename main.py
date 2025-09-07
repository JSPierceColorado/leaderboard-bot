#!/usr/bin/env python3
"""
Daily buy of the top Coinbase Leaderboard asset (Most buyers or Highest buy ratio).

- Scrapes the leaderboard with Playwright (headless Chromium + UA spoof, anti-automation tweaks).
- Heuristics to grab the 1st row, extract ticker from row text or /price/<slug> link.
- If scraping fails, supports TOP_TICKER_OVERRIDE.
- Places a market IOC BUY for $BUY_USD into the chosen portfolio.

Env:
  COINBASE_API_KEY, COINBASE_API_SECRET
  LEADERBOARD=most-buyers | highest-buy-ratio
  BUY_USD=5
  QUOTE_CURRENCY=USD
  PORTFOLIO_UUID=<uuid>   # preferred
  PORTFOLIO_NAME=bot      # used only if UUID not set
  TOP_TICKER_OVERRIDE=    # e.g., BTC
  DEBUG=1                 # verbose + write /tmp/leaderboard.html if scrape fails

Build-time (for Playwright):
  pip install playwright coinbase-advanced-py
  python -m playwright install --with-deps chromium
"""
import os
import re
import sys
from decimal import Decimal, InvalidOperation
from typing import Optional, List, Dict

from coinbase.rest import RESTClient

# ---------- Config ----------
TOP_URLS = {
    "most-buyers": "https://www.coinbase.com/leaderboards/most-buyers",
    "highest-buy-ratio": "https://www.coinbase.com/leaderboards/highest-buy-ratio",
}
LEADERBOARD = os.getenv("LEADERBOARD", "most-buyers").strip().lower()
BUY_USD_STR = os.getenv("BUY_USD", "5").strip()
QUOTE       = os.getenv("QUOTE_CURRENCY", "USD").upper().strip()
PORTFOLIO_UUID = os.getenv("PORTFOLIO_UUID", "").strip()
PORTFOLIO_NAME = os.getenv("PORTFOLIO_NAME", "bot").strip() if not PORTFOLIO_UUID else ""
TOP_OVERRIDE   = os.getenv("TOP_TICKER_OVERRIDE", "").strip().upper()
DEBUG          = os.getenv("DEBUG", "0") not in ("0", "false", "False", "")

client = RESTClient()

# ---------- Logging ----------
def log(msg: str) -> None:
    print(f"[cb-daily-buy] {msg}", flush=True)
def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[cb-daily-buy][debug] {msg}", flush=True)

# ---------- Helpers ----------
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
        log(f"Portfolio named '{PORTFOLIO_NAME}' not found; will place order unscoped (default portfolio).")
    except Exception as e:
        log(f"Error listing portfolios: {e}")
    return None

def map_slug_to_ticker(slug: str) -> Optional[str]:
    """
    Map a /price/<slug> to a tradable ticker (e.g., 'bitcoin' -> BTC) by scanning products.
    Cached per run.
    """
    try:
        prods = client.get_products()
        # Many SDKs return .products; others return list directly
        items = getattr(prods, "products", None) or getattr(prods, "data", None) or prods
        # Slug may correspond to base_name/product_display_name
        slug_l = slug.lower()
        best = None
        for p in items:
            base = getattr(p, "base_currency_id", None) or p.get("base_currency_id")
            disp = getattr(p, "display_name", None) or p.get("display_name")  # e.g., 'BTC-USD'
            base_name = getattr(p, "base_display_name", None) or p.get("base_display_name") or ""
            product_id = getattr(p, "product_id", None) or p.get("product_id")
            if not base or not product_id:
                continue
            # Normalize
            tkr = str(base).upper()
            dn  = str(disp or "")
            bn  = str(base_name or "")
            if slug_l in (dn.lower(), bn.lower(), tkr.lower(), product_id.lower(), product_id.split("-")[0].lower()):
                return tkr
            # Loose check: slug substring matches base name
            if bn and slug_l in bn.lower():
                best = tkr
        return best
    except Exception as e:
        dbg(f"map_slug_to_ticker error: {e}")
        return None

def extract_top_with_playwright(url: str) -> Optional[str]:
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except Exception as e:
        dbg(f"Playwright not available: {e}")
        return None

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            locale="en-US",
            timezone_id="UTC",
        )
        # Hide webdriver flag
        ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = ctx.new_page()
        page.goto(url, wait_until="networkidle", timeout=90000)

        # Try several selectors for the first row entry
        selectors = [
            # new leaderboard often uses role=list + role=listitem
            "[role=list] [role=listitem] >> nth=0",
            # generic table row
            "table tr >> nth=1",  # skip header
            # asset cards grid
            "[data-testid*='Leaderboards'] [data-testid*='row'] >> nth=0",
            # any clickable row with a link to /price/
            "a[href^='/price/'] >> nth=0",
        ]

        row_handle = None
        for sel in selectors:
            try:
                row_handle = page.wait_for_selector(sel, timeout=5000)
                if row_handle:
                    dbg(f"Found selector: {sel}")
                    break
            except PWTimeout:
                continue

        # Save snapshot for debugging
        if DEBUG:
            try:
                html = page.content()
                with open("/tmp/leaderboard.html", "w", encoding="utf-8") as f:
                    f.write(html)
                dbg("Wrote /tmp/leaderboard.html")
            except Exception as e:
                dbg(f"snapshot write error: {e}")

        if not row_handle:
            browser.close()
            return None

        # 1) Prefer link slug → ticker
        try:
            link = row_handle.query_selector("a[href^='/price/']")
            if link:
                href = link.get_attribute("href") or ""
                m = re.search(r"/price/([a-z0-9-]+)", href)
                if m:
                    slug = m.group(1)
                    tkr = map_slug_to_ticker(slug)
                    if tkr:
                        browser.close()
                        return tkr
        except Exception:
            pass

        # 2) Fallback: parse ticker from visible text
        try:
            text = row_handle.inner_text()
        except Exception:
            text = page.inner_text("body")
        # find uppercase-ish token that looks like a ticker
        tokens = re.findall(r"\b[A-Z0-9]{2,10}\b", text or "")
        block = {"USD","USDT","USDC","BUY","RATIO","VOLUME","PRICE","MARKET","CAP","MOST","BUYERS"}
        for tok in tokens:
            if tok not in block and not tok.endswith("USD"):
                browser.close()
                return tok

        browser.close()
        return None

def get_top_symbol() -> Optional[str]:
    if TOP_OVERRIDE:
        log(f"Using TOP_TICKER_OVERRIDE={TOP_OVERRIDE}")
        return TOP_OVERRIDE
    url = TOP_URLS.get(LEADERBOARD)
    if not url:
        log(f"Unknown LEADERBOARD='{LEADERBOARD}'. Choose one of: {list(TOP_URLS)}")
        return None
    log(f"Fetching top asset from: {url}")
    sym = extract_top_with_playwright(url)
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

    sym = get_top_symbol()
    if not sym:
        log("No symbol found; aborting.")
        sys.exit(1)

    place_market_buy(sym, usd_amt, pf)
    log("Done.")

if __name__ == "__main__":
    main()
