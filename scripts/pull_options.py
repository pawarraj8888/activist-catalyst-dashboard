"""
pull_options.py  -  Polygon.io options chain
Computes: implied move per ticker = ATM straddle / stock price
Cross-references with IBES realized moves for mispricing signal
"""

import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

POLYGON_KEY = os.getenv("POLYGON_API_KEY", "")
FMP_KEY     = os.getenv("FMP_API_KEY", "")
BASE        = "https://api.polygon.io"


def get_earnings_calendar(days_ahead=90):
    """Upcoming earnings via yfinance — no API key needed"""
    import yfinance as yf
    print("  Fetching earnings calendar (yfinance)...")

    UNIVERSE = [
        "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","JPM","V","UNH",
        "XOM","LLY","JNJ","WMT","MA","PG","HD","MRK","ORCL","BAC",
        "ABBV","KO","CVX","PEP","COST","TMO","MCD","CRM","NFLX","AMD",
        "ADBE","WFC","TXN","LIN","PM","DHR","NEE","INTC","QCOM","HON",
        "AMGN","IBM","GE","CAT","BA","GS","MS","BLK","SPGI","AXP",
        "SBUX","NOW","ISRG","PLD","DE","CI","SYK","MDLZ","ADI","TJX",
        "C","USB","MMC","CME","EOG","SO","DUK","ZTS","BSX","HCA",
        "REGN","VRTX","PANW","KLAC","AMAT","LRCX","MU","SNPS","CDNS","FTNT"
    ]

    today  = datetime.today()
    cutoff = today + timedelta(days=days_ahead)
    events = []

    for ticker in UNIVERSE:
        try:
            t   = yf.Ticker(ticker)
            cal = t.calendar
            if not cal or "Earnings Date" not in cal:
                continue
            earn_list = cal["Earnings Date"]
            earn_date = earn_list[0] if isinstance(earn_list, list) else earn_list
            earn_dt = pd.Timestamp(earn_date)
            if today <= earn_dt <= pd.Timestamp(cutoff):
                events.append({
                    "symbol": ticker,
                    "date":   earn_dt.strftime("%Y-%m-%d"),
                })
        except Exception:
            continue

    print(f"  Calendar: {len(events)} events")
    return events


def get_stock_price(ticker):
    """Last trade price from Polygon"""
    url = f"{BASE}/v2/last/trade/{ticker}?apiKey={POLYGON_KEY}"
    try:
        resp = requests.get(url, timeout=10).json()
        return resp.get("results", {}).get("p")
    except:
        return None


def get_implied_move(ticker, earnings_date_str):
    """
    Implied move = (ATM call + ATM put) / stock price
    Uses nearest expiry AFTER earnings date
    """
    stock_price = get_stock_price(ticker)
    if not stock_price:
        return None

    # Get options snapshot
    url = (f"{BASE}/v3/snapshot/options/{ticker}"
           f"?limit=250&apiKey={POLYGON_KEY}")

    try:
        resp = requests.get(url, timeout=15).json()
        contracts = resp.get("results", [])
    except Exception as e:
        return None

    if not contracts:
        return None

    earnings_dt = datetime.strptime(earnings_date_str, "%Y-%m-%d")

    # Filter: expiry after earnings, within 30 days of earnings
    relevant = []
    for c in contracts:
        det = c.get("details", {})
        exp_str = det.get("expiration_date", "")
        if not exp_str:
            continue
        try:
            exp_dt = datetime.strptime(exp_str, "%Y-%m-%d")
        except:
            continue
        if earnings_dt <= exp_dt <= earnings_dt + timedelta(days=30):
            relevant.append(c)

    if not relevant:
        return None

    # Find nearest expiry
    nearest_exp = min(
        set(c.get("details", {}).get("expiration_date", "") for c in relevant
            if c.get("details", {}).get("expiration_date"))
    )

    exp_contracts = [c for c in relevant
                     if c.get("details", {}).get("expiration_date") == nearest_exp]

    # Find ATM call and put
    call_candidates = [c for c in exp_contracts if c.get("details", {}).get("contract_type") == "call"]
    put_candidates  = [c for c in exp_contracts if c.get("details", {}).get("contract_type") == "put"]

    def nearest_atm(candidates):
        if not candidates:
            return None
        return min(candidates,
                   key=lambda x: abs((x.get("details", {}).get("strike_price") or 9999) - stock_price))

    atm_call = nearest_atm(call_candidates)
    atm_put  = nearest_atm(put_candidates)

    if not atm_call or not atm_put:
        return None

    call_price = atm_call.get("day", {}).get("close") or atm_call.get("last_quote", {}).get("ask", 0)
    put_price  = atm_put.get("day", {}).get("close")  or atm_put.get("last_quote", {}).get("ask", 0)

    straddle      = (call_price or 0) + (put_price or 0)
    implied_move  = straddle / stock_price if stock_price else None

    return {
        "implied_move":  round(implied_move, 4) if implied_move else None,
        "stock_price":   round(stock_price, 2),
        "straddle_cost": round(straddle, 2),
        "expiry":        nearest_exp,
        "atm_strike":    atm_call.get("details", {}).get("strike_price"),
    }


def compute_mispricing(implied_move, avg_realized):
    """
    Ratio < 1.0 = options cheap (implied < realized) -> consider long straddle
    Ratio > 1.5 = options rich (implied >> realized) -> consider short straddle
    """
    if not implied_move or not avg_realized or avg_realized == 0:
        return None
    ratio = implied_move / avg_realized
    if ratio < 0.8:
        signal = "CHEAP"
    elif ratio > 1.5:
        signal = "RICH"
    else:
        signal = "FAIR"
    return {"ratio": round(ratio, 2), "signal": signal}


def run():
    # Load WRDS realized moves
    try:
        with open("data/realized_moves.json") as f:
            realized_data = json.load(f).get("data", {})
    except:
        realized_data = {}

    # Load WRDS IBES enrichment
    try:
        with open("data/ibes_enrichment.json") as f:
            ibes_data = json.load(f).get("data", {})
    except:
        ibes_data = {}

    # Get earnings calendar
    calendar = get_earnings_calendar(days_ahead=90)

    results = []
    for event in calendar[:80]:  # limit API calls on free tier
        ticker = event.get("symbol") or event.get("ticker")
        date   = event.get("date") or event.get("reportDate")

        if not ticker or not date:
            continue

        print(f"  Processing {ticker} earnings {date}...")
        opts = get_implied_move(ticker, date)

        realized = realized_data.get(ticker, {})
        ibes     = ibes_data.get(ticker, {})
        avg_real = realized.get("avg_realized_move")

        mispricing = None
        if opts and avg_real:
            mispricing = compute_mispricing(opts["implied_move"], avg_real)

        results.append({
            "ticker":              ticker,
            "earnings_date":       date,
            "eps_estimate":        event.get("epsEstimated"),
            "revenue_estimate":    event.get("revenueEstimated"),
            "implied_move":        opts["implied_move"] if opts else None,
            "implied_move_pct":    round(opts["implied_move"] * 100, 2) if opts and opts["implied_move"] else None,
            "stock_price":         opts["stock_price"] if opts else None,
            "straddle_cost":       opts["straddle_cost"] if opts else None,
            "options_expiry":      opts["expiry"] if opts else None,
            "avg_realized_move":   avg_real,
            "avg_realized_pct":    round(avg_real * 100, 2) if avg_real else None,
            "mispricing":          mispricing,
            "beat_rate":           ibes.get("beat_rate"),
            "avg_surprise":        ibes.get("avg_surprise"),
            "last_surprise":       ibes.get("last_surprise"),
            "avg_analysts":        ibes.get("avg_analysts"),
        })

    # Sort by mispricing signal (CHEAP first, then FAIR, then RICH)
    signal_order = {"CHEAP": 0, "FAIR": 1, "RICH": 2, None: 3}
    results.sort(key=lambda x: signal_order.get(
        x.get("mispricing", {}).get("signal") if x.get("mispricing") else None, 3
    ))

    with open("data/earnings_catalyst.json", "w") as f:
        json.dump({
            "last_updated": datetime.now().isoformat(),
            "events":       results,
        }, f, indent=2, default=str)

    print(f"Options pull complete. {len(results)} earnings events processed.")


if __name__ == "__main__":
    run()
