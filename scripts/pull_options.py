"""
pull_options.py  -  Earnings catalyst screen
Uses yfinance for both earnings calendar AND implied move (free, no Polygon needed)
"""

import json
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
import yfinance as yf

load_dotenv()

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


def get_earnings_calendar(days_ahead=90):
    print("  Fetching earnings calendar (yfinance)...")
    today  = datetime.today()
    cutoff = today + timedelta(days=days_ahead)
    events = []
    for ticker in UNIVERSE:
        try:
            cal = yf.Ticker(ticker).calendar
            if not cal or "Earnings Date" not in cal:
                continue
            earn_list = cal["Earnings Date"]
            earn_date = earn_list[0] if isinstance(earn_list, list) else earn_list
            earn_dt   = pd.Timestamp(earn_date)
            if today <= earn_dt <= pd.Timestamp(cutoff):
                events.append({"symbol": ticker, "date": earn_dt.strftime("%Y-%m-%d")})
        except:
            continue
    print(f"  Calendar: {len(events)} events")
    return events


def get_implied_move_yfinance(ticker, earnings_date_str):
    try:
        t = yf.Ticker(ticker)
        info = t.info
        stock_price = (info.get("regularMarketPrice") or
                       info.get("currentPrice") or
                       info.get("previousClose"))
        if not stock_price or stock_price <= 0:
            return None

        expirations = t.options
        if not expirations:
            return None

        earnings_dt = datetime.strptime(earnings_date_str, "%Y-%m-%d")
        valid = [e for e in expirations
                 if datetime.strptime(e, "%Y-%m-%d") >= earnings_dt]
        if not valid:
            return None

        nearest_exp = valid[0]
        chain = t.option_chain(nearest_exp)
        calls = chain.calls
        puts  = chain.puts
        if calls.empty or puts.empty:
            return None

        strikes   = calls["strike"].tolist()
        atm_strike = min(strikes, key=lambda x: abs(x - stock_price))

        atm_call_rows = calls[calls["strike"] == atm_strike]
        atm_put_rows  = puts[puts["strike"]   == atm_strike]
        if atm_call_rows.empty or atm_put_rows.empty:
            return None

        def mid(row):
            bid, ask = row.get("bid", 0), row.get("ask", 0)
            if bid > 0 and ask > 0:
                return (bid + ask) / 2
            return row.get("lastPrice", 0)

        straddle = mid(atm_call_rows.iloc[0]) + mid(atm_put_rows.iloc[0])
        if straddle <= 0:
            return None

        implied_move = straddle / stock_price
        return {
            "implied_move":     round(implied_move, 4),
            "implied_move_pct": round(implied_move * 100, 2),
            "stock_price":      round(stock_price, 2),
            "straddle_cost":    round(straddle, 2),
            "expiry":           nearest_exp,
            "atm_strike":       atm_strike,
        }
    except:
        return None


def compute_mispricing(implied_move, avg_realized):
    if not implied_move or not avg_realized or avg_realized == 0:
        return None
    ratio  = implied_move / avg_realized
    signal = "CHEAP" if ratio < 0.8 else "RICH" if ratio > 1.5 else "FAIR"
    return {"ratio": round(ratio, 2), "signal": signal}


def run():
    try:
        realized_data = json.load(open("data/realized_moves.json")).get("data", {})
    except:
        realized_data = {}
    try:
        ibes_data = json.load(open("data/ibes_enrichment.json")).get("data", {})
    except:
        ibes_data = {}

    calendar = get_earnings_calendar(days_ahead=90)
    results  = []

    for event in calendar:
        ticker = event.get("symbol")
        date   = event.get("date")
        if not ticker or not date:
            continue
        print(f"  {ticker} {date}...")
        opts       = get_implied_move_yfinance(ticker, date)
        realized   = realized_data.get(ticker, {})
        ibes       = ibes_data.get(ticker, {})
        avg_real   = realized.get("avg_realized_move")
        mispricing = compute_mispricing(opts["implied_move"], avg_real) if opts and avg_real else None

        results.append({
            "ticker":            ticker,
            "earnings_date":     date,
            "implied_move":      opts["implied_move"]     if opts else None,
            "implied_move_pct":  opts["implied_move_pct"] if opts else None,
            "stock_price":       opts["stock_price"]      if opts else None,
            "straddle_cost":     opts["straddle_cost"]    if opts else None,
            "options_expiry":    opts["expiry"]           if opts else None,
            "avg_realized_move": avg_real,
            "avg_realized_pct":  round(avg_real * 100, 2) if avg_real else None,
            "mispricing":        mispricing,
            "beat_rate":         ibes.get("beat_rate"),
            "last_surprise":     ibes.get("last_surprise"),
        })

    order = {"CHEAP": 0, "FAIR": 1, "RICH": 2, None: 3}
    results.sort(key=lambda x: order.get(
        x.get("mispricing", {}).get("signal") if x.get("mispricing") else None, 3))

    with open("data/earnings_catalyst.json", "w") as f:
        json.dump({"last_updated": datetime.now().isoformat(),
                   "events": results}, f, indent=2, default=str)

    cheap = sum(1 for r in results if r.get("mispricing") and r["mispricing"].get("signal") == "CHEAP")
    rich  = sum(1 for r in results if r.get("mispricing") and r["mispricing"].get("signal") == "RICH")
    print(f"Done. {len(results)} events. CHEAP: {cheap}, RICH: {rich}")


if __name__ == "__main__":
    run()