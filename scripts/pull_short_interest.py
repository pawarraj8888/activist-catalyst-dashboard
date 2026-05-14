"""
pull_short_interest.py
Pulls short interest % of float via yfinance for earnings universe + recent insider tickers.
Saves data/short_interest.json keyed by ticker.
"""
import yfinance as yf
import json
import os
from datetime import datetime

UNIVERSE = [
    "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","JPM","V","UNH",
    "XOM","LLY","JNJ","WMT","MA","PG","HD","MRK","ORCL","BAC",
    "ABBV","KO","CVX","PEP","COST","TMO","MCD","CRM","NFLX","AMD",
    "ADBE","WFC","TXN","LIN","PM","DHR","NEE","INTC","QCOM","HON",
    "AMGN","IBM","GE","CAT","BA","GS","MS","BLK","SPGI","AXP",
    "SBUX","NOW","ISRG","PLD","DE","CI","SYK","MDLZ","ADI","TJX",
    "C","USB","CME","EOG","SO","DUK","ZTS","BSX","HCA",
    "REGN","VRTX","PANW","KLAC","AMAT","LRCX","MU","SNPS","CDNS","FTNT"
]

def pull():
    # Add tickers from recent insider trades
    try:
        with open("data/insider_history.json") as f:
            hist = json.load(f)
        insider_tickers = list(set(t.get("ticker","") for t in hist.get("trades",[]) if t.get("ticker")))
    except:
        insider_tickers = []

    all_tickers = list(set(UNIVERSE + insider_tickers))
    print(f"  Pulling short interest for {len(all_tickers)} tickers...")

    result = {}
    for ticker in all_tickers:
        try:
            info = yf.Ticker(ticker).info
            si_pct   = info.get("shortPercentOfFloat")
            si_ratio = info.get("shortRatio")
            float_sh = info.get("floatShares")
            si_shares= info.get("sharesShort")
            if si_pct is not None:
                result[ticker] = {
                    "short_pct_float": round(float(si_pct) * 100, 2),
                    "days_to_cover":   round(float(si_ratio), 1) if si_ratio else None,
                    "shares_short":    int(si_shares) if si_shares else None,
                    "float_shares":    int(float_sh) if float_sh else None,
                    "squeeze_risk":    "HIGH" if si_pct and si_pct > 0.20 else
                                       "MEDIUM" if si_pct and si_pct > 0.10 else "LOW",
                }
        except:
            continue

    with open("data/short_interest.json", "w") as f:
        json.dump({"last_updated": datetime.now().isoformat(), "data": result}, f, indent=2, default=str)

    print(f"  Short interest: {len(result)} tickers saved")

if __name__ == "__main__":
    pull()
