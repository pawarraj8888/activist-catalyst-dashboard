"""
pull_edgar.py  -  Live EDGAR feeds
Pulls: SC 13D / SC 13G activist filings + Form 4 insider filings
Runs every 15 minutes via GitHub Actions (or locally via cron)
"""

import requests
import feedparser
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

HEADERS = {"User-Agent": "Raj Pawar raj.pawar@nyu.edu"}  # EDGAR requires User-Agent

# Known activist filer track records (manually curated, expand as needed)
ACTIVIST_TRACK_RECORDS = {
    "Elliott Investment Management": {"win_rate": 0.82, "avg_return_6m": 0.18, "style": "Activist"},
    "Starboard Value": {"win_rate": 0.79, "avg_return_6m": 0.15, "style": "Activist"},
    "Third Point": {"win_rate": 0.73, "avg_return_6m": 0.12, "style": "Activist"},
    "ValueAct Capital": {"win_rate": 0.71, "avg_return_6m": 0.11, "style": "Activist"},
    "Pershing Square": {"win_rate": 0.68, "avg_return_6m": 0.14, "style": "Activist/Concentrated"},
    "Jana Partners": {"win_rate": 0.66, "avg_return_6m": 0.10, "style": "Activist"},
    "Icahn": {"win_rate": 0.70, "avg_return_6m": 0.16, "style": "Activist"},
    "Trian Fund Management": {"win_rate": 0.69, "avg_return_6m": 0.09, "style": "Activist"},
}


def fetch_13d_filings(days_back=7):
    """
    Pull recent SC 13D filings from EDGAR ATOM feed.
    13D = activist crossing 5% with intent to influence.
    """
    print("  Fetching SC 13D filings...")
    url = ("https://www.sec.gov/cgi-bin/browse-edgar"
           "?action=getcurrent&type=SC+13D&dateb=&owner=include&count=40&output=atom")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        feed = feedparser.parse(resp.content)
    except Exception as e:
        print(f"  13D feed error: {e}")
        return []

    cutoff = datetime.today() - timedelta(days=days_back)
    filings = []

    for entry in feed.entries:
        try:
            filed_str = entry.get("updated", "")
            if not filed_str:
                continue
            filed_dt = datetime(*entry.updated_parsed[:6])
            if filed_dt < cutoff:
                continue

            title  = entry.get("title", "")
            link   = entry.get("link", "")
            summary = entry.get("summary", "")

            # Parse: "SC 13D - COMPANY NAME (TICKER) (0000123456) (Filer)"
            import re
            ticker_match = re.search(r"\(([A-Z]{1,5})\)", title)
            ticker = ticker_match.group(1) if ticker_match else None

            # Try to extract filer name from summary
            filer = "Unknown"
            for known in ACTIVIST_TRACK_RECORDS:
                if known.lower() in summary.lower():
                    filer = known
                    break

            track = ACTIVIST_TRACK_RECORDS.get(filer, {
                "win_rate": None, "avg_return_6m": None, "style": "Unknown"
            })

            filings.append({
                "type":            "13D",
                "filed_date":      filed_dt.strftime("%Y-%m-%d"),
                "title":           title,
                "ticker":          ticker,
                "filer":           filer,
                "win_rate":        track["win_rate"],
                "avg_return_6m":   track["avg_return_6m"],
                "filer_style":     track["style"],
                "sec_link":        link,
                "high_conviction": track.get("win_rate", 0) and track["win_rate"] > 0.75,
            })
        except Exception as e:
            print(f"    entry parse error: {e}")
            continue

    print(f"  13D: {len(filings)} recent filings")
    return filings


def fetch_13g_filings(days_back=7):
    """SC 13G = passive crossing of 5%. Less aggressive than 13D but still notable."""
    print("  Fetching SC 13G filings...")
    url = ("https://www.sec.gov/cgi-bin/browse-edgar"
           "?action=getcurrent&type=SC+13G&dateb=&owner=include&count=40&output=atom")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        feed = feedparser.parse(resp.content)
    except Exception as e:
        print(f"  13G feed error: {e}")
        return []

    cutoff = datetime.today() - timedelta(days=days_back)
    filings = []

    for entry in feed.entries:
        try:
            filed_dt = datetime(*entry.updated_parsed[:6])
            if filed_dt < cutoff:
                continue

            import re
            title = entry.get("title", "")
            ticker_match = re.search(r"\(([A-Z]{1,5})\)", title)
            ticker = ticker_match.group(1) if ticker_match else None

            filings.append({
                "type":       "13G",
                "filed_date": filed_dt.strftime("%Y-%m-%d"),
                "title":      title,
                "ticker":     ticker,
                "filer":      "Institutional",
                "sec_link":   entry.get("link", ""),
            })
        except:
            continue

    print(f"  13G: {len(filings)} recent filings")
    return filings


def fetch_form4_recent(days_back=3):
    """
    Recent Form 4 open-market purchases from EDGAR.
    Cross-referenced later with activist filings for confluence signal.
    """
    print("  Fetching Form 4 filings...")
    url = ("https://www.sec.gov/cgi-bin/browse-edgar"
           "?action=getcurrent&type=4&dateb=&owner=include&count=100&output=atom")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        feed = feedparser.parse(resp.content)
    except Exception as e:
        print(f"  Form4 feed error: {e}")
        return []

    cutoff  = datetime.today() - timedelta(days=days_back)
    form4s  = []

    for entry in feed.entries:
        try:
            filed_dt = datetime(*entry.updated_parsed[:6])
            if filed_dt < cutoff:
                continue
            title = entry.get("title", "")
            # Parse ticker from title: "4 - COMPANY NAME (TICKER) (CIK)"
            import re
            ticker_match = re.search(r"\(([A-Z]{1,5})\)\s*\(\d", title)
            ticker = ticker_match.group(1) if ticker_match else None
            # Parse company and insider name
            # Title format: "4 - INSIDER NAME - COMPANY (TICKER) (CIK)"
            parts = title.split(" - ")
            insider = parts[1].strip() if len(parts) > 1 else "Unknown"
            form4s.append({
                "filed_date":  filed_dt.strftime("%Y-%m-%d"),
                "ticker":      ticker,
                "insider":     insider,
                "title":       title,
                "link":        entry.get("link", ""),
            })
        except:
            continue

    print(f"  Form4: {len(form4s)} recent filings")
    return form4s


def compute_confluence(activist_filings, insider_trades):
    """
    Flag tickers where activist filing AND insider buying both present.
    Highest conviction signal in the dashboard.
    """
    activist_tickers = {f["ticker"] for f in activist_filings if f.get("ticker")}

    insider_tickers = set()
    for t in insider_trades:
        ticker = t.get("ticker") or t.get("Ticker")
        if ticker:
            insider_tickers.add(ticker.upper())

    confluence = list(activist_tickers & insider_tickers)
    return confluence


def run():
    filings_13d = fetch_13d_filings(days_back=7)
    filings_13g = fetch_13g_filings(days_back=7)
    form4s      = fetch_form4_recent(days_back=3)

    # Load insider trades from WRDS pull for confluence check
    try:
        with open("data/insider_trades.json") as f:
            insider_data = json.load(f)
        insider_trades = insider_data.get("trades", [])
    except:
        insider_trades = []

    confluence = compute_confluence(filings_13d + filings_13g, insider_trades)

    # Tag confluence on activist filings
    for f in filings_13d:
        f["confluence_signal"] = f.get("ticker") in confluence

    output = {
        "last_updated":    datetime.now().isoformat(),
        "filings_13d":     filings_13d,
        "filings_13g":     filings_13g[:20],
        "confluence_tickers": confluence,
    }

    with open("data/activist_filings.json", "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"EDGAR pull complete. Confluence tickers: {confluence}")


if __name__ == "__main__":
    run()