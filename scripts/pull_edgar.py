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


def parse_form4_xml(filing_index_url):
    """
    Fetch and parse Form 4 XML to extract open-market purchases only.
    Returns dict with transaction details or None if not a purchase.
    """
    import re
    try:
        # Get filing index page to find XML document
        idx = requests.get(filing_index_url, headers=HEADERS, timeout=10).text
        # Find the actual form4 XML link
        xml_match = re.search(r'href="(/Archives/edgar/data/[^"]+\.xml)"', idx)
        if not xml_match:
            return None
        xml_url = "https://www.sec.gov" + xml_match.group(1)
        xml = requests.get(xml_url, headers=HEADERS, timeout=10).text

        # Transaction code P = open-market purchase
        trans_code = re.search(r"<transactionCode>([^<]+)</transactionCode>", xml)
        if not trans_code or trans_code.group(1).strip() != "P":
            return None

        # Extract fields
        def extract(tag):
            m = re.search(rf"<{tag}>([^<]+)</{tag}>", xml)
            return m.group(1).strip() if m else None

        shares = extract("transactionShares")
        price  = extract("transactionPricePerShare")
        ticker = extract("issuerTradingSymbol")
        company= extract("issuerName")
        name   = extract("rptOwnerName")
        role_tags = re.findall(r"<officerTitle>([^<]+)</officerTitle>", xml)
        is_director = "<isDirector>1</isDirector>" in xml
        is_officer  = "<isOfficer>1</isOfficer>" in xml
        is_ten_pct  = "<isTenPercentOwner>1</isTenPercentOwner>" in xml

        role = role_tags[0] if role_tags else ("Director" if is_director else ("10%+ Owner" if is_ten_pct else "Insider"))

        if not shares or not price:
            return None

        shares_n = float(shares)
        price_n  = float(price)
        value    = shares_n * price_n

        # Filter: only surface meaningful purchases ($50K+)
        if value < 50000:
            return None

        return {
            "ticker":    ticker,
            "company":   company,
            "insider":   name,
            "role":      role,
            "shares":    int(shares_n),
            "price":     round(price_n, 2),
            "value_usd": round(value, 0),
            "value_m":   round(value / 1e6, 3),
            "is_ceo":    any(t.lower() in ["chief executive officer","ceo","president & ceo"] for t in role_tags),
            "is_director": is_director,
            "is_ten_pct":  is_ten_pct,
        }
    except Exception as e:
        return None


def fetch_form4_recent(days_back=3):
    """
    Fetch Form 4 filings, parse XML for each, return only open-market purchases $50K+.
    """
    print("  Fetching & parsing Form 4 filings (open-market purchases only)...")
    url = ("https://www.sec.gov/cgi-bin/browse-edgar"
           "?action=getcurrent&type=4&dateb=&owner=include&count=100&output=atom")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        feed = feedparser.parse(resp.content)
    except Exception as e:
        print(f"  Form4 feed error: {e}")
        return []

    cutoff = datetime.today() - timedelta(days=days_back)
    entries = []
    for entry in feed.entries:
        try:
            filed_dt = datetime(*entry.updated_parsed[:6])
            if filed_dt < cutoff:
                continue
            entries.append((filed_dt, entry.get("link", "")))
        except:
            continue

    print(f"  Parsing {len(entries)} Form 4 filings for open-market purchases...")
    purchases = []
    for filed_dt, link in entries:
        parsed = parse_form4_xml(link)
        if parsed:
            parsed["filed_date"] = filed_dt.strftime("%Y-%m-%d")
            parsed["sec_link"]   = link
            purchases.append(parsed)

    purchases.sort(key=lambda x: x["value_usd"], reverse=True)
    print(f"  Form4: {len(purchases)} open-market purchases $50K+ found")
    return purchases


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


def fetch_13d_filer_names(days_back=7):
    """
    Use EDGAR full-text search JSON API for richer 13D data including filer names.
    """
    from datetime import date, timedelta
    start = (date.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    url = (f"https://efts.sec.gov/LATEST/search-index?q=%22beneficial+owner%22"
           f"&forms=SC+13D&dateRange=custom&startdt={start}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15).json()
        hits = resp.get("hits", {}).get("hits", [])
        results = []
        for h in hits:
            src = h.get("_source", {})
            import re
            # Extract ticker from entity name or file_date
            entity = src.get("entity_name", "") or src.get("display_names", [""])[0]
            ticker_m = re.search(r"\(([A-Z]{1,5})\)", entity)
            ticker = ticker_m.group(1) if ticker_m else None
            filer_list = src.get("display_names", [])
            filer = filer_list[0] if filer_list else "Unknown"
            track = ACTIVIST_TRACK_RECORDS.get(filer, {
                "win_rate": None, "avg_return_6m": None, "style": "Unknown"
            })
            results.append({
                "type": "13D",
                "filed_date": src.get("file_date", ""),
                "ticker": ticker,
                "filer": filer,
                "win_rate": track.get("win_rate"),
                "avg_return_6m": track.get("avg_return_6m"),
                "filer_style": track.get("style", "Unknown"),
                "sec_link": f"https://www.sec.gov/Archives/edgar/data/{src.get('entity_id','')}/",
                "high_conviction": bool(track.get("win_rate") and track["win_rate"] > 0.75),
                "confluence_signal": False,
            })
        return results
    except Exception as e:
        print(f"  13D search error: {e}")
        return []


def run():
    # Try richer API first, fall back to ATOM feed
    filings_13d = fetch_13d_filer_names(days_back=7)
    if not filings_13d:
        filings_13d = fetch_13d_filings(days_back=7)

    filings_13g = fetch_13g_filings(days_back=7)
    form4s      = fetch_form4_recent(days_back=3)

    # Save Form 4 data to insider_trades.json
    # Save today's trades
    with open("data/insider_trades.json", "w") as f:
        json.dump({
            "last_updated": datetime.now().isoformat(),
            "trades": form4s
        }, f, indent=2, default=str)
    print(f"  Saved {len(form4s)} Form 4 filings to insider_trades.json")

    # Append to rolling history (deduplicated)
    try:
        with open("data/insider_history.json") as f:
            history = json.load(f)
        hist_trades = history.get("trades", [])
    except:
        hist_trades = []

    existing_keys = set(
        (t.get("ticker",""), t.get("insider",""), t.get("filed_date",""), str(t.get("value_usd",0)))
        for t in hist_trades
    )
    new_trades = []
    for t in form4s:
        key = (t.get("ticker",""), t.get("insider",""), t.get("filed_date",""), str(t.get("value_usd",0)))
        if key not in existing_keys:
            new_trades.append(t)
            existing_keys.add(key)

    hist_trades = (new_trades + hist_trades)[:2000]  # keep rolling 2000
    hist_trades.sort(key=lambda x: x.get("filed_date",""), reverse=True)

    with open("data/insider_history.json", "w") as f:
        json.dump({
            "last_updated": datetime.now().isoformat(),
            "trades": hist_trades
        }, f, indent=2, default=str)
    print(f"  History: {len(hist_trades)} total trades stored")

    # Confluence: activist + insider on same ticker
    confluence = compute_confluence(filings_13d + filings_13g, form4s)

    for f in filings_13d:
        f["confluence_signal"] = f.get("ticker") in confluence

    output = {
        "last_updated":       datetime.now().isoformat(),
        "filings_13d":        filings_13d,
        "filings_13g":        filings_13g[:20],
        "confluence_tickers": confluence,
    }

    with open("data/activist_filings.json", "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"EDGAR pull complete. Confluence tickers: {confluence}")


if __name__ == "__main__":
    run()