"""
pull_insider_history.py - 1 year of insider purchases via OpenInsider date ranges
"""
import requests, pandas as pd, json
from io import StringIO
from datetime import datetime, timedelta

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}

def fetch_range(start_dt, end_dt):
    s = start_dt.strftime("%m/%d/%Y")
    e = end_dt.strftime("%m/%d/%Y")
    url = (f"http://openinsider.com/screener?s=&o=&pl=50&ph=&ll=&lh=&"
           f"fd=-1&fdr={s}+-+{e}&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&"
           f"vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&"
           f"grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&"
           f"oe=&ogr=&sector=&action=Filter")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        tables = pd.read_html(StringIO(resp.text))
        return next((t for t in tables if t.shape[1] >= 15 and t.shape[0] > 5), None)
    except Exception as e:
        print(f"    Error: {e}")
        return None

def parse_df(df):
    trades = []
    df.columns = [c.replace('\xa0',' ') for c in df.columns]
    for _, row in df.iterrows():
        try:
            if "P - Purchase" not in str(row.get("Trade Type","")):
                continue
            def clean(v):
                return str(v).replace("$","").replace(",","").replace("+","").strip()
            price = float(clean(row["Price"])) if clean(row["Price"]) not in ("nan","") else 0
            qty   = int(float(clean(row["Qty"]))) if clean(row["Qty"]) not in ("nan","") else 0
            value = float(clean(row["Value"])) if clean(row["Value"]) not in ("nan","") else price*qty
            if value < 50000:
                continue
            ticker = str(row.get("Ticker","")).strip().upper()
            if ticker in ("nan","","NAN"):
                continue
            role  = str(row.get("Title","")).strip()
            role_l = role.lower()
            trades.append({
                "filed_date":  str(row.get("Filing Date","")).strip(),
                "trade_date":  str(row.get("Trade Date","")).strip(),
                "ticker":      ticker,
                "company":     str(row.get("Company Name","")).strip(),
                "insider":     str(row.get("Insider Name","")).strip(),
                "role":        role,
                "shares":      qty,
                "price":       price,
                "value_usd":   value,
                "value_m":     round(value/1e6,3),
                "is_ceo":      any(t in role_l for t in ["ceo","chief executive"]),
                "is_director": "dir" in role_l,
                "is_ten_pct":  "10%" in role_l,
                "source":      "openinsider",
                "sec_link":    f"https://openinsider.com/{ticker}",
            })
        except:
            continue
    return trades

def run():
    print("Fetching 1-year insider purchases (12 x 30-day windows)...")
    all_trades = []
    end = datetime.today()

    for i in range(12):
        chunk_end   = end - timedelta(days=i*30)
        chunk_start = chunk_end - timedelta(days=30)
        print(f"  Window {i+1}/12: {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}...")
        df = fetch_range(chunk_start, chunk_end)
        if df is None:
            print("    No data, skipping.")
            continue
        trades = parse_df(df)
        all_trades.extend(trades)
        print(f"    +{len(trades)} trades (running total: {len(all_trades)})")

    # Deduplicate
    seen, deduped = set(), []
    for t in all_trades:
        key = (t["ticker"], t["insider"], t.get("trade_date",""), str(int(t["value_usd"])))
        if key not in seen:
            seen.add(key)
            deduped.append(t)

    deduped.sort(key=lambda x: x.get("filed_date",""), reverse=True)
    print(f"\nUnique purchases: {len(deduped)}")

    # Merge with existing
    try:
        existing = json.load(open("data/insider_history.json")).get("trades",[])
    except:
        existing = []

    seen2, merged = set(), []
    for t in deduped + existing:
        key = (t.get("ticker",""), t.get("insider",""),
               t.get("trade_date", t.get("filed_date","")),
               str(int(float(t.get("value_usd",0)))))
        if key not in seen2:
            seen2.add(key)
            merged.append(t)

    merged.sort(key=lambda x: x.get("filed_date",""), reverse=True)
    merged = merged[:5000]

    with open("data/insider_history.json","w") as f:
        json.dump({"last_updated": datetime.now().isoformat(), "trades": merged}, f, indent=2, default=str)
    print(f"Saved {len(merged)} trades to insider_history.json")

    today = datetime.today().strftime("%Y-%m-%d")
    recent = [t for t in merged if t.get("filed_date","")[:10] >= today]
    if not recent:
        recent = merged[:50]
    with open("data/insider_trades.json","w") as f:
        json.dump({"last_updated": datetime.now().isoformat(), "trades": recent[:50]}, f, indent=2, default=str)
    print(f"Updated insider_trades.json with {len(recent[:50])} trades")

if __name__ == "__main__":
    run()
