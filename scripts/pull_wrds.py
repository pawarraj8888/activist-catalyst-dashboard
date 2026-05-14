"""
pull_wrds.py  -  WRDS data layer
Pulls: IBES surprise history, CRSP realized moves, Insiders Form 4, Compustat snapshot
"""

import wrds
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
WRDS_USER = os.getenv("WRDS_USERNAME", "rajpawar88")


def connect():
    return wrds.Connection(wrds_username=WRDS_USER)


def explore_library(db, lib):
    """Run once to confirm table names in any library"""
    print(f"Tables in {lib}:", db.list_tables(library=lib))


def pull_ibes(db):
    print("  IBES surprise history...")
    query = """
        SELECT  ticker, anndats AS announce_date,
                actual, surpmean AS surprise_vs_mean,
                suescore
        FROM    ibes.surpsumu
        WHERE   fiscalp = 'QTR' AND usfirm = 1
        AND     anndats >= CURRENT_DATE - INTERVAL '2 years'
        ORDER   BY ticker, anndats DESC
    """
    df = db.raw_sql(query)
    if df.empty:
        return {}, df

    enrichment = {}
    for ticker, grp in df.groupby("ticker"):
        last8 = grp.head(8)
        enrichment[ticker] = {
            "avg_surprise":      round(float(last8["surprise_vs_mean"].mean()), 4),
            "beat_rate":         round(float((last8["surprise_vs_mean"] > 0).mean()), 4),
            "quarters_analyzed": int(len(last8)),
            "last_surprise":     round(float(last8.iloc[0]["surprise_vs_mean"]), 4),
                    }

    print(f"  IBES: {len(enrichment)} tickers")
    return enrichment, df


def pull_realized_moves(db, ibes_raw):
    """Use yfinance for realized moves — faster and no CRSP permno issues"""
    import yfinance as yf
    print("  Realized moves via yfinance...")

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

    realized = {}
    for ticker in UNIVERSE:
        try:
            t = yf.Ticker(ticker)
            # Get earnings dates with actual vs estimate
            ed = t.earnings_dates
            if ed is None or ed.empty:
                continue
            ed = ed.dropna(subset=["EPS Estimate"]).head(8)
            if ed.empty:
                continue
            # Get 2yr daily prices
            hist = t.history(period="2y")
            if hist.empty:
                continue
            hist.index = hist.index.tz_localize(None)
            moves = []
            for dt in ed.index:
                dt_naive = dt.tz_localize(None) if hasattr(dt, "tz_localize") else dt
                for delta in [0, 1, -1]:
                    d = dt_naive + timedelta(days=delta)
                    matches = hist.index[hist.index.date == d.date()]
                    if len(matches):
                        close = hist.loc[matches[0], "Close"]
                        prev = hist["Close"].shift(1).loc[matches[0]]
                        if prev and prev > 0:
                            moves.append(abs((close - prev) / prev))
                            break
            if moves:
                realized[ticker] = {
                    "avg_realized_move": round(float(np.mean(moves)), 4),
                    "max_realized_move": round(float(np.max(moves)), 4),
                    "n_obs":             len(moves),
                }
        except Exception as e:
            continue

    print(f"  Realized moves: {len(realized)} tickers")
    return realized


def pull_insiders(db):
    print("  Insider trades (Form 4)...")
    cutoff = (datetime.today() - timedelta(days=60)).strftime("%Y-%m-%d")

    queries = [
        f"""
        SELECT  ticker, issuername, rptownername, rptownerrelationship,
                transactiondate, transactionshares, transactionpricepershare,
                transactionacquireddisposedcode
        FROM    wrds_insiders.nonderivative_transactions
        WHERE   transactiondate >= '{cutoff}'
        AND     transactionacquireddisposedcode = 'A'
        AND     transactionshares > 0
        ORDER   BY transactiondate DESC LIMIT 500
        """,
        f"""
        SELECT * FROM wrds_insiders.transactions
        WHERE  trans_date >= '{cutoff}' AND trans_code = 'P'
        ORDER  BY trans_date DESC LIMIT 500
        """,
    ]

    for q in queries:
        try:
            df = db.raw_sql(q)
            if df is not None and not df.empty:
                print(f"  Insiders: {len(df)} trades")
                return df
        except Exception as e:
            print(f"    attempt failed: {e}")

    print("  Note: wrds_insiders not in NYU subscription. Using EDGAR Form 4 feed instead.")
    return pd.DataFrame()


def pull_compustat(db, tickers):
    if not tickers:
        return {}
    t_str = "','".join(tickers[:50])
    print(f"  Compustat for {len(tickers)} tickers...")
    try:
        df = db.raw_sql(f"""
            SELECT f.tic AS ticker, f.conm AS company_name,
                   f.prcc_f AS price, f.csho AS shares_out,
                   f.at AS total_assets, f.dltt AS long_term_debt,
                   f.ebitda, f.revt AS revenue, f.ni AS net_income
            FROM   comp.funda f
            INNER JOIN (
                SELECT tic, MAX(datadate) md FROM comp.funda
                WHERE  tic IN ('{t_str}') GROUP BY tic
            ) mx ON f.tic = mx.tic AND f.datadate = mx.md
            WHERE  f.indfmt='INDL' AND f.datafmt='STD'
            AND    f.popsrc='D'   AND f.consol='C'
        """)
        result = {}
        for _, row in df.iterrows():
            mktcap = (row["price"] or 0) * (row["shares_out"] or 0)
            ebitda = row["ebitda"] or 0
            ev     = mktcap + (row["long_term_debt"] or 0)
            result[row["ticker"]] = {
                "name":       row["company_name"],
                "mktcap_m":   round(mktcap, 1),
                "ev_ebitda":  round(ev / ebitda, 1) if ebitda else None,
                "revenue_m":  round(row["revenue"] or 0, 1),
                "net_debt_m": round(row["long_term_debt"] or 0, 1),
            }
        return result
    except Exception as e:
        print(f"  Compustat error: {e}")
        return {}


def run():
    print("Connecting to WRDS...")
    db = connect()

    ibes_enrichment, ibes_raw = pull_ibes(db)
    with open("data/ibes_enrichment.json", "w") as f:
        json.dump({"last_updated": datetime.now().isoformat(),
                   "data": ibes_enrichment}, f, indent=2, default=str)

    realized = pull_realized_moves(db, ibes_raw)
    with open("data/realized_moves.json", "w") as f:
        json.dump({"last_updated": datetime.now().isoformat(),
                   "data": realized}, f, indent=2, default=str)

    insider_df = pull_insiders(db)
    trades = insider_df.head(200).to_dict(orient="records") if not insider_df.empty else []
    with open("data/insider_trades.json", "w") as f:
        json.dump({"last_updated": datetime.now().isoformat(),
                   "trades": trades}, f, indent=2, default=str)

    db.close()
    print("WRDS pull complete.")


if __name__ == "__main__":
    run()
