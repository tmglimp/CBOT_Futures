"""
scraper.py
"""
import os
import re
import time
import config
import requests
import pandas as pd
from datetime import datetime
from zeroes import fetch_treasury_data

# ────────────────────────────────────────────────────────────────────────────────
# Fetch CTD Basket List from CME Group
# ────────────────────────────────────────────────────────────────────────────────
def download_tcf_file() -> str:
    print("Connecting to CME for TCF.xlsx metadata …")
    base_url = "https://www.cmegroup.com/trading/interest-rates/treasury-conversion-factors.html"
    headers   = {"User-Agent": "Mozilla/5.0"}
    html = requests.get(base_url, headers=headers, timeout=10).text

    m = re.search(r"Updated U\.S\. Treasury Conversion Factors\s*-\s*(\d{1,2} \w+ \d{4})", html)
    if not m:
        raise RuntimeError("Could not locate update date on CME page.")

    raw_date = m.group(1)
    date_obj = datetime.strptime(raw_date, "%d %B %Y")
    date_str = date_obj.strftime("%Y-%m-%d")
    tcf_url  = f"https://www.cmegroup.com/trading/interest-rates/files/TCF.xlsx?lastUpdated-{date_str}"

    out_path = os.path.join(os.getcwd(), "TCF.xlsx")
    print(f"Downloading TCF.xlsx ({date_str}) …")
    r = requests.get(tcf_url, headers=headers, timeout=15)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)
    print("Saved:", out_path)
    return out_path

# ────────────────────────────────────────────────────────────────────────────────
# Ping Treasury for Detailed SecDef. Derive Spot Dirty CF.
# ────────────────────────────────────────────────────────────────────────────────
def run_scraper() -> None:
    print("Starting UST Index Generator")

    # CME download → dataframe
    tcf_file = pd.DataFrame()
    tcf_file = download_tcf_file()

    # Create USTs.index.csv via your helper
    print("Running fetch_treasury_data() …")
    fetch_treasury_data()             # writes USTs.index.csv

    # Load USTs.index.csv
    csv_name = "UST.index.csv"
    if not os.path.exists(csv_name):
        raise FileNotFoundError(f"Expected {csv_name} produced by fetch_treasury_data()")

    ust_df = pd.read_csv(csv_name, dtype=str)
    if {"cusip", "corpusCusip"} - set(ust_df.columns):
        raise RuntimeError("CSV missing required 'cusip' or 'corpusCusip' columns")


    # Write out enriched index
    ust_df.to_csv("UST.index.csv", index=False)
    print(f"Wrote enriched file UST.index  ({len(ust_df)} rows)")

    # Push into config
    try:
        config.USTs = ust_df.copy()
        print("config.USTs updated.")
    except Exception as exc:
        print("Could not assign to config.USTs:", exc)

    print(ust_df.head())

# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_scraper()
