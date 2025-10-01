import logging
import config
import re
import pandas as pd
import numpy as np
from datetime import datetime
from market_data import refresh_market_data
from fixed_income_calc import BPrice, calculate_ytm

# ---------------- Market Data Import and Sorting ----------------
def refresh_data():
    refresh_market_data()
    return

def normalize_date(date_val):
    """Convert a date value to an 8-digit string (YYYYMMDD)."""
    if pd.isnull(date_val):
        return None
    date_str = str(date_val).strip()
    match = re.search(r"(\d{8})", date_str)
    if match:
        return match.group(1)
    return date_str.replace('-', '').replace(' ', '')

def convert_dates(df: pd.DataFrame, cols, inplace=True, suffix=None) -> pd.DataFrame:
    def to_yyyymmdd(value):
        if isinstance(value, pd.Series):
            dt = pd.to_datetime(value, errors="coerce", utc=False)
            return dt.dt.strftime("%Y%m%d")
        else:
            dt = pd.to_datetime(value, errors="coerce", utc=False)
            return None if pd.isna(dt) else dt.strftime("%Y%m%d")
    out = df if inplace else df.copy()
    for c in cols:
        if c in out.columns:
            formatted = to_yyyymmdd(out[c])
            out[c if suffix is None else f"{c}{suffix}"] = formatted
    return out

# ---------------- Volume Modulation ----------------
def modulate_volume(fut_updated):
    """Convert the api response object of mixed type"""
    df = fut_updated
    logging.info("Processing volume data...")
    def process_value(val):
        if isinstance(val, str) and val.endswith('K'):
            try:
                return float(val[:-1]) * 1000
            except ValueError:
                return float('nan')
        if isinstance(val, str) and val.endswith('M'):
            try:
                return float(val[:-1]) * 1000000
            except ValueError:
                return float('nan')
        try:
            return float(val)
        except ValueError:
            return float('nan')
    df['volume'] = df['volume'].apply(process_value)
    return df

def otr_yld(usts: pd.DataFrame) -> pd.DataFrame:
    """ Extracts on-the-run Treasury yields and returns a DataFrame with years_to_maturity, symbol, and corresponding market yield."""
    # Normalize columns
    usts.columns = usts.columns.str.strip().str.lower()

    # Filter on-the-run issues
    filtered = usts[usts['otr_issue'].astype(str).str.strip().str.upper() == "YES"].copy()
    filtered['years_round'] = pd.to_numeric(filtered['years_to_maturity'], errors='coerce').round()
    filtered['yld'] = pd.to_numeric(filtered['yield'], errors='coerce')

    # Map of rounded original maturity to yield
    maturity_yld_map = dict(zip(filtered['years_to_maturity'], filtered['yld']))

    otr_df = pd.DataFrame([
        {"years_round": 2, "ticker": "ZT"},
        {"years_round": 3, "ticker": "Z3N"},
        {"years_round": 5, "ticker": "ZF"},
        {"years_round": 7, "ticker": "ZN"},
        {"years_round": 10, "ticker": "TN"},
    ])

    otr_df["yld"] = otr_df["years_to_maturity"].map(maturity_yld_map)
    return otr_df

# ---------------- FUTURES -> HEDGES Transformation ----------------
def transform_futures_hedges():
    """ Transform FUTURES into HEDGES. """
    futures_df = config.FUTURES
    new_rows = []

    for idx, row in futures_df.iterrows():
        raw_last = row.get('last_price')
        if isinstance(raw_last, str) and raw_last.lower().startswith("c"):
            continue

        bid_dec = row.get('bid_price')
        bid_y = row.get('bid_yield')
        ask_dec = row.get('ask_price')
        ask_y = row.get('ask_yield')
        last_dec = row.get('last_price')
        bid_valid = (bid_dec is not None) and (not pd.isna(bid_dec)) and (bid_dec != 0)
        ask_valid = (ask_dec is not None) and (not pd.isna(ask_dec)) and (ask_dec != 0)
        last_valid = (last_dec is not None) and (not pd.isna(last_dec))

        if bid_valid and ask_valid:
            row_bid = row.copy()
            row_bid['price'] = bid_dec
            row_bid['yield'] = bid_y
            row_bid['src'] = 'bid'
            new_rows.append(row_bid)
            row_ask = row.copy()
            row_ask['price'] = ask_dec
            row_ask['yield'] = ask_y
            row_ask['src'] = 'ask'
            new_rows.append(row_ask)
        elif last_valid:
            row_single = row.copy()
            row_single['price'] = last_dec
            row_single['src'] = 'last'
            new_rows.append(row_single)
        else:
            continue

    hedges_df = pd.DataFrame(new_rows)
    hedges_df = modulate_volume(hedges_df)  # Reassign HEDGES = df inside modulate_volume
    hedges_df.columns = hedges_df.columns.astype(str).str.lower().str.strip()
    hedges_df = hedges_df.add_prefix("fut_")
    logging.info("Transformed FUTURES into HEDGES with %d rows.", len(hedges_df))
    return hedges_df

def avg_ust_by_conid(df: pd.DataFrame | None = None) -> pd.DataFrame:
    TARGET_COLS = ["bid_yield", "ask_yield", "yield", "ask_price", "bid_price", "price"]

    if df is None:
        df = getattr(config, "ust_hist_y", None)
    if df is None or df.empty:
        raise ValueError("ust_hist_y (or provided df) is missing or empty.")
    if "conid" not in df.columns:
        raise KeyError("DataFrame must contain a 'conid' column.")

    present = [c for c in TARGET_COLS if c in df.columns]
    if not present:
        return df.dropna(subset=["conid"]).groupby("conid", as_index=False).first()

    work = df.dropna(subset=["conid"]).copy()
    if work.empty:
        return df.head(0)

    base = work.groupby("conid", as_index=False).first()
    means = (work.groupby("conid", as_index=False)[present].mean(numeric_only=True))
    base = base.set_index("conid")
    means = means.set_index("conid")
    base.loc[:, present] = means[present]
    results = base.reset_index()
    print(results.head())
    return results

# ---------------- Helper Function ----------------
def safe_datetime(val):
    try:
        if pd.isna(val):
            return None
        dt = pd.to_datetime(val, errors='coerce')
        return None if pd.isna(dt) else dt
    except Exception:
        return None

# ---------- IRR-based Fair Value Derivation for CTD Baskets ----------------
def fair_value_derivation():
    implied = avg_ust_by_conid(config.ust_hist_y)
    implied['years_to_maturity'] = pd.to_numeric(implied['years_to_maturity'], errors='coerce')
    implied['coupon'] = pd.to_numeric(implied['coupon'], errors='coerce')
    implied['conversion_factor'] = pd.to_numeric(implied['conversion_factor'], errors='coerce')
    cols = ['prev_coupon', 'next_coupon','maturity_date']
    implied = convert_dates(implied, cols)
    required_cols = ['coupon', 'years_to_maturity', 'conversion_factor', 'prev_coupon', 'maturity_date', 'next_coupon']
    for col in required_cols:
        print(f"-> {implied[col].isna().sum()} missing in {col}")
    implied = implied.dropna(subset=required_cols)
    implied['yield'] = implied['yield']/100
    settle_date = datetime.today().strftime('%Y%m%d')
    implied['BPrice'] = implied.apply(lambda row: BPrice(cpn=row['coupon'],term=row['years_to_maturity'],yield_=row['yield'],
        period=2,begin=row['prev_coupon'],settle=settle_date,next_coupon=row['next_coupon'],day_count=1), axis=1)
    implied = implied.drop(columns=["87_raw", "6508","increment_lower_edge", "strike", "avg_price"], errors="ignore")
    return implied

# ---------- CTD Pairing ----------------
def ctd_pairing(HEDGES, implied):
    print("Starting CTD pairing")
    today = pd.to_datetime(datetime.now())

    for idx, fut in HEDGES.iterrows():
        symbol = fut.get("fut_ticker", "")[:2]
        sym_full = fut.get("fut_ticker", "")
        expiry = float(fut.get("fut_year_to_maturity", np.nan))
        fut_price = float(fut.get("fut_price", np.nan))
        print(f"\n-> Processing row {idx}: symbol={sym_full}, expiry={expiry}, Fwd Mkt Price={fut_price}")

        if pd.isna(expiry) or pd.isna(fut_price):
            continue

        # Determine deliverable window and original maturity cap
        if symbol == "ZQ":
            lower = expiry
            upper = lower + (30 / 360)
            max_origin = np.inf
        elif symbol == "ZT":
            lower = expiry + 1.72
            upper = expiry + 2.03
            max_origin = 5.28
        elif symbol == "Z3":
            lower = expiry + 2.72
            upper = expiry + 3.03
            max_origin = 7.03
        elif symbol == "ZF":
            lower = expiry + 4.16
            upper = expiry + 5.28
            max_origin = 5.27
        elif symbol == "ZN":
            lower = expiry + 6.47
            upper = expiry + 8.03
            max_origin = 10.03
        elif symbol == "TN":
            lower = expiry + 9.47
            upper = expiry + 10.03
            max_origin = 10.03
        else:
            print(f"Unknown prefix for symbol '{sym_full}' — skipping")
            continue

        print(f"-> Deliverable range: {lower:.2f} to {upper:.2f}, max_origin: {max_origin}")
        (implied["original_maturity"].astype(float) <= max_origin)
        candidates = implied[(implied["years_to_maturity"] >= lower) &(implied["years_to_maturity"] <= upper) &
                             (pd.to_numeric(implied["original_maturity"], errors="coerce") <= max_origin)].copy()

        print(f"-> {len(candidates)} candidates after filter for {symbol}")
        candidates = candidates
        if candidates.empty or "BPrice" not in candidates or candidates["BPrice"].isna().all():
            print("Candidates missing price column or all prices are NaN")
            continue

        candidates["Gross_Basis"] = (fut_price * candidates["conversion_factor"]) - candidates["BPrice"]
        candidates["IRR"] = (((fut_price * candidates["conversion_factor"]) - candidates["BPrice"])/candidates["BPrice"] - 1) * (candidates["years_to_maturity"] * 365)/365
        candidates["YTM"] = candidates["years_to_maturity"]

        selected = candidates.sort_values("IRR", ascending=True).iloc[0]
        HEDGES.at[idx, 'ctd_BPrice'] = selected.get('BPrice')
        HEDGES.at[idx, 'ctd_gross_basis'] = selected.get('Gross_Basis')
        HEDGES.at[idx, 'ctd_irr'] = selected.get('IRR')
        HEDGES.at[idx, 'ctd_ytm'] = selected.get('YTM')
        HEDGES.at[idx, 'carry'] = HEDGES.at[idx, 'ctd_gross_basis'] - HEDGES.at[idx, 'ctd_BPrice'] * HEDGES.at[idx, 'ctd_irr'] * (HEDGES.at[idx, 'ctd_ytm']*365//365) /365
        HEDGES.at[idx, 'ctd_cusip'] = selected.get('cusip_y')
        HEDGES.at[idx, 'ctd_conid'] = selected.get('conid')
        HEDGES.at[idx, 'ctd_price'] = selected.get('price')
        HEDGES.at[idx, 'ctd_yield'] = selected.get('yield')
        HEDGES.at[idx, 'ctd_gross_basis'] = selected.get('Gross_Basis')
        HEDGES.at[idx, 'ctd_irr'] = selected.get('IRR')
        HEDGES.at[idx, 'ctd_coupon_rate'] = selected.get('coupon')
        HEDGES.at[idx, 'ctd_maturity_date'] = selected.get('maturity_date')
        HEDGES.at[idx, 'ctd_cf'] = selected.get('conversion_factor')
        HEDGES.at[idx, 'ctd_prev_coupon'] = selected.get('prev_coupon')
        HEDGES.at[idx, 'ctd_next_coupon'] = selected.get('next_coupon')
        print(f"{sym_full} CTD conid: {selected['conid']}, IRR: {selected['IRR']}, Gross Basis: {selected['Gross_Basis']}")

    print("CTD pairing complete")
    HEDGES.to_csv("HEDGES.csv", index=False)
    config.HEDGES = HEDGES

    return HEDGES

# ---------------- Main ----------------
def cf_ctd_main():
    logging.info("Starting cf_ctd processing script.")
    refresh_data()
    HEDGES = transform_futures_hedges()
    implied = fair_value_derivation()
    HEDGES = ctd_pairing(HEDGES, implied)
    return HEDGES

if __name__ == "__main__":
    cf_ctd_main()
