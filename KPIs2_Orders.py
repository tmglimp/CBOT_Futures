"""
KPIs2_Orders
"""
import json
import logging
import math
import numpy as np
import pandas as pd
import requests
import urllib3
import config
from datetime import datetime
from config import updated_ORDERS
from leaky_bucket import leaky_bucket
from risklimits import compute_risk_metrics

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# SIA-Standardized KPI Derivations
def accrued_interest(coupon, mat_date, today):
    try:
        last_coupon = mat_date.replace(year=today.year)
    except ValueError:
        last_day = pd.Timestamp(today.year, mat_date.month, 1) + pd.offsets.MonthEnd(0)
        last_coupon = last_day

    if last_coupon > today:
        last_coupon -= pd.DateOffset(months=6)

    days_accrued = (today - last_coupon).days
    return (coupon / 2) * (days_accrued / 182.5)

def sia_implied_repo(fut_price, dirty_price, cf, days):
    adj_fut = fut_price * cf
    return (((adj_fut - dirty_price) / dirty_price)-1) * (365 / days)

def sia_gross_basis(fut_price, cf, dirty_price):
    return fut_price * cf - dirty_price

def sia_convexity_yield(dirty_price, coupon, days):
    return ((coupon / dirty_price) * (days / 365)) * (365 / days)

def sia_carry(gross_basis, implied_repo, dirty_price, days):
    financing_cost = dirty_price * implied_repo * days / 365
    return gross_basis - financing_cost

def sia_net_basis(gross_basis, carry):
    return gross_basis + carry

def fut_tail(A_FUT_DV01, A_MULT, B_FUT_DV01, B_MULT):
    return (-1 *
        (((A_FUT_DV01 * A_MULT) - (B_FUT_DV01 * B_MULT)) / (B_FUT_DV01 * B_MULT))
        if (A_FUT_DV01 * A_MULT) > (B_FUT_DV01 * B_MULT)
        else (((B_FUT_DV01 * B_MULT) - (A_FUT_DV01 * A_MULT)) / (A_FUT_DV01 * A_MULT))
    )

def fwd_fut_tail(A_FUT_DV01, A_FWD_DV01, A_MULT, B_FUT_DV01, B_FWD_DV01, B_MULT):
    return (-1 *
        ((((A_FUT_DV01+A_FWD_DV01) * A_MULT) - ((B_FUT_DV01+B_FWD_DV01) * B_MULT)) /
                ((B_FUT_DV01+B_FWD_DV01) * B_MULT))
        if (A_FUT_DV01 * A_MULT) > (B_FUT_DV01 * B_MULT)
        else ((((B_FUT_DV01+B_FWD_DV01) * B_MULT) - ((A_FUT_DV01+A_FWD_DV01) * A_MULT)) /
                ((A_FUT_DV01+A_FWD_DV01) * A_MULT))
    )

def filter_updated_orders(HEDGES_Combos: pd.DataFrame) -> pd.DataFrame:
    def pick(cols, *futdets):
        filter_1 = {c.lower(): c for c in cols}
        for futdet in futdets:
            if futdet.lower() in filter_1:
                return filter_1[futdet.lower()]
        raise KeyError(f"Missing required column (tried: {futdets})")

    # Columns (support a couple common aliases)
    AQ   = pick(HEDGES_Combos.columns, "A_Q_Value")
    BQ   = pick(HEDGES_Combos.columns, "B_Q_Value")
    AYTM = pick(HEDGES_Combos.columns, "A_FUT_YEAR_TO_MATURITY")
    BYTM = pick(HEDGES_Combos.columns, "B_FUT_YEAR_TO_MATURITY")

    # Make sure YTM columns are numeric
    a_ytm = pd.to_numeric(HEDGES_Combos[AYTM], errors="coerce")
    b_ytm = pd.to_numeric(HEDGES_Combos[BYTM], errors="coerce")

    # Thresholds (as requested)
    long_thresh  = 36 / 360
    short_thresh =  2 / 360

    cond_a_long  = (HEDGES_Combos[AQ] ==  1)
    cond_a_short = (HEDGES_Combos[AQ] == -1)
    cond_b_long  = (HEDGES_Combos[BQ] ==  1)
    cond_b_short = (HEDGES_Combos[BQ] == -1)

    a_ok_long  = a_ytm.gt(long_thresh).fillna(False)
    a_ok_short = a_ytm.gt(short_thresh).fillna(False)
    b_ok_long  = b_ytm.gt(long_thresh).fillna(False)
    b_ok_short = b_ytm.gt(short_thresh).fillna(False)
    mask = ((~cond_a_long  | a_ok_long)  &
            (~cond_a_short | a_ok_short) &
            (~cond_b_long  | b_ok_long)  &
            (~cond_b_short | b_ok_short))

    return HEDGES_Combos[mask].copy()

def calculate_quantities_with_sma(HEDGES_Combos):
    today = pd.to_datetime(datetime.now())

    for leg in ['A', 'B']:
        coupons = HEDGES_Combos[f'{leg}_CTD_COUPON_RATE']
        mat_dates = pd.to_datetime(HEDGES_Combos[f'{leg}_CTD_MATURITY_DATE'])
        HEDGES_Combos[f'{leg}_AccruedInterest'] = [accrued_interest(c, m, today) for c, m in zip(coupons, mat_dates)]

        col = f"{leg}_FUT_EXPIRY"
        HEDGES_Combos[col] = pd.to_datetime(HEDGES_Combos[col].astype(str), format="%Y%m%d", errors="coerce")
        HEDGES_Combos[f"{leg}_Days"] = (HEDGES_Combos[col] - today).dt.days.astype("Int64")
        HEDGES_Combos[f'{leg}_Carry'] = [
            sia_carry(gb, repo, dp, d)
            for gb, repo, dp, d in zip(
                HEDGES_Combos[f'{leg}_CTD_GROSS_BASIS'],
                HEDGES_Combos[f'{leg}_CTD_IRR'],
                HEDGES_Combos[f'{leg}_CTD_BPRICE'],
                HEDGES_Combos[f'{leg}_Days'])]
        HEDGES_Combos[f'{leg}_NetBasis'] = [
            sia_net_basis(gb, carry)
            for gb, carry in zip(HEDGES_Combos[f'{leg}_CTD_GROSS_BASIS'], HEDGES_Combos[f'{leg}_Carry'])]

    nl_value = get_acct_dets()
    nl_value = float(nl_value)
    SMA = nl_value * 4  # approximate reg-t margin limit
    config.SMA = SMA
    print(f'SMA => {SMA}')
    return config.SMA

def calculate_quantities(HEDGES_Combos):
    SMA = calculate_quantities_with_sma(HEDGES_Combos)
    if SMA > 2000:
        HEDGES_Combos['A_Q_Value'], HEDGES_Combos['B_Q_Value'] = 1, 1
        HEDGES_Combos['A_Q_Value'] = np.where(
            HEDGES_Combos['A_CTD_IRR'] < HEDGES_Combos['B_CTD_IRR'], -1, 1)
        HEDGES_Combos['B_Q_Value'] = np.where(
            HEDGES_Combos['A_CTD_IRR'] < HEDGES_Combos['B_CTD_IRR'], 1, -1)

    HEDGES_Combos = filter_updated_orders(HEDGES_Combos)
    # Convert volume columns to numeric.
    HEDGES_Combos['A_FUT_VOLUME'] = pd.to_numeric(HEDGES_Combos['A_FUT_VOLUME'], errors='coerce')
    HEDGES_Combos['B_FUT_VOLUME'] = pd.to_numeric(HEDGES_Combos['B_FUT_VOLUME'], errors='coerce')
    HEDGES_Combos = HEDGES_Combos.dropna(subset=['A_FUT_VOLUME', 'B_FUT_VOLUME'])
    HEDGES_Combos['ln_A_FUT'] = np.log(HEDGES_Combos['A_FUT_VOLUME'])
    HEDGES_Combos['ln_B_FUT'] = np.log(HEDGES_Combos['B_FUT_VOLUME'])
    HEDGES_Combos['Base_Ln_A'] = (1 + HEDGES_Combos['ln_A_FUT']/config.VS)
    HEDGES_Combos['Base_Ln_B'] = (1 + HEDGES_Combos['ln_B_FUT']/config.VS)
    HEDGES_Combos['Z_Ln_WeightedVol'] = ((HEDGES_Combos['Base_Ln_A'] + HEDGES_Combos['Base_Ln_B'])/2)

    # Compute RENTD metric.
    HEDGES_Combos['A_NetBasis'] = HEDGES_Combos.apply(
        lambda row: sia_net_basis(gross_basis=row['A_CTD_GROSS_BASIS'],carry=row['A_Carry']), axis=1)
    HEDGES_Combos['B_NetBasis'] = HEDGES_Combos.apply(
        lambda row: sia_net_basis(gross_basis=row['B_CTD_GROSS_BASIS'],carry=row['B_Carry']), axis=1)

    HEDGES_Combos["A_NetBasis"]         = pd.to_numeric(HEDGES_Combos["A_NetBasis"], errors="coerce")
    HEDGES_Combos['A_FUT_MULTIPLIER']   = pd.to_numeric(HEDGES_Combos["A_FUT_MULTIPLIER"], errors="coerce")
    HEDGES_Combos['A_Q_Value']          = pd.to_numeric(HEDGES_Combos["A_Q_Value"], errors="coerce")
    HEDGES_Combos['B_NetBasis']         = pd.to_numeric(HEDGES_Combos["B_NetBasis"], errors="coerce")
    HEDGES_Combos['B_FUT_MULTIPLIER']   = pd.to_numeric(HEDGES_Combos["B_FUT_MULTIPLIER"], errors="coerce")
    HEDGES_Combos['B_Q_Value']          = pd.to_numeric(HEDGES_Combos["B_Q_Value"], errors="coerce")

    for c in ["A_Q_Value", "B_Q_Value", "A_NetBasis", "B_NetBasis"]:
        HEDGES_Combos[c] = pd.to_numeric(HEDGES_Combos[c], errors="coerce")

    conds = [HEDGES_Combos["B_Q_Value"].eq(-1),
        HEDGES_Combos["A_Q_Value"].eq(-1)]
    choices = [HEDGES_Combos["B_NetBasis"] - HEDGES_Combos["A_NetBasis"],
        HEDGES_Combos["A_NetBasis"] - HEDGES_Combos["B_NetBasis"]]

    HEDGES_Combos["PositionNetBasis"] = np.select(conds, choices, default=np.nan)
    HEDGES_Combos['val_vol'] = HEDGES_Combos['PositionNetBasis'] * HEDGES_Combos['Z_Ln_WeightedVol']
    HEDGES_Combos = HEDGES_Combos.sort_values(by='val_vol', ascending=True)

    unique_rows = (HEDGES_Combos.drop_duplicates(subset=['A_FUT_CONID', 'B_FUT_CONID'], keep='first').copy())

    if len(unique_rows) >= 5:
        A = unique_rows.iloc[0]
        B = unique_rows.iloc[1]
        C = unique_rows.iloc[2]

    else:
        # Assign defaults or replicate the last row enough times to have 3 rows.
        default = unique_rows.iloc[0] if len(unique_rows) > 0 else pd.Series(
            {col: None for col in HEDGES_Combos.columns})
        A = unique_rows.iloc[0] if len(unique_rows) > 0 else default
        B = unique_rows.iloc[1] if len(unique_rows) > 1 else default
        C = unique_rows.iloc[2] if len(unique_rows) > 2 else default

    unique_rows2 = HEDGES_Combos.sort_values(by='val_vol', ascending=False)

    if len(unique_rows2) >= 5:
        F = unique_rows2.iloc[0]
        G = unique_rows2.iloc[1]
        H = unique_rows2.iloc[2]

    else:
        # Assign defaults or replicate the last row enough times to have 3 rows.
        default = unique_rows2.iloc[0] if len(unique_rows2) > 0 else pd.Series(
            {col: None for col in HEDGES_Combos.columns})
        F = unique_rows2.iloc[0] if len(unique_rows2) > 0 else default
        G = unique_rows2.iloc[1] if len(unique_rows2) > 1 else default
        H = unique_rows2.iloc[2] if len(unique_rows2) > 2 else default

    config.ORDERS = pd.DataFrame([A,B,C,F,G,H], columns=HEDGES_Combos.columns)
    config.ORDERS.to_csv('config.ORDERS.csv')

    # call risklimits here
    config.updated_ORDERS = compute_risk_metrics(config.ORDERS)
    config.updated_ORDERS.to_csv('updated_ORDERS.csv')
    return config.updated_ORDERS

def optimize_quantities_for_row(row, limit):
    """
    For a given row (i.e. for one hedge pair), find the integer quantities Q_A and Q_B
    (with Q_A >= 1 and Q_B >= 1) such that the total cost:
       cost = Q_A * (A_FUT_MULTIPLIER * A_FUT_PRICE) + Q_B * (B_FUT_MULTIPLIER * B_FUT_PRICE)
    is maximized while remaining <= limit,
    and such that Q_A/Q_B is as close as possible to the DV01 ratio, r.
    """
    cost_A = row['A_FUT_MULTIPLIER'] * row['A_FUT_PRICE']
    cost_B = row['B_FUT_MULTIPLIER'] * row['B_FUT_PRICE']
    r = row['A_FUT_DV01'] / row['B_FUT_DV01']

    best_q_a = None
    best_q_b = None
    best_cost = -1
    best_error = float('inf')
    max_q_b = int(limit // cost_B) if cost_B > 0 else 1
    for q_b in range(1, max_q_b + 1):
        q_a_candidate = int(round(r * q_b))
        if q_a_candidate < 1:
            q_a_candidate = 1
        cost_candidate = q_a_candidate * cost_A + q_b * cost_B
        if cost_candidate <= limit:
            error_candidate = abs((q_a_candidate / q_b) - r)
            if cost_candidate > best_cost:
                best_cost = cost_candidate
                best_q_a = q_a_candidate
                best_q_b = q_b
                best_error = error_candidate
            elif cost_candidate == best_cost and error_candidate < best_error:
                best_q_a = q_a_candidate
                best_q_b = q_b
                best_error = error_candidate

    if best_q_a is None or best_q_b is None:
        best_q_a, best_q_b = 1, 1
    return pd.Series({'A_Q_Value': best_q_a, 'B_Q_Value': best_q_b})
