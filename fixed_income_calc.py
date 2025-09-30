"""
fixed_income_calc.py
"""
from math import pow
import pandas as pd
from datetime import datetime, timedelta

### THANKS TO PROF. JOHN MILLER OF THE U. IL AT CHICAGO FOR SHARING MUCH OF THE BELOW WITH THE UIC MS FIN. DEPT. ###

"""
This module computes SIA/FIA standardized fixed‑income metrics for U.S. Treasury bonds including:
 - Theoretical price for spot dirty no-arbitrage derivations (BPrice)
 - Yield‑to‑maturity and price-to-yield (calculate_ytm and P2Y)
 - Accrued interest (AInt)
 - Modified duration (MDur)
 - Macaulay duration (MacDur)
 - DV01 (PVBP)
 - Convexity (Cvx)
 - Approximate duration
 - Approximate convexity
"""

def round_ytm(ytm):
    if pd.isnull(ytm):
        return None
    return round(ytm * 2) / 2.0

def calculate_term(settlement_date_str, maturity_date_str, day_count_convention=365.25):
    settlement_date = datetime.strptime(settlement_date_str, '%Y%m%d')
    maturity_date = datetime.strptime(maturity_date_str, '%Y%m%d')
    days_to_maturity = (maturity_date - settlement_date).days
    term_in_years = days_to_maturity / day_count_convention
    return term_in_years

def compute_settlement_date(trade_date, t_plus=1):
    if isinstance(trade_date, str):
        trade_date = datetime.strptime(trade_date, '%Y%m%d')
    settlement_date = trade_date
    business_days_added = 0
    while business_days_added < t_plus:
        settlement_date += timedelta(days=1)
        if settlement_date.weekday() < 5:  # Monday=0, ..., Friday=4
            business_days_added += 1
    return settlement_date.strftime('%Y%m%d')

def accrual_period(begin, settle, next_coupon, day_count=1):
    if day_count == 1:
        L = datetime.strptime(str(begin), '%Y%m%d')
        S = datetime.strptime(str(settle), '%Y%m%d')
        N = datetime.strptime(str(next_coupon if next_coupon is not None else settle), '%Y%m%d')
        return (S - L).days / (N - L).days
    else:
        # 30/360 convention
        L = [int(begin[:4]), int(begin[4:6]), int(begin[6:8])]
        S = [int(settle[:4]), int(settle[4:6]), int(settle[6:8])]
        return (360 * (S[0] - L[0]) + 30 * (S[1] - L[1]) + S[2] - L[2]) / 180

def AInt(cpn, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    v = accrual_period(begin, settle, next_coupon, day_count)
    return cpn / period * v

def BPrice(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    if term is None or yield_ is None:
        return None

    rounded_term = round_ytm(term)
    if rounded_term is None:
        return None
    T = int(rounded_term * period)  # Total coupon periods
    C = cpn / period
    Y = yield_ / period

    try:
        price = C * (1 - pow(1 + Y, -T)) / Y + 100 / pow(1 + Y, T)
    except ZeroDivisionError:
        price = None

    if begin and settle and next_coupon:
        ai = AInt(cpn, period=2, begin=begin, settle=settle, next_coupon=next_coupon, day_count=day_count)
        price = price + ai
    return price

def TPrice(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,conv_factor=None):
    if term is None or yield_ is None:
        return None

    rounded_term = round_ytm(term)
    if rounded_term is None:
        return None
    T = int(rounded_term * period)  # Total coupon periods
    C = cpn / period
    Y = yield_ / period

    try:
        price = C * (1 - pow(1 + Y, -T)) / Y + 100 / pow(1 + Y, T)
    except ZeroDivisionError:
        price = None

    if begin and settle and next_coupon:
        ai = AInt(cpn, period=2, begin=begin, settle=settle, next_coupon=next_coupon, day_count=day_count)
        price = price + ai
    return price/conv_factor

def MDur(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    if term is None or yield_ is None:
        return None

    rounded_term = round_ytm(term)
    if rounded_term is None:
        return None

    T = int(rounded_term * period)
    C = cpn / period
    Y = yield_ / period
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if P is None or P == 0:
        return None

    if begin and settle and next_coupon:
        v = accrual_period(begin, settle, next_coupon, day_count)
        P = pow(1 + Y, v) * P
        mdur = (-v * pow(1 + Y, v - 1) * C / Y * (1 - pow(1 + Y, -T))
                + pow(1 + Y, v) * (
                        C / pow(Y, 2) * (1 - pow(1 + Y, -T))
                        - T * C / (Y * pow(1 + Y, T + 1))
                        + (T - v) * 100 / pow(1 + Y, T + 1)))
    else:
        mdur = (C / pow(Y, 2) * (1 - pow(1 + Y, -T))) + (T * (100 - C / Y) / pow(1 + Y, T + 1))
    return mdur / (period * P)

def MacDur(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if mdur is None:
        return None
    return mdur * (1 + yield_ / period)

def DV01(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if P is None or mdur is None:
        return None
    return round((mdur) * P * 0.001, 6)

def fut_DV01(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,conv_factor=None):
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if P is None or mdur is None:
        return None
    return round((mdur) * P * 0.001, 6)/ conv_factor

def DV01minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    yield_ = yield_ - .0001
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if P is None or mdur is None:
        return None
    return round((mdur) * P * 0.001, 6)

def fut_DV01minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, conv_factor=None):
    yield_ = yield_ - .0001
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if P is None or mdur is None:
        return None
    return round((mdur) * P * 0.001, 6)/ conv_factor

def DV01plus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if P is None or mdur is None:
        return None
    return round((mdur + cvx) * P * 0.001, 6)

def DV10(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    yield_ = yield_+.001
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def fut_DV10(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, conv_factor=None):
    yield_ = yield_+.001
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)/conv_factor

def DV10minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    yield_ = yield_-.001
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def fut_DV10minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, conv_factor=None):
    yield_ = yield_-.001
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6) /conv_factor

def DV50(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    yield_ = yield_ + .005
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def fut_DV50(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, conv_factor=None):
    yield_ = yield_ + .005
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)/ conv_factor

def DV50minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    yield_ = yield_ - .005
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def fut_DV50minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, conv_factor=None):
    yield_ = yield_ - .005
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6) / conv_factor

def DV100(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    yield_ = yield_ + .01
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def fut_DV100(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, conv_factor=None):
    yield_ = yield_ + .01
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)/ conv_factor

def DV100minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    yield_ = yield_ - .01
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def fut_DV100minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, conv_factor=None):
    yield_ = yield_ - .01
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6) / conv_factor

def Cvx(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    if term is None or yield_ is None:
        return None
    rounded_term = round_ytm(term)
    if rounded_term is None:
        return None
    T = int(rounded_term * period)
    C = cpn / period
    Y = yield_ / period
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if P is None or P == 0:
        return None
    v = accrual_period(begin, settle, next_coupon, day_count) if (begin and settle and next_coupon) else 0
    dcv = (
            -v * (v - 1) * pow(1 + Y, v - 2) * C / Y * (1 - pow(1 + Y, -T))
            - 2 * v * pow(1 + Y, v - 1) * (C / pow(Y, 2) * (1 - pow(1 + Y, -T)) - T * C / (Y * pow(1 + Y, T + 1)))
            - pow(1 + Y, v) * (
                    -C / pow(Y, 3) * (1 - pow(1 + Y, -T)) +
                    2 * T * C / (pow(Y, 2) * pow(1 + Y, T + 1)) +
                    T * (T + 1) * C / (Y * pow(1 + Y, T + 2))
            )
            + (T - v) * (T + 1) * 100 / pow(1 + Y, T + 2 - v)
    )
    return dcv / (P * period ** 2)

def fut_Cvx(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,conv_factor=None):
    if term is None or yield_ is None:
        return None
    rounded_term = round_ytm(term)
    if rounded_term is None:
        return None
    T = int(rounded_term * period)
    C = cpn / period
    Y = yield_ / period
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if P is None or P == 0:
        return None
    v = accrual_period(begin, settle, next_coupon, day_count) if (begin and settle and next_coupon) else 0
    dcv = (
            -v * (v - 1) * pow(1 + Y, v - 2) * C / Y * (1 - pow(1 + Y, -T))
            - 2 * v * pow(1 + Y, v - 1) * (C / pow(Y, 2) * (1 - pow(1 + Y, -T)) - T * C / (Y * pow(1 + Y, T + 1)))
            - pow(1 + Y, v) * (
                    -C / pow(Y, 3) * (1 - pow(1 + Y, -T)) +
                    2 * T * C / (pow(Y, 2) * pow(1 + Y, T + 1)) +
                    T * (T + 1) * C / (Y * pow(1 + Y, T + 2))
            )
            + (T - v) * (T + 1) * 100 / pow(1 + Y, T + 2 - v)
    )
    return dcv / (P * period ** 2)/conv_factor

def sensitivity22(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    yield_ = yield_ + .0002
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def sensitivity22minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    yield_ = yield_ - .0002
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def sensitivity55(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    yield_ = yield_ + .0005
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def sensitivity55minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    yield_ = yield_ - .0005
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def fut_sensitivity22(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,conv_factor=None):
    yield_ = yield_ + .0002
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)/conv_factor

def fut_sensitivity22minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,conv_factor=None):
    yield_ = yield_ - .0002
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)/conv_factor

def fut_sensitivity55(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,conv_factor=None):
    yield_ = yield_ + .0005
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)/conv_factor

def fut_sensitivity55minus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, conv_factor=None):
    yield_ = yield_ - .0005
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)/conv_factor

def sensitivityMKT(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, mvol = None):
    yield_ = yield_ + mvol
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def sensitivityMKTminus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, mvol = None):
    yield_ = yield_ - mvol
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)

def fut_sensitivityMKT(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, mvol = None,conv_factor=None):
    yield_ = yield_ + mvol
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)/conv_factor

def fut_sensitivityMKTminus(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1, mvol = None,conv_factor=None):
    yield_ = yield_ - mvol
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    #cvx = Cvx(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if yield_ is None or mdur is None:
        return None
    return round((mdur) * 0.001, 6)/conv_factor

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

def approximate_duration(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,delta_y=0.0001):
    if yield_ is None:
        return None
    price = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    p_bp_up = BPrice(cpn, term, (yield_ + delta_y), period, begin, settle, next_coupon, day_count)
    p_bp_dn = BPrice(cpn, term, (yield_ - delta_y), period, begin, settle, next_coupon, day_count)
    if price is None or price == 0:
        return None
    return (p_bp_dn - p_bp_up) / (2 * price * delta_y)

def approximate_convexity(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,delta_y=0.0001):
    if yield_ is None:
        return None
    price = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    p_bp_up = BPrice(cpn, term, (yield_ + delta_y), period, begin, settle, next_coupon, day_count)
    p_bp_dn = BPrice(cpn, term, (yield_ - delta_y), period, begin, settle, next_coupon, day_count)
    if price is None or price == 0:
        return None
    return (p_bp_up + p_bp_dn - 2 * price) / (price * delta_y ** 2)

def calculate_bond_metrics(face_value, market_price, issue_date_str, maturity_date_str, coupon_rate,
                           periods_per_year, day_count, coupon_prev_date_str=None, coupon_next_date_str=None,
                           trade_settle_date_str=None, market_yield=None):

    begin = coupon_prev_date_str if coupon_prev_date_str is not None else issue_date_str
    settle = trade_settle_date_str if trade_settle_date_str is not None else maturity_date_str
    effective_date = trade_settle_date_str if trade_settle_date_str is not None else begin

    time_to_maturity = calculate_term(effective_date, maturity_date_str)
    ytm = calculate_ytm(market_price, face_value, coupon_rate, time_to_maturity, periods_per_year, n_digits=5)
    yield_to_maturity = market_yield if market_yield is not None else ytm
    bond_price = BPrice(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                        settle, coupon_next_date_str, day_count)
    accrued_interest = AInt(coupon_rate, periods_per_year, begin, settle, coupon_next_date_str, day_count)
    modified_duration = MDur(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                             settle, coupon_next_date_str, day_count)
    macaulay_duration = MacDur(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                               settle, coupon_next_date_str, day_count)
    dv01 = DV01(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                settle, coupon_next_date_str, day_count)

    dv01plus =DV01plus(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                settle, coupon_next_date_str, day_count)
    dv10 = DV10(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                settle, coupon_next_date_str, day_count)

    dv50 = DV50(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                settle, coupon_next_date_str, day_count)

    dv100 = DV100(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                settle, coupon_next_date_str, day_count)

    convexity = Cvx(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                    settle, coupon_next_date_str, day_count)
    approx_duration = approximate_duration(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                                           settle, coupon_next_date_str, day_count)
    approx_convexity = approximate_convexity(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                                             settle, coupon_next_date_str, day_count)
    return {
        "time_to_maturity": time_to_maturity,
        "accrued_interest": accrued_interest,
        "yield_to_maturity": ytm,
        "clean_price": bond_price,
        "dirty_price": bond_price,
        "macaulay_duration": macaulay_duration,
        "modified_duration": modified_duration,
        "convexity": convexity,
        "dv01": dv01,
        "dv01plus": dv01plus,
        "dv10": dv10,
        "dv50": dv50,
        "dv100": dv100,
        "approx_duration": approx_duration,
        "approx_convexity": approx_convexity,
    }

def compute_ust_kpis(item):
    """
    Expected keys:
      issue_date, maturity_date, coupon_rate, coupon_prev_date, coupon_ncpdt,
      principal_value, ask_price, bid_price, last_price, and optionally "price".
    """
    issue_date = item['issue_date']
    maturity_date = item['maturity_date']
    coupon_rate = item['coupon_rate']
    coupon_prev_date = item['coupon_prev_date']
    coupon_next_date = item['coupon_ncpdt']
    face_value = item['principal_value']
    ask_price = item['ask_price']
    bid_price = item['bid_price']
    last_price = item['last_price']

    is_valid = (
            pd.notna(ask_price) and ask_price != "" and
            pd.notna(bid_price) and bid_price != "" and
            pd.notna(issue_date) and issue_date != "" and
            pd.notna(maturity_date) and maturity_date != "" and
            pd.notna(coupon_rate) and coupon_rate != "" and
            pd.notna(coupon_prev_date) and coupon_prev_date != "" and
            pd.notna(coupon_next_date) and coupon_next_date != "" and
            pd.notna(face_value) and face_value != ""
    )

    if is_valid:
        coupon_rate = float(coupon_rate)
        issue_date = str(int(issue_date))
        maturity_date = str(int(maturity_date))
        coupon_prev_date = str(int(coupon_prev_date))
        coupon_next_date = str(int(coupon_next_date))
        periods_per_year = 2
        day_count = 1

        if 'price' in item and pd.notna(item['price']) and item['price'] != "":
            current_market_price = float(item['price'])
        else:
            current_market_price = (float(ask_price) + float(bid_price) + float(last_price)) / 3

        trade_date = compute_settlement_date(datetime.today().strftime('%Y%m%d'))

        bond_metrics = calculate_bond_metrics(
            face_value,
            current_market_price,
            issue_date,
            maturity_date,
            coupon_rate,
            periods_per_year,
            day_count,
            coupon_prev_date,
            coupon_next_date,
            trade_date,
            market_yield=None)

        return
    else:
        return None

# ---------------- Main Test Block ----------------
if __name__ == '__main__':

    kpis = calculate_bond_metrics(face_value=100, market_price=101, issue_date_str=str(23-1-1), maturity_date_str=str(36-1-1), coupon_rate=4.5,
                           periods_per_year=2, day_count=1, coupon_prev_date_str=str(22-7-1), coupon_next_date_str=str(23-1-1),
                           trade_settle_date_str=str(25-7-1), market_yield=4.411)
    print("Computed KPIs:")
    print(kpis)
