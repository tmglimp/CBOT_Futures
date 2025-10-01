"""
CTD and FUT KPIs
"""

import itertools
import pandas as pd
from config import HEDGES
from fixed_income_calc import BPrice,MDur,MacDur,Cvx,DV01,fut_DV01,fut_DV10,fut_DV50,fut_DV100, \
    fut_DV01minus,fut_DV10minus,fut_DV50minus,fut_DV100minus,fut_sensitivity22,fut_sensitivity22minus, \
    fut_sensitivity55,fut_sensitivity55minus,TPrice,fut_Cvx

def display_hedges_info():
    print("Displaying first 5 rows of HEDGES dataframe:")
    print(HEDGES.head())

def run_fixed_income_calculation(HEDGES):
    HEDGES.columns = HEDGES.columns.str.upper()

    period = 2
    day_count = 1

    HEDGES['CTD_BPRICE'] = HEDGES.apply(lambda row: BPrice(cpn=row['CTD_COUPON'],
                           term=row['CTD_YTM'],
                           yield_=row['CTD_YIELD'],
                           period=period,
                           begin=row['CTD_PREV_COUPON'],
                           next_coupon=row['CTD_NEXT_COUPON'],
                           day_count=day_count), axis=1)

    HEDGES['FUT_TPRICE'] = HEDGES.apply(lambda row: TPrice(cpn=row['CTD_COUPON'],
                                                           term=row['CTD_YTM'],
                                                           yield_=row['CTD_YIELD'],
                                                           period=period,
                                                           begin=row['CTD_PREV_COUPON'],
                                                           next_coupon=row['CTD_NEXT_COUPON'],
                                                           day_count=day_count,
                                                           conv_factor=row["CTD_CF"]), axis=1)

    HEDGES['CTD_MDUR'] = HEDGES.apply(lambda row: MDur(cpn=row['CTD_COUPON'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_YIELD'],
                         period=period,
                         begin=row['CTD_PREV_COUPON'],
                         next_coupon=row['CTD_NEXT_COUPON'],
                         day_count=day_count), axis=1)

    HEDGES['CTD_MACDUR'] = HEDGES.apply(lambda row: MacDur(cpn=row['CTD_COUPON'],
                           term=row['CTD_YTM'],
                           yield_=row['CTD_YIELD'],
                           period=period,
                           begin=row['CTD_PREV_COUPON'],
                           next_coupon=row['CTD_NEXT_COUPON'],
                           day_count=day_count), axis=1)

    HEDGES['CTD_CVX'] = HEDGES.apply(lambda row: Cvx(cpn=row['CTD_COUPON'],
                                                     term=row['CTD_YTM'],
                                                     yield_=row['CTD_YIELD'],
                                                     period=period,
                                                     begin=row['CTD_PREV_COUPON'],
                                                     next_coupon=row['CTD_NEXT_COUPON'],
                                                     day_count=day_count,), axis=1)

    HEDGES['FUT_CVX'] = HEDGES.apply(lambda row: fut_Cvx(cpn=row['CTD_COUPON'],
                                                     term=row['CTD_YTM'],
                                                     yield_=row['CTD_YIELD'],
                                                     period=period,
                                                     begin=row['CTD_PREV_COUPON'],
                                                     next_coupon=row['CTD_NEXT_COUPON'],
                                                     day_count=day_count,
                                                     conv_factor=row["CTD_CF"]), axis=1)

    HEDGES['CTD_DV01'] = HEDGES.apply(lambda row: DV01(cpn=row['CTD_COUPON'],
                                                           term=row['CTD_YTM'],
                                                           yield_=row['CTD_YIELD'],
                                                           period=period,
                                                           begin=row['CTD_PREV_COUPON'],
                                                           next_coupon=row['CTD_NEXT_COUPON'],
                                                           day_count=day_count,
                                                           conv_factor=row["CTD_CF"]), axis=1)

    HEDGES['FUT_DV01'] = HEDGES.apply(lambda row: fut_DV01(cpn=row['CTD_COUPON'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_YIELD'],
                         period=period,
                         begin=row['CTD_PREV_COUPON'],
                         next_coupon=row['CTD_NEXT_COUPON'],
                         day_count=day_count,
                         conv_factor=row["CTD_CF"]), axis=1)

    HEDGES['FUT_DV01_MINUS'] = HEDGES.apply(lambda row: fut_DV01minus(cpn=row['CTD_COUPON'],
                                                       term=row['CTD_YTM'],
                                                       yield_=row['CTD_YIELD'],
                                                       period=period,
                                                       begin=row['CTD_PREV_COUPON'],
                                                       next_coupon=row['CTD_NEXT_COUPON'],
                                                       day_count=day_count,
                                                       conv_factor=row["CTD_CF"]), axis=1)


    HEDGES['FUT_DV10'] = HEDGES.apply(lambda row: fut_DV10(cpn=row['CTD_COUPON'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_YIELD'],
                         period=period,
                         begin=row['CTD_PREV_COUPON'],
                         next_coupon=row['CTD_NEXT_COUPON'],
                         day_count=day_count,
                         conv_factor=row["CTD_CF"]), axis=1)

    HEDGES['FUT_DV10_MINUS'] = HEDGES.apply(lambda row: fut_DV10minus(cpn=row['CTD_COUPON'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_YIELD'],
                         period=period,
                         begin=row['CTD_PREV_COUPON'],
                         next_coupon=row['CTD_NEXT_COUPON'],
                         day_count=day_count,
                         conv_factor=row["CTD_CF"]), axis=1)

    HEDGES['FUT_DV50'] = HEDGES.apply(lambda row: fut_DV50(cpn=row['CTD_COUPON'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_YIELD'],
                         period=period,
                         begin=row['CTD_PREV_COUPON'],
                         next_coupon=row['CTD_NEXT_COUPON'],
                         day_count=day_count,
                         conv_factor=row["CTD_CF"]), axis=1)

    HEDGES['FUT_DV50_MINUS'] = HEDGES.apply(lambda row: fut_DV50minus(cpn=row['CTD_COUPON'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_YIELD'],
                         period=period,
                         begin=row['CTD_PREV_COUPON'],
                         next_coupon=row['CTD_NEXT_COUPON'],
                         day_count=day_count,
                         conv_factor=row["CTD_CF"]), axis=1)

    HEDGES['FUT_DV100'] = HEDGES.apply(lambda row: fut_DV100(cpn=row['CTD_COUPON'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_YIELD'],
                         period=period,
                         begin=row['CTD_PREV_COUPON'],
                         next_coupon=row['CTD_NEXT_COUPON'],
                         day_count=day_count,
                         conv_factor=row["CTD_CF"]), axis=1)

    HEDGES['FUT_DV100_MINUS'] = HEDGES.apply(lambda row: fut_DV100minus(cpn=row['CTD_COUPON'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_YIELD'],
                         period=period,
                         begin=row['CTD_PREV_COUPON'],
                         next_coupon=row['CTD_NEXT_COUPON'],
                         day_count=day_count,
                         conv_factor=row["CTD_CF"]), axis=1)

    HEDGES['FUT_DV22'] = HEDGES.apply(lambda row: fut_sensitivity22(cpn=row['CTD_COUPON'],
                          term=row['CTD_YTM'],
                          yield_=row['CTD_YIELD'],
                          period=period,
                          begin=row['CTD_PREV_COUPON'],
                          next_coupon=row['CTD_NEXT_COUPON'],
                          day_count=day_count,
                          conv_factor=row["CTD_CF"]), axis=1)

    HEDGES['FUT_DV22_MINUS'] = HEDGES.apply(lambda row: fut_sensitivity22minus(cpn=row['CTD_COUPON'],
                          term=row['CTD_YTM'],
                          yield_=row['CTD_YIELD'],
                          period=period,
                          begin=row['CTD_PREV_COUPON'],
                          next_coupon=row['CTD_NEXT_COUPON'],
                          day_count=day_count,
                          conv_factor=row["CTD_CF"]), axis=1)

    # Generate all combinations of distinct HEDGES rows (based on CTD_CONID).
    combinations = [(row1, row2) for row1, row2 in itertools.product(HEDGES.iterrows(), repeat=2)
                    if row1[1]['CTD_CONID'] != row2[1]['CTD_CONID']]

    combos_data = []
    for combo in combinations:
        row1, row2 = combo
        # Prefix the headers of row1 with 'A_' and row2 with 'B_'
        row1_data = {f'A_{key}': value for key, value in row1[1].to_dict().items()}
        row2_data = {f'B_{key}': value for key, value in row2[1].to_dict().items()}
        combined_row = {**row1_data, **row2_data}
        combos_data.append(combined_row)

    ## dollar roll SP and CBOT front tenor combinations (back mo deferred by 31-96 days).
    HEDGES_Combos = pd.DataFrame(combos_data)
    # Keep under 96 days defer (B later than A)
    a = pd.to_numeric(HEDGES_Combos['A_FUT_YEAR_TO_MATURITY'], errors='coerce')
    b = pd.to_numeric(HEDGES_Combos['B_FUT_YEAR_TO_MATURITY'], errors='coerce')

    HEDGES_Combos = HEDGES_Combos.loc[a.notna() & b.notna()].copy()
    a = a.loc[HEDGES_Combos.index]
    b = b.loc[HEDGES_Combos.index]

    max_days = 96
    days_in_year = 360  # keep Act/360 to be consistent with your prior code
    max_years = max_days / days_in_year

    diff = b - a
    mask = (diff >= 0) & (diff < max_years)
    HEDGES_Combos = HEDGES_Combos.loc[mask].copy()
    return HEDGES_Combos

if __name__ == "__main__":
    display_hedges_info()
    combos = run_fixed_income_calculation(HEDGES)
    print("Fixed income calculation completed. CTD-FUT combinations shape:", combos.shape)