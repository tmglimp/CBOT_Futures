import logging
import sys
import os
import pandas as pd
import requests
import urllib3
import datetime
from datetime import datetime, timedelta
import config
from config import leg_delta
from leaky_bucket import leaky_bucket

# Configure logging to both file and stdout
logging.basicConfig(
    level=config.LOG_LEVEL,
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

# Ignore insecure error messages
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def solve_tails_hedge(row):
    row["D_ratio"] = row['A_FUT_DV01']/row['B_FUT_DV01']
    print("Delta ratio as", row["D_ratio"])
    row['A_FUT_TL'] = round((
        (float(config.leg_delta) * float(row['B_FUT_MULTIPLIER'])/1000 * abs(row['B_Q_Value']) * float(row['B_FUT_DV01'])
         - float(row['A_FUT_MULTIPLIER'])/1000 * abs(row['A_Q_Value']) * float(row['A_FUT_DV01']))
        / float(row['A_FUT_DV01'])), 3)

    row['B_FUT_TL'] = round((
        (float(row['A_FUT_MULTIPLIER'])/1000 * abs(row['A_Q_Value']) * float(row['A_FUT_DV01']))
        / (float(config.leg_delta) * float(row['B_FUT_DV01']))
        - (float(row['B_FUT_MULTIPLIER'])/1000 * abs(row['B_Q_Value']))), 3)



def compute_risk_metrics(ORDERS):
    print("Starting risk metrics computation...")
    orders_df = config.ORDERS.copy()

    for idx, row in orders_df.iterrows():

        front_multiplier = row['A_FUT_MULTIPLIER']
        back_multiplier = row['B_FUT_MULTIPLIER']
        front_contract_value = row['A_FUT_TPRICE']
        back_contract_value = row['B_FUT_TPRICE']
        front_ratio = row["A_Q_Value"]
        back_ratio = row["B_Q_Value"]
        front_dv01 = row['A_FUT_DV01']
        front_dv01_minus = row['A_FUT_DV01_MINUS']
        back_dv01 = row['B_FUT_DV01']
        back_dv01_minus = row['B_FUT_DV01_MINUS']
        front_dv10 = row['A_FUT_DV10']
        front_dv10_minus = row['A_FUT_DV10_MINUS']
        back_dv10 = row['B_FUT_DV10']
        back_dv10_minus = row['B_FUT_DV10_MINUS']
        front_dv50 = row['A_FUT_DV50']
        front_dv50_minus = row['A_FUT_DV50_MINUS']
        back_dv50 = row['B_FUT_DV50']
        back_dv50_minus = row['B_FUT_DV50_MINUS']
        front_dv100 = row['A_FUT_DV100']
        front_dv100_minus = row['A_FUT_DV100_MINUS']
        back_dv100 = row['B_FUT_DV100']
        back_dv100_minus = row['B_FUT_DV100_MINUS']
        front_dv22 = row['A_FUT_DV22']
        front_dv22_minus = row['A_FUT_DV22_MINUS']
        back_dv22 = row['B_FUT_DV22']
        back_dv22_minus = row['B_FUT_DV22_MINUS']
        front_dv55 = row['A_FUT_DV55']
        front_dv55_minus = row['A_FUT_DV55_MINUS']
        back_dv55 = row['B_FUT_DV55']
        back_dv55_minus = row['B_FUT_DV55_MINUS']
       # front_DVMKT = row['A_FUT_DVMKT']
       # front_DVMKT_minus = row['A_FUT_DVMKT_MINUS']
       # back_DVMKT = row['B_FUT_DVMKT']
       # back_dVMKT_minus = row['B_FUT_DVMKT_MINUS']
        gross_pos_impl_notional = (front_multiplier * front_contract_value * abs(front_ratio) + back_multiplier * back_contract_value * abs(back_ratio))

        # DERIVE DELTA65 TL RATIOS W/ SOLVER
        orders_df.at[idx, 'B_FUT_TL'], orders_df.at[idx, 'A_FUT_TL'] = solve_tails_hedge(row)
        front_tl = row["A_FUT_TL"]
        back_tl = row["B_FUT_TL"]
        front_basis = int(row["A_NetBasis"])
        back_basis  = int(row["B_NetBasis"])
        if front_basis < back_basis:
            tail = back_tl
            tail_ratio = back_ratio
            tail_multiplier = back_multiplier
            tail_dv01 = back_dv01
            tail_dv01_minus = back_dv01_minus
        else:
            tail = front_tl
            tail_ratio = front_ratio
            tail_multiplier = front_multiplier
            tail_dv01 = front_dv01
            tail_dv01_minus = front_dv01_minus

        ## DOLLAR OVERLAY FOR 2BP/2BP CHANGE (WHAT HAPPENS IF YIELDS CHANGE 2BP/2BP?)
        SEN22 = round((front_dv22 * front_contract_value * front_ratio * front_multiplier + back_dv22 * back_contract_value * back_ratio * back_multiplier), 7)
        SEN2M2 = round((front_dv22_minus * front_contract_value* front_ratio * front_multiplier + back_dv22 *back_contract_value * back_ratio * back_multiplier), 7)
        SEN22M = round((front_dv22 * front_contract_value * front_ratio * front_multiplier + back_dv22_minus * back_contract_value * back_ratio * back_multiplier),7)
        SEN2M2M = round((front_dv22_minus * front_contract_value* front_ratio * front_multiplier + back_dv22_minus * back_contract_value *back_ratio * back_multiplier),7)

        ## DOLLAR OVERLAY FOR 5BP/5BP CHANGE (WHAT HAPPENS IF YIELDS CHANGE 5BP/5BP?)
        SEN55 = round((front_dv55 * front_contract_value* front_ratio * front_multiplier + back_dv55 * back_contract_value *back_ratio * back_multiplier), 7)
        SEN5M5 = round((front_dv55_minus * front_contract_value * front_ratio * front_multiplier + back_dv55 * back_contract_value *back_ratio * back_multiplier), 7)
        SEN55M = round((front_dv55 * front_contract_value * front_ratio * front_multiplier + back_dv55_minus * back_contract_value *back_ratio * back_multiplier),7)
        SEN5M5M = round((front_dv55_minus * front_contract_value * front_ratio * front_multiplier + back_dv55_minus * back_contract_value *back_ratio * back_multiplier),7)

        ## DOLLAR OVERLAY FOR 5BP/2BP CHANGE (WHAT HAPPENS IF YIELDS CHANGE 5BP/2BP?)
        SEN52 = round((front_dv55 * front_contract_value * front_ratio * front_multiplier + back_dv22 * back_contract_value *back_ratio * back_multiplier), 7)
        SEN5M2 = round((front_dv55_minus * front_contract_value* front_ratio * front_multiplier + back_dv22 * back_contract_value *back_ratio * back_multiplier), 7)
        SEN52M = round((front_dv55 * front_contract_value *front_ratio * front_multiplier + back_dv22_minus * back_contract_value *back_ratio * back_multiplier), 7)
        SEN5M2M = round((front_dv55_minus * front_contract_value *front_ratio * front_multiplier + back_dv22_minus * back_contract_value *back_ratio * back_multiplier), 7)

        ## DOLLAR OVERLAY FOR 2BP/5BP CHANGE (WHAT HAPPENS IF YIELDS CHANGE 2BP/5BP?)
        SEN25 = round((front_dv22 * front_contract_value * front_ratio * front_multiplier + back_dv55 * back_contract_value *back_ratio * back_multiplier), 7)
        SEN2M5 = round((front_dv22_minus * front_contract_value * front_ratio * front_multiplier + back_dv55 * back_contract_value *back_ratio * back_multiplier), 7)
        SEN25M = round((front_dv22 * front_contract_value * front_ratio * front_multiplier + back_dv55_minus * back_contract_value *back_ratio * back_multiplier), 7)
        SEN2M5M = round((front_dv22_minus * front_contract_value * front_ratio * front_multiplier + back_dv55_minus *back_contract_value * back_ratio * back_multiplier), 7)

        ## DOLLAR OVERLAY FOR mktBP/mktBP CHANGE (WHAT HAPPENS IF YIELDS CHANGE mktBP/mktBP?)
      #  SENMKTMKT = round((front_DVMKT * front_ratio * front_multiplier + back_DVMKT * back_ratio * back_multiplier), 7)
      #  SENMKT_M_MKT = round((front_DVMKT_minus * front_ratio * front_multiplier + back_DVMKT * back_ratio * back_multiplier), 7)
      #  SENMKT_MKT_M = round((front_DVMKT * front_ratio * front_multiplier + back_dVMKT_minus * back_ratio * back_multiplier),7)
      #  SENMKT_M_MKT_M = round((front_DVMKT_minus * front_ratio * front_multiplier + back_dVMKT_minus * back_ratio * back_multiplier), 7)

     #   orders_df.at[idx, 'SENS_MKT/MKT'] = SENMKTMKT
     #   orders_df.at[idx, 'SENS_-MKT/MKT'] = SENMKT_M_MKT
     #   orders_df.at[idx, 'SENS_MKT/-MKT'] = SENMKT_MKT_M
     #   orders_df.at[idx, 'SENS_-MKT/-MKT'] = SENMKT_M_MKT_M

        orders_df.at[idx, 'SENS_2BP/2BP'] = SEN22
        orders_df.at[idx, 'SENS_-2BP/2BP'] = SEN2M2
        orders_df.at[idx, 'SENS_2BP/-2BP'] = SEN22M
        orders_df.at[idx, 'SENS_-2BP/-2BP'] = SEN2M2M
        orders_df.at[idx, 'SENS_5BP/5BP'] = SEN55
        orders_df.at[idx, 'SENS_-5BP/5BP'] = SEN5M5
        orders_df.at[idx, 'SENS_5BP/-5BP'] = SEN55M
        orders_df.at[idx, 'SENS_-5BP/-5BP'] = SEN5M5M
        orders_df.at[idx, 'SENS_5BP/2BP'] = SEN52
        orders_df.at[idx, 'SENS_-5BP/2BP'] = SEN5M2
        orders_df.at[idx, 'SENS_5BP/-2BP'] = SEN52M
        orders_df.at[idx, 'SENS_-5BP/-2BP'] = SEN5M2M
        orders_df.at[idx, 'SENS_2BP/5BP'] = SEN25
        orders_df.at[idx, 'SENS_-2BP/5BP'] = SEN2M5
        orders_df.at[idx, 'SENS_2BP/-5BP'] = SEN25M
        orders_df.at[idx, 'SENS_-2BP/-5BP'] = SEN2M5M

        orders_df.at[idx, 'SENS_2BP/2BP_NET']       = orders_df.at[idx, 'SENS_2BP/2BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_-2BP/2BP_NET']      = orders_df.at[idx, 'SENS_-2BP/2BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_2BP/-2BP_NET']      = orders_df.at[idx, 'SENS_2BP/-2BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_-2BP/-2BP_NET']     = orders_df.at[idx, 'SENS_-2BP/-2BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_5BP/5BP_NET']       = orders_df.at[idx, 'SENS_5BP/5BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_-5BP/5BP_NET']      = orders_df.at[idx, 'SENS_-5BP/5BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_5BP/-5BP_NET']      = orders_df.at[idx, 'SENS_5BP/-5BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_-5BP/-5BP_NET']     = orders_df.at[idx, 'SENS_-5BP/-5BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_5BP/2BP_NET']       = orders_df.at[idx, 'SENS_5BP/2BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_-5BP/2BP_NET']      = orders_df.at[idx, 'SENS_-5BP/2BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_5BP/-2BP_NET']      = orders_df.at[idx, 'SENS_5BP/-2BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_-5BP/-2BP_NET']     = orders_df.at[idx, 'SENS_-5BP/-2BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_2BP/5BP_NET']       = orders_df.at[idx, 'SENS_2BP/5BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_-2BP/5BP_NET']      = orders_df.at[idx, 'SENS_-2BP/5BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_2BP/-5BP_NET']      = orders_df.at[idx, 'SENS_2BP/-5BP'] + orders_df.at[idx, 'PositionNetBasis']
        orders_df.at[idx, 'SENS_-2BP/-5BP_NET']     = orders_df.at[idx, 'SENS_-2BP/-5BP'] + orders_df.at[idx, 'PositionNetBasis']
    #    orders_df.at[idx, 'SENS_MKT/MKT_NET']       = orders_df.at[idx, 'SENS_MKT/MKT'] + orders_df.at[idx, '_PositionNetBasis']
    #    orders_df.at[idx, 'SENS_-MKT/MKT_NET']      = orders_df.at[idx, 'SENS_-MKT/MKT'] + orders_df.at[idx, '_PositionNetBasis']
    #    orders_df.at[idx, 'SENS_MKT/-MKT_NET']      = orders_df.at[idx, 'SENS_MKT/-MKT'] + orders_df.at[idx, '_PositionNetBasis']
    #    orders_df.at[idx, 'SENS_-MKT/-MKT_NET']     = orders_df.at[idx, 'SENS_-MKT/-MKT'] + orders_df.at[idx, '_PositionNetBasis']

        overlayA = round((front_dv01 * front_ratio * front_multiplier + back_dv01 * back_ratio * back_multiplier), 7)
        overlayA_minus = round((front_dv01_minus * front_ratio * front_multiplier + back_dv01_minus * back_ratio * back_multiplier), 7)
        #overlayA_ratio = (round((front_dv01_minus * front_ratio * front_multiplier),7) / round((back_dv01_minus * back_ratio * back_multiplier), 7))
        #overlayA_ratio_minus = (round((front_dv01_minus * front_ratio * front_multiplier), 7) / round((back_dv01_minus * back_ratio * back_multiplier), 7))
        overlayB = round((front_dv10 * front_contract_value * front_ratio * front_multiplier + back_dv10 * back_contract_value * back_ratio * back_multiplier), 7)
        overlayB_minus = round((front_dv10_minus * front_contract_value *front_ratio * front_multiplier + back_dv10_minus * back_contract_value *back_ratio * back_multiplier), 7)
        #overlayB_ratio = (round((front_dv10 * front_ratio * front_multiplier), 7) / round((back_dv10 * back_ratio * back_multiplier), 7))
        #overlayB_ratio_minus = (round((front_dv10_minus * front_ratio * front_multiplier), 7) / round((back_dv10_minus * back_ratio * back_multiplier), 7))
        overlayC = round((front_dv50 * front_contract_value *front_ratio * front_multiplier + back_dv50 * back_contract_value *back_ratio * back_multiplier), 7)
        overlayC_minus = round((front_dv50_minus * front_contract_value *front_ratio * front_multiplier + back_dv50_minus * back_contract_value *back_ratio * back_multiplier), 7)
        #overlayC_ratio = (round((front_dv100 * front_ratio * front_multiplier), 7) / round((back_dv100 * back_ratio * back_multiplier), 7))
        #overlayC_ratio_minus = (round((front_dv100_minus * front_ratio * front_multiplier), 7) / round((back_dv100_minus * back_ratio * back_multiplier), 7))
        overlayD = round((front_dv100 * front_contract_value *front_ratio * front_multiplier + back_dv100 * back_contract_value *back_ratio * back_multiplier), 7)
        overlayD_minus = round((front_dv100_minus * front_contract_value *front_ratio * front_multiplier + back_dv100_minus * back_contract_value *back_ratio * back_multiplier), 7)
        #overlayD_ratio = (round((front_dv100 * front_ratio * front_multiplier), 7) / round((back_dv100 * back_ratio * back_multiplier), 7))
        #overlayD_ratio_minus = (round((front_dv100_minus * front_ratio * front_multiplier), 7) / round((back_dv100_minus * back_ratio * back_multiplier), 7))
        overlayE = round(((front_dv01 * front_ratio * front_multiplier + back_dv01 * back_ratio * back_multiplier) + (tail*tail_ratio*tail_dv01*tail_multiplier)), 7)
        overlayE_minus = round(((front_dv01 * front_ratio * front_multiplier + back_dv01 * back_ratio * back_multiplier) + (tail*tail_ratio*tail_dv01_minus*tail_multiplier)), 7)
        overlayE = round(((front_dv01 * front_ratio * front_multiplier + back_dv01 * back_ratio * back_multiplier) + (
                    tail * tail_ratio * tail_dv01 * tail_multiplier)), 7)
        overlayE_minus = round(((front_dv01 * front_ratio * front_multiplier + back_dv01 * back_ratio * back_multiplier) + (
                                            tail * tail_ratio * tail_dv01_minus * tail_multiplier)), 7)


        orders_df.at[idx, 'NET_DURATION_OVERLAY_1BP'] = overlayA
        orders_df.at[idx, 'NET_DURATION_OVERLAY_1BP_MINUS'] = overlayA_minus
        orders_df.at[idx, 'EQUITY_DELTA_1BP'] = round((overlayA / gross_pos_impl_notional), 7)
        orders_df.at[idx, 'EQUITY_DELTA_1BP_MINUS'] = round((overlayA_minus / gross_pos_impl_notional), 7)
        orders_df.at[idx, 'NET_DURATION_OVERLAY_10BP'] = overlayB
        orders_df.at[idx, 'NET_DURATION_OVERLAY_10BP_MINUS'] = overlayB_minus
        orders_df.at[idx, 'EQUITY_DELTA_10BP'] = round((overlayB / gross_pos_impl_notional), 7)
        orders_df.at[idx, 'EQUITY_DELTA_10BP_MINUS'] = round((overlayB_minus / gross_pos_impl_notional), 7)
        orders_df.at[idx, 'NET_DURATION_OVERLAY_50BP'] = overlayC
        orders_df.at[idx, 'NET_DURATION_OVERLAY_50BP_MINUS'] = overlayC_minus
        orders_df.at[idx, 'EQUITY_DELTA_50BP'] = round((overlayC / gross_pos_impl_notional), 7)
        orders_df.at[idx, 'EQUITY_DELTA_50BP_MINUS'] = round((overlayC_minus / gross_pos_impl_notional), 7)
        orders_df.at[idx, 'NET_DURATION_OVERLAY_100BP'] = overlayD
        orders_df.at[idx, 'NET_DURATION_OVERLAY_100BP_MINUS'] = overlayD_minus
        orders_df.at[idx, 'EQUITY_DELTA_100BP'] = round((overlayD / gross_pos_impl_notional), 7)
        orders_df.at[idx, 'EQUITY_DELTA_100BP_MINUS'] = round((overlayD_minus / gross_pos_impl_notional), 7)
        orders_df.at[idx, 'NET_DURATION_OVERLAY_TAIL'] = overlayE
        orders_df.at[idx, 'NET_DURATION_OVERLAY_TAIL_MINUS'] = overlayE_minus


    ### REJECT SENSITIVITY BASIS > $20  ###
    net_cols = [c for c in orders_df.columns if c.startswith('SENS_') and c.endswith('BP')]
    selected_df = pd.DataFrame()

    if net_cols and not orders_df.empty:
        max_check = min(10, len(orders_df))
        for pos in range(max_check):
            row = orders_df.iloc[pos]
            net_vals = pd.to_numeric(row[net_cols], errors='coerce')
            breach = (abs(net_vals) > 20).fillna(False).any()  # scalar bool
            if not breach:
                selected_df = orders_df.iloc[[pos]].copy()
                logging.info(f"Row [{pos}] passed NET sensitivity checks.")
                break
        else:
            logging.info("No rows in [0]..[9] passed NET sensitivity checks.")
    else:
        logging.info("No qualifying NET sensitivity columns found.")

    basis_overlay_cols = [c for c in orders_df.columns if c.startswith('NET_DURATION') and (c.endswith('BP') or c.endswith('BP_MINUS'))]
    keep_df = pd.DataFrame()

    if not selected_df.empty:
        if basis_overlay_cols:
            # Coerce to numeric on the selected row only
            vals = pd.to_numeric(selected_df.loc[:, basis_overlay_cols].iloc[0], errors='coerce')
            reject = ((vals > 10) | vals.isna()).any()  # scalar bool
            if reject:
                bad_cols = list(vals.index[(vals > 10) | vals.isna()])
                selected_df = selected_df.drop(columns=bad_cols, errors='ignore')
                logging.info(f"Selected row rejected by basis overlay tests: {bad_cols}.")
            else:
                keep_df = selected_df.copy()
                logging.info("Selected row passed basis overlay test.")
        else:
            # No overlay columns to test -> treat as pass
            keep_df = selected_df.copy()
            logging.info("No NET_DURATION overlay columns found; passing by default.")
    else:
        logging.info("No candidate row passed basis overlay test.")

    equity_delta_cols = [c for c in orders_df.columns if c.startswith("EQUITY_") and (c.endswith('BP') or c.endswith("BP_MINUS"))]
    equity_df = pd.DataFrame()

    if not keep_df.empty:
        if equity_delta_cols:
            vals = pd.to_numeric(keep_df.loc[:, equity_delta_cols].iloc[0], errors='coerce')
            reject = ((vals > .01) | vals.isna()).any()  # scalar bool
            if reject:
                bad_cols = list(vals.index[(vals > .01) | vals.isna()])
                keep_df = keep_df.drop(columns=bad_cols, errors='ignore')
                logging.info(f"Selected row rejected by equity-delta test: {bad_cols}.")
            else:
                equity_df = keep_df.copy()
                logging.info("Selected row passed equity-delta test.")
        else:
            equity_df = keep_df.copy()
            logging.info("No EQUITY_* delta columns found; passing by default.")
    else:
        logging.info("No candidate row passed equity-delta test.")

    print("Verified stable hedge selected:", equity_df)
    config.updated_ORDERS = equity_df
    return config.updated_ORDERS

if __name__ == "__main__":
    compute_risk_metrics(ORDERS)