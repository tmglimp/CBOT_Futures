# CBOT Duration Tails

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
