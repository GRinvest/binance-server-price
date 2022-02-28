import pandas as pd
from ta import atr
from .depns import zero, rma


def load(df: pd.DataFrame, length=14, index='all'):
    """Indicator: ADX"""

    # Calculate Result
    atr_ = atr.load(df, length=length)

    up = df.High - df.High.shift(1)  # high.diff(drift)
    dn = df.Low.shift(1) - df.Low    # low.diff(-drift).shift(drift)

    pos = ((up > dn) & (up > 0)) * up
    neg = ((dn > up) & (dn > 0)) * dn

    pos = pos.apply(zero)
    neg = neg.apply(zero)

    k = 100 / atr_
    dmp = k * rma(pos, length=length)
    dmn = k * rma(neg, length=length)

    dx = 100 * (dmp - dmn).abs() / (dmp + dmn)
    adx_ = rma(dx, length=length)

    # Name and Categorize it
    adx_.name = "adx"
    dmp.name = "dmp"
    dmn.name = "dmn"

    # Prepare DataFrame to return
    data = {adx_.name: adx_, dmp.name: dmp, dmn.name: dmn}
    temp = pd.DataFrame(data)
    temp.name = "adx"

    if isinstance(index, str):
        result = temp
    else:
        result = temp.adx.iloc[index]
    return result
