import pandas as pd
from .depns import hlc3


def load(df: pd.DataFrame, index: str | int = 'all') -> pd.Series | pd.DataFrame:
    """Indicator: Volume Weighted Average Price (VWAP)"""

    typical_price = hlc3(high=df.High, low=df.Low, close=df.Close)

    # Calculate Result
    wp = typical_price * df.Volume
    temp = wp.groupby(wp.index.to_period('D')).cumsum()
    temp /= df.Volume.groupby(df.Volume.index.to_period('D')).cumsum()

    # Name
    temp.name = "VWAP_D"
    if isinstance(index, str):
        result = temp
    else:
        result = temp.iloc[index]
    return result
