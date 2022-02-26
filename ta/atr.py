import pandas as pd
from .depns import true_range


def load(df: pd.DataFrame, length=5, index: str | int = 'all') -> pd.Series | pd.DataFrame:
    tr = true_range(high=df.High, low=df.Low, close=df.Close, drift=1)
    temp = tr.ewm(span=length, adjust=False).mean()
    if isinstance(index, str):
        result = temp
    else:
        result = temp.iloc[index]
    return result

