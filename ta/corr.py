import pandas as pd


def load(df_main: pd.DataFrame, df_slave: pd.DataFrame, index: str | int = 'all') -> pd.Series | pd.DataFrame:
    df = pd.concat([df_main.Close.tail(15), df_slave.Close.tail(15)], axis=1, keys=['main_close', 'slave_close'])  # объединение
    df.ffill(axis=0, inplace=True)

    corr = df.corr(method='pearson', min_periods=1)

    if isinstance(index, str):
        result = corr.main_close
    else:
        result = corr.main_close.iloc[index]
    return result
