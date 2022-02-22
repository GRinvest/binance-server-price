import asyncio
import pickle

import numpy as np
import pandas as pd
from tapy import Indicators
from multiprocessing import JoinableQueue
from config import redis
from redis.df import df_in_redis


def _converter_kline_to_df(data):
    a = np.array(data)
    df = pd.DataFrame(a.reshape(-1, 12),
                      columns=['open_time',
                               'Open',
                               'High',
                               'Low',
                               'Close',
                               'Volume',
                               'close_time',
                               'quote_asset_volume',
                               'number_of_trades',
                               'taker_buy_base',
                               'taker_buy_quote',
                               'ignore'])
    df = df.astype({
        'open_time': np.int64,
        'Open': float,
        'High': float,
        'Low': float,
        'Close': float,
        'Volume': float,
        'close_time': np.int64,
        'quote_asset_volume': float,
        'number_of_trades': int,
        'taker_buy_base': float,
        'taker_buy_quote': float,
        'ignore': int})
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
    df.drop_duplicates(subset=['close_time'], keep=False, inplace=True)
    df = df.set_index('close_time')
    df = df.sort_index()
    return df


def _add_indicators(df, length=180):
    i = Indicators(df)
    i.fractals('fractals_high', 'fractals_low')
    i.ema(length, 'ema_high', 'High')
    i.ema(length, 'ema_low', 'Low')
    i.atr(5)
    i.df.fillna(0)
    return i.df


async def _save_df(data, alias):
    df = _converter_kline_to_df(data)
    df = _add_indicators(df, 180)
    async with redis.pipeline() as pipe:
        _alias = ':'.join(['df', alias[1], alias[0]])
        await pipe.delete(_alias).execute()
        df_in_redis(pipe, _alias, df)
        await pipe.execute()


async def _create_df(alias):  # [symbol, time_frame]
    klines = {}
    async with redis.client() as conn:
        res = await conn.lrange(':'.join(alias), 0, 200)
    data = []
    for item in res:
        data.append(pickle.loads(item))
    await _save_df(data, alias)


async def run(q: JoinableQueue):
    while True:
        alias = q.get()
        await _create_df(alias)
        q.task_done()
        print(f'update {alias}')

