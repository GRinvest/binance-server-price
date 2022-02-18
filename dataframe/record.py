import asyncio
import pickle

import numpy as np
import pandas as pd
from tapy import Indicators

from config import redis
from redis.df import df_in_redis


def _converter_kline_to_df(data):
    temp = {}
    for key, value in data.items():
        a = np.array(value)
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
        pd.set_option('display.max_columns', None)
        temp[key] = df
    return temp


def _add_indicators(data, length=180):
    _data = {}
    for key, value in data.items():
        i = Indicators(value)
        i.fractals('fractals_high', 'fractals_low')
        i.ema(length, 'ema_high', 'High')
        i.ema(length, 'ema_low', 'Low')
        i.atr(30)
        data[key] = i.df


async def _save_df(data, tf):
    async with redis.pipeline() as pipe:
        temp = _converter_kline_to_df(data)
        _add_indicators(temp, 180)
        for k, v in temp.items():
            alias = ':'.join(['df', tf, k])
            await pipe.delete(alias).execute()
            df_in_redis(pipe, alias, v)
        await pipe.execute()


async def _create_df(time_frame: str, symbols):
    klines = {}
    async with redis.client() as conn:
        for symbol in symbols:
            _s = symbol.decode("utf-8")
            res = await conn.lrange(':'.join([_s, time_frame]), 0, 200)
            data = []
            for item in res:
                data.append(pickle.loads(item))
            klines[_s] = data
    await _save_df(klines, time_frame)


async def run(time_frame):
    async with redis.client() as conn:
        symbols = await conn.lrange('symbols', 0, -1)
    while True:
        await _create_df(time_frame, symbols)
        print(f'update create_df {time_frame}')
        await asyncio.sleep(60)
