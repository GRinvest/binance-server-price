import asyncio
import pickle

import ujson
from aio_binance.futures.usdt import WsClient

from config import redis


class Tasks:

    def __init__(self, symbols: list):
        self.pipe = None
        self.symbols = symbols

    async def event_kline(self, data: dict):
        k = data['data']['k']
        alias = ':'.join(['klines', data['data']['ps']])
        score = k['t']
        self.pipe.zremrangebyscore(alias, score, score).zadd(alias, {pickle.dumps([
            float(k['o']),
            float(k['h']),
            float(k['l']),
            float(k['c']),
            float(k['v']),
            k['T']
        ]): score})
        await self.pipe.execute()

    async def creation(self):
        ws = WsClient(reply_timeout=160)
        print(f"run: {self.symbols}")
        while True:
            streams = []
            async with redis.pipeline() as self.pipe:
                for symbol in self.symbols:
                    streams.append(ws.stream_continuous_kline(symbol, 'perpetual', '1m'))
                res = await asyncio.gather(*streams)
                await ws.subscription_streams(res, self.event_kline)
