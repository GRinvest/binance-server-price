import asyncio

from random import randrange
import ujson
from aio_binance.futures.usdt import WsClient

from config import config, redis


class Tasks:

    def __init__(self, symbols: list):
        self.conn = None
        self.symbols = symbols

    async def event_kline(self, data: dict):
        p = data['data']
        await self.conn.zadd(':'.join(['price', p['s']]), {ujson.dumps([
            float(p['p']),
            float(p['q'])
        ]): str(p['T']) + str(randrange(100000, 999999, 1))})

    async def creation(self):
        ws = WsClient(reply_timeout=160)
        print(f"run: {self.symbols}")
        while True:
            streams = []
            async with redis.client() as self.conn:
                for symbol in self.symbols:
                    streams.append(ws.stream_agg_trade(symbol))
                res = await asyncio.gather(*streams)
                await ws.subscription_streams(res, self.event_kline)
