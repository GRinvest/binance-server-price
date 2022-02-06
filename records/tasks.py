import asyncio

import aioredis
import timeframe as timeframe
from aio_binance.futures.usdt import WsClient, Client, ApiSession

from config import settings

CONFIG = settings.load_config()
REDIS_URL = 'redis://{}:{}/{}'.format(
    CONFIG['database']['host'],
    CONFIG['database']['port'],
    CONFIG['database']['dbname']
)


class Tasks:

    def __init__(self, time_frame='1m'):
        self.timeframe = time_frame
        self.redis = None
        self.conn = None
        self.pipe = None
        self.symbols = []
        self.list_keys = ['t', 'o', 'h', 'l', 'c', 'v', 'T', 'q', 'n', 'V', 'Q', 'B']

    async def event_kline(self, data: dict):
        if data['data']['k']['x']:
            for key in self.list_keys:
                self.pipe.lpush(':'.join([
                    data['data']['s'],
                    data['data']['k']['i'],
                    key]), data['data']['k'][key])
            await self.pipe.execute()

    async def creation(self):
        self.redis = await aioredis.from_url(REDIS_URL,
                                             encoding='UTF-8',
                                             decode_responses=True)

        async with self.redis.client() as conn:
            self.symbols = await conn.lrange('symbols', 0, -1)
        if CONFIG['general']['download_kline']:
            await self.__download_kline()
        tasks = [
            asyncio.create_task(self.__task_kline()),
            asyncio.create_task(self.__del_kline())
        ]
        res = await asyncio.gather(*tasks, return_exceptions=True)
        print(res)

    @staticmethod
    async def find_symbol():
        redis = await aioredis.from_url(REDIS_URL,
                                        encoding='UTF-8',
                                        decode_responses=True)
        async with redis.pipeline(transaction=True) as pipe:
            await pipe.delete('symbols').execute()
            res = await Client().get_public_exchange_info()
            for item in res['data']['symbols']:
                if item['contractType'] == 'PERPETUAL' and item['status'] == 'TRADING':
                    pipe.lpush('symbols', item['symbol'])
            await pipe.execute()
        await asyncio.sleep(1)

    async def __task_kline(self):
        streams = []
        ws = WsClient(reply_timeout=600)
        for symbol in self.symbols:
            streams.append(ws.stream_kline(symbol, self.timeframe))
        res = await asyncio.gather(*streams, return_exceptions=True)
        async with self.redis.pipeline(transaction=True) as self.pipe:
            await ws.subscription_streams(res, self.event_kline)

    async def __del_kline(self, limit=10000):
        while True:
            async with self.redis.pipeline(transaction=True) as pipe:
                for symbol in self.symbols:
                    s: str = symbol
                    i: str = self.timeframe
                    for key in self.list_keys:
                        pipe.ltrim(':'.join([s, i, key]), 0, limit)
                await pipe.execute()
            await asyncio.sleep(60*10)  # 1 hour

    async def __download_kline(self):
        await self.redis.flushdb()
        async with ApiSession() as session:
            async with self.redis.pipeline(transaction=True) as pipe:
                for symbol in self.symbols:
                    res = await session.get_public_klines(symbol, self.timeframe, limit=1000)
                    for item in res['data']:
                        data = {
                            self.list_keys[0]: item[0],
                            self.list_keys[1]: item[1],
                            self.list_keys[2]: item[2],
                            self.list_keys[3]: item[3],
                            self.list_keys[4]: item[4],
                            self.list_keys[5]: item[5],
                            self.list_keys[6]: item[6],
                            self.list_keys[7]: item[7],
                            self.list_keys[8]: item[8],
                            self.list_keys[9]: item[9],
                            self.list_keys[10]: item[10],
                            self.list_keys[11]: item[11]
                        }
                        for key in self.list_keys:
                            pipe.lpush(':'.join([
                                symbol,
                                self.timeframe,
                                key]), data[key])
                        await pipe.execute()
        await self.find_symbol()
