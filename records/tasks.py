import asyncio
import pickle

import aioredis
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
            _data = []
            for key in self.list_keys:
                _data.append(data['data']['k'][key])
            await self.conn.lpush(':'.join([
                data['data']['s'],
                data['data']['k']['i']]), pickle.dumps(_data))

    async def creation(self):
        self.redis = await aioredis.from_url(REDIS_URL,
                                             encoding='UTF-8',
                                             decode_responses=True)
        while True:
            async with self.redis.client() as conn:
                self.symbols = await conn.lrange('symbols', 0, -1)
            tasks = [
                asyncio.create_task(self.__task_kline()),
                asyncio.create_task(self.__del_kline())
            ]
            res = await asyncio.gather(*tasks, return_exceptions=True)
            print(res)

    async def __task_kline(self):
        streams = []
        ws = WsClient(reply_timeout=600)
        for symbol in self.symbols:
            streams.append(ws.stream_kline(symbol, self.timeframe))
        res = await asyncio.gather(*streams)
        async with self.redis.client() as self.conn:
            await ws.subscription_streams(res, self.event_kline)

    async def __del_kline(self, limit=1000000):
        while True:
            async with self.redis.pipeline(transaction=True) as pipe:
                for symbol in self.symbols:
                    s: str = symbol
                    i: str = self.timeframe
                    pipe.ltrim(':'.join([s, i]), 0, limit)
                await pipe.execute()
            await asyncio.sleep(60 * 60)  # 1 hour


class AddedKlines:

    def __init__(self, session):
        self.session: ApiSession = session
        self.klines = {}

    async def added(self,
                    symbol: str,
                    time_frame: str,
                    time_kline_last: list) -> None:
        res = await self.session.get_public_klines(symbol,
                                                   time_frame,
                                                   start_time=int(time_kline_last[6]),
                                                   limit=100)
        if len(res['data']) > 0:
            self.calculate(res['data'], ':'.join([symbol, time_frame]))

    async def new(self,
                  symbol: str,
                  time_frame: str) -> None:
        res = await self.session.get_public_klines(symbol,
                                                   time_frame,
                                                   limit=1000)
        self.calculate(res['data'], ':'.join([symbol, time_frame]))

    def calculate(self,
                  data: list,
                  key: str) -> None:
        temp = []
        for item in data:
            temp.append(pickle.dumps(item))
        temp.reverse()
        self.klines[key] = temp


async def find_symbol():
    redis = await aioredis.from_url(REDIS_URL)
    async with redis.pipeline(transaction=True) as pipe:
        await pipe.delete('symbols').execute()
        res = await Client().get_public_exchange_info()
        for item in res['data']['symbols']:
            if item['contractType'] == 'PERPETUAL' and item['status'] == 'TRADING':
                pipe.lpush('symbols', item['symbol'])
        await pipe.execute()
    await asyncio.sleep(1)
