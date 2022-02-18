import asyncio
import pickle

from aio_binance.futures.usdt import WsClient, ApiSession

from config import redis


class Tasks:

    def __init__(self, event_kline, time_frame='1m'):
        self._event_kline = event_kline
        self.timeframe = time_frame
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
            if data['data']['s'] == 'BTCUSDT' and data['data']['k']['i'] == '1m':
                self._event_kline.put(True)

    async def creation(self):
        self._event_kline.put(True)
        async with redis.client() as conn:
            symbols = await conn.lrange('symbols', 0, -1)
        for _s in symbols:
            self.symbols.append(_s.decode("utf-8"))
        while True:
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
        async with redis.client() as self.conn:
            await ws.subscription_streams(res, self.event_kline)

    async def __del_kline(self, limit=500):
        while True:
            async with redis.pipeline(transaction=True) as pipe:
                for symbol in self.symbols:
                    s: str = symbol
                    i: str = self.timeframe
                    pipe.ltrim(':'.join([s, i]), 0, limit)
                await pipe.execute()
            await asyncio.sleep(60 * 15)  # 15 min


class AddedKlines:

    def __init__(self):
        self.klines = {}

    async def new(self,
                  symbol: str,
                  time_frame: str,
                  session: ApiSession,
                  sem: asyncio.Semaphore) -> None:
        async with sem:
            res = await session.get_public_klines(
                symbol,
                time_frame,
                limit=500)
        self.calculate(res['data'], ':'.join([symbol, time_frame]))

    def calculate(self,
                  data: list,
                  key: str) -> None:
        temp = []
        for item in data:
            temp.append(pickle.dumps(item))
        self.klines[key] = temp
