import asyncio
import zlib

import ujson
from aio_binance.futures.usdt import ApiSession, Client

from config import config, redis


class AddKlines:

    def __init__(self, pipe, api: Client):
        self.pipe = pipe
        self.api = api

    async def new(self,
                  symbol,
                  time_frame,
                  sem: asyncio.Semaphore) -> None:
        limit = 1500 if time_frame == '1m' else 500
        async with sem:
            res = await self.api.get_public_continuous_klines(
                symbol,
                'PERPETUAL',
                time_frame,
                limit=limit)
        for i in res['data']:
            alias = ':'.join(['klines', symbol, time_frame])
            score = i[0]
            self.pipe.zremrangebyscore(alias, score, score).zadd(alias, {ujson.dumps([
                float(i[1]),
                float(i[2]),
                float(i[3]),
                float(i[4]),
                float(i[5])
            ]): score})


async def run(symbols: list):
    time_frame = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d']
    sem = asyncio.Semaphore(3)
    async with redis.pipeline() as pipe:
        if config.price.flush_db:
            await pipe.flushall().execute()
        async with ApiSession() as session:
            instance = AddKlines(pipe, session)
            for tf in time_frame:
                for symbol in symbols:
                    pipe.lpush('symbols', symbol)
                    await instance.new(symbol, tf, sem)
                    await pipe.execute()
