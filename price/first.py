import asyncio

import ujson
from aio_binance.futures.usdt import ApiSession, Client

from config import config, redis


class AddKlines:

    def __init__(self, pipe, api: Client):
        self.pipe = pipe
        self.api = api

    async def new(self, symbol,
                  sem: asyncio.Semaphore) -> None:
        async with sem:
            res = await self.api.get_public_continuous_klines(
                symbol,
                'PERPETUAL',
                '1m',
                limit=1500)
        for i in res['data']:
            alias = ':'.join(['klines', symbol])
            score = i[0]
            await self.pipe.zremrangebyscore(alias, score, score)
            await self.pipe.zadd(alias, {ujson.dumps([
                score,
                float(i[1]),
                float(i[2]),
                float(i[3]),
                float(i[4]),
                float(i[5]),
                i[6]
            ]): score})


async def run(symbols: list):
    srize = 4
    sem = asyncio.Semaphore(10)
    async with redis.client() as conn:
        if config.price.flush_db:
            await conn.flushall()
        tasks = []
        async with ApiSession() as session:
            instance = AddKlines(conn, session)
            for symbol in symbols:
                tasks.append(asyncio.create_task(instance.new(symbol, sem)))
            srize_list = [tasks[i:i + srize] for i in range(0, len(tasks), srize)]
            for item in srize_list:
                await asyncio.gather(*item)
    await asyncio.sleep(2)
