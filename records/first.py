import asyncio

from aio_binance.futures.usdt import ApiSession

from config import redis, CONFIG
from .tasks import AddedKlines


class Klines:

    def __init__(self):
        self.pipe: redis.pipeline = None
        self.session: ApiSession = None

    async def run(self):
        srize = 4
        await redis.flushall()
        sem = asyncio.Semaphore(3)
        async with redis.pipeline(transaction=True) as self.pipe:
            async with ApiSession() as self.session:
                await self.__find_symbol()
            instance = AddedKlines()
            symbols = await self.pipe.lrange('symbols', 0, -1).execute()
            for _tf in CONFIG['general']['timeframe']:
                await asyncio.sleep(5)
                _tasks = []
                async with ApiSession() as self.session:
                    for _s in symbols[0]:
                        symbol = _s.decode("utf-8")
                        _tasks.append(asyncio.create_task(instance.new(symbol, _tf, self.session, sem)))
                    srize_list = [_tasks[i:i + srize] for i in range(0, len(_tasks), srize)]
                    for item in srize_list:
                        await asyncio.gather(*item)

            for key, value in instance.klines.items():
                print(f"{key} {len(value)}")
                self.pipe.lpush(key, *value)
            await self.pipe.execute()
        await redis.close()
        await asyncio.sleep(1)

    async def __find_symbol(self):
        await self.pipe.delete('symbols').execute()
        res = await self.session.get_public_exchange_info()
        for item in res['data']['symbols']:
            if item['contractType'] == 'PERPETUAL' and item['status'] == 'TRADING' and item['symbol'][-4:] == 'USDT':
                self.pipe.lpush('symbols', item['symbol'])
        await self.pipe.execute()
