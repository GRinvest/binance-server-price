import asyncio
from multiprocessing import Process, Event

import uvicorn

from config import settings
from records import tasks

CONFIG = settings.load_config()
REDIS_URL = 'redis://{}:{}/{}'.format(
    CONFIG['database']['host'],
    CONFIG['database']['port'],
    CONFIG['database']['dbname']
)


def process_api(_event):
    _event.wait()
    uvicorn.run("app:app", host=CONFIG['api']['host'], port=CONFIG['api']['port'],
                log_level='debug', reload=False)


def process_records(_event, timeframe):
    import platform
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    _event.wait()
    try:
        asyncio.run(tasks.Tasks(timeframe).creation())
    except KeyboardInterrupt:
        pass
    finally:
        asyncio.run(asyncio.sleep(1))


async def calculate_kline():
    import aioredis
    from aio_binance.futures.usdt import ApiSession
    import pickle

    _tasks = []
    srize = 4
    await tasks.find_symbol()
    redis = await aioredis.from_url(REDIS_URL)
    async with ApiSession() as session:
        instance = tasks.AddedKlines(session)
        for _tf in CONFIG['general']['timeframe']:
            async with redis.pipeline(transaction=True) as pipe:
                time_kline_last, symbols = await pipe.lindex(f'BTCUSDT:{_tf}', 0).lrange('symbols', 0, -1).execute()
            if time_kline_last:
                time_kline_last = pickle.loads(time_kline_last)
                for _s in symbols:
                    symbol = _s.decode("utf-8")
                    _tasks.append(asyncio.create_task(instance.added(symbol, _tf, time_kline_last)))
            else:
                for _s in symbols:
                    symbol = _s.decode("utf-8")
                    _tasks.append(asyncio.create_task(instance.new(symbol, _tf)))

        srize_list = [_tasks[i:i + srize] for i in range(0, len(_tasks), srize)]

        for item in srize_list:
            await asyncio.gather(*item)
    async with redis.pipeline(transaction=True) as pipe:

        for key, value in instance.klines.items():
            print(f"{key} {len(value)}")
            pipe.lpush(key, *value)
        await pipe.execute()
    await asyncio.sleep(1)


def process_symbol(_event):
    from loguru import logger
    try:
        asyncio.run(calculate_kline())
    except Exception as e:
        logger.exception(e)
    else:
        _event.set()


if __name__ == '__main__':
    print('   $$$ Run program:')
    event = Event()
    try:
        procs = [
            Process(target=process_symbol, args=(event,)),
            Process(target=process_api, args=(event,))
        ]
        for tf in CONFIG['general']['timeframe']:
            procs.append(Process(target=process_records, args=(event, tf,)))
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
    except KeyboardInterrupt:
        print(f"Close program")
