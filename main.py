import asyncio
from multiprocessing import Process, Event, JoinableQueue

import uvicorn

from config import CONFIG
from records import tasks


def process_api(_event, _q):
    from starlette.datastructures import State
    _event.wait()
    State.q = q
    uvicorn.run("app:app",
                host=CONFIG['api']['host'],
                port=CONFIG['api']['port'],
                log_level='debug',
                reload=False)


def process_records(_event, timeframe, _q):
    _event.wait()
    try:
        asyncio.run(tasks.Tasks(_q, timeframe).creation())
    except KeyboardInterrupt:
        pass
    finally:
        asyncio.run(asyncio.sleep(1))


def process_symbol(_event):
    from loguru import logger
    from records.first import Klines
    try:
        if CONFIG['general']['download_kline']:
            asyncio.run(Klines().run())
    except Exception as e:
        logger.exception(e)
    else:
        _event.set()


def process_create_df(_event, _q: JoinableQueue):
    """ Create DataFrame and save Redis"""
    from dataframe import record
    _event.wait()
    asyncio.run(record.run(_q))


if __name__ == '__main__':
    print('   $$$ Run program:')
    event = Event()
    q = JoinableQueue()
    try:
        procs = [
            Process(target=process_symbol, args=(event,)),
            Process(target=process_api, args=(event, q, ))
        ]
        for tf in CONFIG['general']['timeframe']:
            procs.append(Process(target=process_records, args=(event, tf, q, )))
            procs.append(Process(target=process_create_df, args=(event, q, )))
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
    except KeyboardInterrupt:
        print(f"Close program")
