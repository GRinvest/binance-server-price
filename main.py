import asyncio
from multiprocessing import Process, Event

import uvicorn

from config import CONFIG
from records import tasks


def process_api(_event):
    _event.wait()
    uvicorn.run("app:app",
                host=CONFIG['api']['host'],
                port=CONFIG['api']['port'],
                log_level='debug',
                reload=False)


def process_records(_event, timeframe):
    _event.wait()
    try:
        asyncio.run(tasks.Tasks(timeframe).creation())
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


def process_create_df(_event, time_frame):
    """ Create DataFrame and save Redis"""
    from dataframe import record
    _event.wait()
    asyncio.run(record.run(time_frame))


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
            procs.append(Process(target=process_create_df, args=(event, tf,)))
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
    except KeyboardInterrupt:
        print(f"Close program")
