import asyncio
from multiprocessing import Process, Event

import uvicorn

from config import settings
from records import tasks

CONFIG = settings.load_config()


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


def process_symbol(_event):
    try:
        asyncio.run(tasks.Tasks().find_symbol())
    except Exception as e:
        print(e)
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
