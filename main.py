import asyncio
import multiprocessing as mp

from aio_binance.error_handler.error import BinanceException
from aio_binance.futures.usdt import Client
from loguru import logger
from config import config


async def run_record(e: mp.Event, symbol: list):
    from price import record
    e.wait()
    await record.Tasks(symbol).creation()


def process_record(e: mp.Event, symbol):
    try:
        asyncio.run(run_record(e, symbol))
    except KeyboardInterrupt:
        pass


async def run_first(e: mp.Event, symbols_):
    from price import first
    try:
        await first.run(symbols_)
    except BinanceException as e:
        logger.error(e)
    else:
        e.set()


def process_first(e: mp.Event, symbols_):
    try:
        asyncio.run(run_first(e, symbols_))
    except KeyboardInterrupt:
        pass


async def run_symbols():
    api = Client(debug='info')
    res = await api.get_public_exchange_info()
    symbols_ = []
    for item in res['data']['symbols']:
        if item['contractType'] == 'PERPETUAL' \
                and item['status'] == 'TRADING' \
                and item['symbol'][-4:] == 'USDT' \
                and item['symbol'] not in config.price.symbols_not_work:
            symbols_.append(item['symbol'])
    return symbols_


if __name__ == '__main__':
    print('   $$$ Run price program:')
    event = mp.Event()
    symbols = asyncio.run(run_symbols())
    srize = 10
    try:
        procs = [
            mp.Process(target=process_first, args=(event, symbols,))
        ]
        srize_symbols = [symbols[i:i + srize] for i in range(0, len(symbols), srize)]
        for symbol_ in srize_symbols:
            procs.append(mp.Process(target=process_record, args=(event, symbol_, )))
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
    except KeyboardInterrupt:
        pass
    print("Close price program")
