import asyncio
import pickle
from copy import deepcopy
from multiprocessing import JoinableQueue
from time import time

import aioredis
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import UJSONResponse
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from loguru import logger
from starlette.datastructures import State

from api.v1 import models
from config import CONFIG
from redis.df import df_from_redis
from ta import atr, vwap, corr


router = APIRouter(prefix='/v1')
ALGORITHM = "HS256"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def authenticate_token(username: str):
    if username in State.username:
        return True
    return False


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, State.secret, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        expires: int = payload.get('exp', 0)
        if expires < time():
            raise credentials_exception
        token_data = models.TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = authenticate_token(username=token_data.username)
    if not user:
        raise credentials_exception
    return models.User(
        username=username,
        is_active=True
    )


async def get_current_active_user(current_user: models.User = Depends(get_current_user)):
    pass


@router.get("/symbols", dependencies=[Depends(get_current_active_user)])
async def read_users_me(timeframe: str = '1m'):
    return UJSONResponse(content={"data": State.klines[timeframe]}, headers={"Access-Control-Allow-Origin": "*"})


@router.get("/ma", dependencies=[Depends(get_current_active_user)])
async def read_users_me(timeframe: str = '1m'):
    return UJSONResponse(content={"data": State.klines_ma.get(timeframe)}, headers={"Access-Control-Allow-Origin": "*"})


async def get_kline(symbol, time_frame, limit, redis, klines):
    async with redis.client() as conn:
        data = []
        res = await conn.lrange(':'.join([symbol, time_frame]), 0, limit)
        for item in res:
            data.append(pickle.loads(item))
        klines[symbol] = data


async def run_klines(q: JoinableQueue):
    redis: aioredis.Redis = State.redis
    async with redis.client() as conn:
        symbols = await conn.lrange('symbols', 0, -1)
    _temp = []
    for _s in symbols:
        _temp.append(_s.decode("utf-8"))
    State.symbols = deepcopy(_temp)
    del _temp
    lock = asyncio.Lock()
    while True:
        if q.qsize() > 0:
            q.join()
            temp_klines = {}
            async with lock:
                await asyncio.sleep(10)
                async with redis.client() as conn:
                    for time_frame in CONFIG['general']['timeframe']:
                        klines = {}
                        for _s in State.symbols:
                            res = await df_from_redis(conn, f'df:{time_frame}:{_s}')
                            res.drop_duplicates(subset=['open_time'], keep=False, inplace=True)
                            klines[_s] = res.to_json()
                        try:
                            temp_klines[time_frame] = klines
                        except Exception as e:
                            logger.error(e)
                if temp_klines.get('1m'):
                    State.klines = deepcopy(temp_klines)
            print('update kline')
            await asyncio.sleep(40)
        await asyncio.sleep(0.1)


async def create_ma(q: JoinableQueue):
    redis: aioredis.Redis = State.redis
    async with redis.client() as conn:
        symbols = await conn.lrange('symbols', 0, -1)
    _temp = []
    for _s in symbols:
        _temp.append(_s.decode("utf-8"))
    State.symbols_ma = deepcopy(_temp)
    del _temp
    lock = asyncio.Lock()
    while True:
        if q.qsize() > 0:
            q.join()
            temp_klines = {}
            async with lock:
                await asyncio.sleep(1)
                async with redis.client() as conn:
                    for time_frame in CONFIG['general']['timeframe']:
                        df_klines = {}
                        klines = {}
                        for _s in State.symbols_ma:
                            df: pd.DataFrame = await df_from_redis(conn, f'df:{time_frame}:{_s}')
                            df.drop_duplicates(subset=['open_time'], keep=False, inplace=True)
                            df_klines[_s] = deepcopy(df)
                            ind = []
                            for row in df.itertuples(index=False):
                                ind.append(row)
                            ind.reverse()
                            fractals_high = 0
                            for row in ind:
                                if row.fractals_high:
                                    fractals_high = row.High
                                    break
                            fractals_low = 0
                            for row in ind:
                                if row.fractals_low:
                                    fractals_low = row.Low
                                    break
                            klines[_s] = dict(
                                open_time=str(df.open_time.iloc[-1]),
                                Open=str(df.Open.iloc[-1]),
                                High=str(df.High.iloc[-1]),
                                Low=str(df.Low.iloc[-1]),
                                Close=str(df.Close.iloc[-1]),
                                ema_high=str(df.ema_high.iloc[-1]),
                                ema_low=str(df.ema_low.iloc[-1]),
                                atr=str(atr.load(df, 30, -1)),
                                vwap=str(vwap.load(df, -1)),
                                fractals_high=str(fractals_high),
                                fractals_low=str(fractals_low)
                            )
                        df_main = df_klines['BTCUSDT']
                        for symbol, df in df_klines.items():
                            klines[symbol]['corr'] = str(corr.load(df_main, df, -1))
                        try:
                            temp_klines[time_frame] = klines
                        except Exception as e:
                            logger.error(e)

                if temp_klines.get('1m'):
                    State.klines_ma = deepcopy(temp_klines)
                    await State.manager.broadcast(temp_klines)
            print('update kline_ma')
        await asyncio.sleep(0.1)
