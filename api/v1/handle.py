import asyncio
import pickle
from time import time

import aioredis
from aio_binance.timer import AioTimer
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import UJSONResponse
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from starlette.datastructures import State

from api.v1 import models
from config import settings

CONFIG = settings.load_config()
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


@router.get("/symbol", dependencies=[Depends(get_current_active_user)])
async def read_users_me(sym: str = 'all',
                        timeframe: str = '1m',
                        limit: int = 500):
    klines = {}
    redis: aioredis.Redis = State.redis
    async with AioTimer(name='Handle symbol'):
        async with redis.client() as conn:
            tasks = []
            if sym == 'all':
                symbols = await conn.lrange('symbols', 0, -1)
                for _s in symbols:
                    _symbol = _s.decode("utf-8")
                    list_klines = State.klines[timeframe][_symbol]
                    _limit = await conn.lpos(':'.join([_symbol, timeframe]), pickle.dumps(list_klines[0]))

                    if _limit and _limit > 0:
                        tasks.append(
                            asyncio.create_task(
                                get_kline(
                                    _symbol,
                                    timeframe,
                                    _limit - 1,
                                    redis,
                                    klines
                                )
                            )
                        )
                await asyncio.gather(*tasks)

                for _s in symbols:
                    _symbol = _s.decode("utf-8")
                    list_klines: list = State.klines[timeframe][_symbol]
                    i = 0
                    while i < len(klines.get(_symbol, [])):
                        list_klines.pop()
                        i += 1
                    if klines.get(_symbol):
                        temp = klines[_symbol]
                        temp.extend(list_klines)
                        State.klines[timeframe].update({_symbol: temp})
                    klines[_symbol] = State.klines[timeframe][_symbol][:limit]
            else:
                await get_kline(sym,
                                timeframe,
                                limit,
                                redis,
                                klines)
        return UJSONResponse(content={"data": klines}, headers={"Access-Control-Allow-Origin": "*"})

# Example
async def get_kline(symbol, time_frame, limit, redis, klines):
    async with redis.client() as conn:
        data = []
        res = await conn.lrange(':'.join([symbol, time_frame]), 0, limit)
        for item in res:
            data.append(pickle.loads(item))
        klines[symbol] = data


async def first_run_klines():
    temp_klines = {}
    redis: aioredis.Redis = State.redis
    async with redis.client() as conn:
        symbols = await conn.lrange('symbols', 0, -1)
        for time_frame in CONFIG['general']['timeframe']:
            klines = {}
            for _symbol in symbols:
                _s = _symbol.decode("utf-8")
                data = []
                res = await conn.lrange(':'.join([_s, time_frame]), 0, 1000)
                for item in res:
                    data.append(pickle.loads(item))
                klines[_s] = data
            temp_klines[time_frame] = klines
    State.klines = temp_klines
