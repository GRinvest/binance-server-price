import asyncio
from time import time

import aioredis
from aio_binance.timer import AioTimer
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import UJSONResponse
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from starlette.datastructures import State

from api.v1 import models

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
async def read_users_me(symbol: str = 'all',
                        timeframe: str = '1m',
                        limit: int = 500):
    klines = {}
    redis: aioredis.Redis = State.redis
    async with redis.client() as conn:
        tasks = []
        if symbol == 'all':
            symbols = await conn.lrange('symbols', 0, -1)
            for _symbol in symbols:
                tasks.append(
                    asyncio.create_task(
                        get_kline(
                            _symbol,
                            timeframe,
                            limit,
                            redis,
                            klines
                        )
                    )
                )
            await asyncio.gather(*tasks)
        else:
            await get_kline(symbol,
                            timeframe,
                            limit,
                            redis,
                            klines)
    return UJSONResponse(content={"data": klines}, headers={"Access-Control-Allow-Origin": "*"})


async def get_kline(symbol, timeframe, limit, redis, klines):
    list_keys = ['t', 'o', 'h', 'l', 'c', 'v', 'T', 'q', 'n', 'V', 'Q', 'B']
    list_klines = []
    async with AioTimer(name='redis'):
        async with redis.pipeline(transaction=True) as pipe:
            for key in list_keys:
                pipe.lrange(
                    ':'.join([symbol, timeframe, key]), 0, limit
                )
            k = await pipe.execute()
    i = 0
    async with AioTimer(name='civil'):
        while i < len(k[0]):
            _k = []
            a = 0
            while a < len(k):
                _k.append(k[a][i])
                a += 1
            list_klines.append(_k)
            i += 1
    klines[symbol] = {
        'timeframe': timeframe,
        'klines': list_klines
    }
