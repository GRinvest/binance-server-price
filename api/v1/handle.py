import asyncio
import pickle
from copy import deepcopy
from time import time

import aioredis
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import UJSONResponse
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from loguru import logger
from starlette.datastructures import State

from api.v1 import models
from config import CONFIG
from redis.df import df_from_redis

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
    klines = {}
    for _s in State.symbols:
        klines[_s] = State.klines[timeframe][_s]

    return UJSONResponse(content={"data": klines}, headers={"Access-Control-Allow-Origin": "*"})


async def get_kline(symbol, time_frame, limit, redis, klines):
    async with redis.client() as conn:
        data = []
        res = await conn.lrange(':'.join([symbol, time_frame]), 0, limit)
        for item in res:
            data.append(pickle.loads(item))
        klines[symbol] = data


async def run_klines():
    redis: aioredis.Redis = State.redis
    async with redis.client() as conn:
        symbols = await conn.lrange('symbols', 0, -1)
    _temp = []
    for _s in symbols:
        _temp.append(_s.decode("utf-8"))
    State.symbols = deepcopy(_temp)
    del _temp
    while True:
        State.event_df.acquire()
        temp_klines = {}
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
