from decimal import Decimal
from typing import Optional

from pydantic import BaseModel


class General(BaseModel):
    name_bot: Optional[str] = 'binance-server-price'


class Price(BaseModel):
    symbols_not_work: Optional[list[str]] = ['BTCDOMUSDT']
    flush_db: Optional[bool] = False


class DataBase(BaseModel):
    host: Optional[str] = "127.0.0.1"
    port: Optional[int] = 6379
    dbname: Optional[int] = 0
    password: Optional[str] = "password"


class Config(BaseModel):
    general: General
    price: Price
    database: DataBase

