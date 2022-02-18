import pyarrow as _pa
from aioredis import Redis
from loguru import logger


def df_in_redis(r: Redis.pipeline, alias: str, df) -> None:
    df_compressed = _pa.serialize(df).to_buffer().to_pybytes()
    r.set(alias, df_compressed)


async def df_from_redis(r: Redis, alias: str) -> any:
    data = await r.get(alias)
    try:
        return _pa.deserialize(data)
    except Exception as e:
        logger.error(f"df_from_redis {e}")
