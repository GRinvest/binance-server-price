import aioredis

from config.loads import data

if data.database.password == 'password':
    host = 'redis://{}:{}/{}'.format(
        data.database.host,
        data.database.port,
        data.database.dbname
    )
else:
    host = 'redis://:{}@{}:{}/{}'.format(
        data.database.password,
        data.database.host,
        data.database.port,
        data.database.dbname
    )

redis: aioredis.Redis = aioredis.Redis.from_url(host)

redis_decoder: aioredis = aioredis.Redis.from_url(host, decode_responses=True)
