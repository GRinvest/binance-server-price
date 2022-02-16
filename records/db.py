import aioredis

from config import settings

CONFIG = settings.load_config()
REDIS_URL = 'redis://{}:{}/{}'.format(
    CONFIG['database']['host'],
    CONFIG['database']['port'],
    CONFIG['database']['dbname']
)

redis: aioredis.Redis = aioredis.Redis.from_url(REDIS_URL)
