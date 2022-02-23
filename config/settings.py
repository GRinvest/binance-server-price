import pathlib
import sys

import pytoml as toml
from loguru import logger

BASE_DIR = pathlib.Path(__file__).parent.parent
DEFAULT_CONFIG_PATH = BASE_DIR / 'config' / 'binance-price.toml'


def load_config(path=DEFAULT_CONFIG_PATH):
    try:
        with open(path) as f:
            conf = toml.load(f)
    except FileNotFoundError:
        logger.remove()
        logger.add(
            sys.stdout, format="<r>{level}:</r> <W><bold><k>{message}</k></bold></W>")
        logger.warning(
            f"I created a template in {path}, please fill it with the required data and start the server again")
        config_dict = {
            'general': {
                'symbols': 'all',  # or list ['BTCUSDT', 'ETHUSDT', ...],
                'timeframe': ['1m', '15m'],
                'method': 'standard',  # 'standard' or 'index' or 'mark'
                'download_kline': True,
                'symbols_not_work': []
            },
            'database': {
                'host': '127.0.0.1',
                'port': 6379,
                'dbname': 0
            },
            'api': {
                'host': '127.0.0.1',
                'port': 7777,
                'username': ['your_username', '...'],
                'password': ['your_password', '...'],
                'secret_key': 'For create run command: openssl rand -hex 32',  # openssl rand -hex 32
                'access_to_token_expiry_minutes': 1440  # ACCESS_TOKEN_EXPIRE_MINUTES
            }
        }
        with open(path, 'w') as f:
            toml.dump(config_dict, f)
        raise SystemExit(1)
    else:
        return conf
