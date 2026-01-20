import os
from pathlib import Path


class Config:
    # 基础配置
    BASE_DIR = Path(__file__).parent.parent
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'sdfs-secret-key-2023'

    # 数据路径配置
    DATA_DIR = BASE_DIR / 's_data'
    AIS_FILE = DATA_DIR / 'AIS.txt'
    ADSB_FILE = DATA_DIR / 'ADSB.jsonl'

    # 缓存配置
    CACHE_DIR = BASE_DIR / 'data_cache'
    PROCESSED_DATA_CACHE = CACHE_DIR / 'processed_data.json'

    # 确保目录存在
    CACHE_DIR.mkdir(exist_ok=True)

    # GIS配置
    MAP_CENTER = [39.9042, 116.4074]  # 北京为中心
    DEFAULT_ZOOM = 8

    # 数据状态配置
    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    MAX_DATA_AGE_HOURS = 24  # 数据最大有效时间（小时）