"""
Superset configuration for DFS Analytics Pipeline
"""
import os
from cachelib.redis import RedisCache

# PostgreSQL Database URI
SQLALCHEMY_DATABASE_URI = os.getenv(
    'SQLALCHEMY_DATABASE_URI',
    'postgresql+psycopg2://airflow:airflow@postgres:5432/superset'
)

# Redis configuration for caching and rate limiting
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')

# Cache configuration using Redis (must be dict for flask-caching)
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': int(REDIS_PORT),
    'CACHE_REDIS_DB': 1,
}

# Results backend configuration (must be RedisCache instance for Superset)
RESULTS_BACKEND = RedisCache(
    host=REDIS_HOST,
    port=int(REDIS_PORT),
    db=0,
    key_prefix='superset_results_',
    default_timeout=86400  # 24 hours
)

# Rate limiting using Redis
RATELIMIT_ENABLED = True
RATELIMIT_STORAGE_URI = f'redis://{REDIS_HOST}:{REDIS_PORT}/2'

# Secret key
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'CHANGE_ME_IN_PRODUCTION')

# Feature flags
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
}

# Allow file uploads
UPLOAD_FOLDER = '/app/superset_home/uploads/'
IMG_UPLOAD_FOLDER = '/app/superset_home/uploads/'

# Row limit for SQL Lab
SQL_MAX_ROW = 100000

# Enable async queries
SUPERSET_WEBSERVER_TIMEOUT = 300
