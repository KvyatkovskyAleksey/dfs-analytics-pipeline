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

# Database connection pooling (critical for RDS performance)
SQLALCHEMY_POOL_SIZE = 10  # Number of permanent connections
SQLALCHEMY_MAX_OVERFLOW = 20  # Additional connections when pool is full
SQLALCHEMY_POOL_TIMEOUT = 30  # Seconds to wait for available connection
SQLALCHEMY_POOL_RECYCLE = 3600  # Recycle connections after 1 hour
SQLALCHEMY_POOL_PRE_PING = True  # Verify connection before using
SQLALCHEMY_ECHO = False  # Disable SQL query logging for performance

# Redis configuration for caching and rate limiting
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')

# Cache configuration using Redis (must be dict for flask-caching)
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 3600,  # Increased from 300 to 3600 (1 hour)
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': int(REDIS_PORT),
    'CACHE_REDIS_DB': 1,
}

# Data cache configuration (for chart/dashboard data)
DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 86400,  # 24 hours for data
    'CACHE_KEY_PREFIX': 'superset_data_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': int(REDIS_PORT),
    'CACHE_REDIS_DB': 3,
}

# Filter state cache configuration (for dashboard filters)
FILTER_STATE_CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 86400,  # 24 hours
    'CACHE_KEY_PREFIX': 'superset_filter_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': int(REDIS_PORT),
    'CACHE_REDIS_DB': 4,
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
SUPERSET_WEBSERVER_TIMEOUT = 600  # Increased from 300 to 600 seconds

# Query optimization
QUERY_CACHE_TIMEOUT = 3600  # Cache query results for 1 hour
QUERY_EARLY_CANCEL = True  # Cancel queries if client disconnects
PREVENT_UNSAFE_DB_CONNECTIONS = False  # Allow DuckDB connections

# Compression for responses
COMPRESS_REGISTER = True
COMPRESS_ALGORITHM = 'gzip'
COMPRESS_LEVEL = 6

# Additional performance settings
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None  # No time limit for CSRF tokens

# Enable data compression in responses
ENABLE_PROXY_FIX = True
