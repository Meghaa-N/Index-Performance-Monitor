from functools import wraps
import duckdb
import json

from app.database.redis_client import get_redis

DB_PATH = "app/database/index_data.duckdb"

def with_db_connection(func):
    """
    Decorator to manage database connection.

    :param func: Function to be decorated
    :return      : Wrapped function with database connection management
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        conn = duckdb.connect(DB_PATH)
        try:
            return func(conn, *args, **kwargs)
        finally:
            conn.close()

    return wrapper


def memoize(func):
    """
    Decorator to cache by function name and date.

    :param func     : Function to be decorated
    :return         : Wrapped function with caching
    """

    @wraps(func)
    def wrapper(*args, **kwargs):

        # Extract date argument
        if "date" in kwargs:
            dt = kwargs["date"]
        else:
            dt = args[0]  # assume first param is date

        cache_key = f"{func.__name__}:{dt}"  # Cache by function name and date only

        redis_client = get_redis()
        # Check Redis cache
        cached = redis_client.get(cache_key)
        if cached:
            print("CACHE HIT:", cache_key)
            return json.loads(cached)

        print("CACHE MISS:", cache_key)

        # On cache miss, call the function
        result = func(*args, **kwargs)
        # Store the result in Redis cache
        redis_client.set(cache_key, json.dumps(result, default=str))

        return result

    return wrapper
