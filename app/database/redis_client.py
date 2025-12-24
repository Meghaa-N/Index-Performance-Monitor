import redis
"""
Simple Redis client setup with connection pooling."""
_redis = None

def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.Redis(
            host="redis",
            port=6379,
            decode_responses=True,
            socket_connect_timeout=2
        )
    return _redis

def flush_redis():
    """
    Utility function to flush all Redis data that contains 'index' in its key.
    """
    redis_client = get_redis()
    for key in redis_client.scan_iter("*index*"):
        redis_client.delete(key)
    print("Flushed Redis keys containing 'index'")