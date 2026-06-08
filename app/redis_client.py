import os
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
