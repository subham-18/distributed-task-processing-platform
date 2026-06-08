from app.redis_client import redis_client

redis_client.set("name", "subham")

value = redis_client.get("name")

print(value)
