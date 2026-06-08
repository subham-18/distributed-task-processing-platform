from app.redis_client import redis_client

tasks = redis_client.lrange("dead_letter_queue", 0, -1)

print(tasks)
