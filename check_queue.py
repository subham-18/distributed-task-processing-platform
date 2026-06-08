from app.redis_client import redis_client

task = redis_client.lpop("task_queue")

print(task)