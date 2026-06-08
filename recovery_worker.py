from app import models
from app.database import SessionLocal
from datetime import datetime, timedelta, UTC
from app.redis_client import redis_client

db = SessionLocal()

now = datetime.now(UTC)

if db:

    task = db.query(models.Task).filter(
        models.Task.updated_at < now - timedelta(seconds=10)
    )

    processed_task = (
        db.query(models.Task).filter(models.Task.status == "PROCESSING").all()
    )

    for t in processed_task:

        t.status = "PENDING"

        t.retry_count += 1

        redis_client.rpush("task_queue", t.id)

        print(f"Recovered task {t.id}")

        db.commit()
