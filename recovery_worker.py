from datetime import datetime, timedelta, UTC

from app import models
from app.database import SessionLocal
from app.tasks import process_task

db = SessionLocal()

try:
    now = datetime.now(UTC)

    processed_task = (
        db.query(models.Task)
        .filter(
            models.Task.status == "PROCESSING",
            models.Task.updated_at < now - timedelta(seconds=10),
        )
        .all()
    )

    for task in processed_task:
        task.status = "PENDING"
        task.retry_count += 1

        db.commit()

        process_task.delay(task.id)

        print(f"Recovered task {task.id}")

finally:
    db.close()
