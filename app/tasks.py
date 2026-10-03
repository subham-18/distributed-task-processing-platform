import time
from datetime import datetime, UTC

from app.celery_app import celery_app
from app.database import SessionLocal
from app.models import Task

MAX_RETRIES = 3
BASE_DELAY = 2


@celery_app.task
def process_task(task_id):
    db = SessionLocal()

    try:
        task = db.query(Task).filter(Task.id == task_id).with_for_update().first()

        if not task:
            return

        if task.status != "PENDING":
            print(f"Task {task_id} is {task.status}. Skipping.")
            return

        task.status = "PROCESSING"
        db.commit()

        print(f"Processing task {task_id}")

        try:
            # Simulate actual work
            time.sleep(5)

            task.status = "COMPLETED"
            task.updated_at = datetime.now(UTC)
            db.commit()

            print(f"Task {task_id} completed")

        except Exception as exc:
            task.retry_count += 1
            task.updated_at = datetime.now(UTC)

            if task.retry_count <= MAX_RETRIES:
                task.status = "PENDING"
                db.commit()

                delay = BASE_DELAY**task.retry_count

                print(
                    f"Task {task_id} failed. "
                    f"Retry {task.retry_count}/{MAX_RETRIES} "
                    f"in {delay}s"
                )

                raise process_task.retry(
                    exc=exc,
                    countdown=delay,
                    max_retries=MAX_RETRIES,
                )

            task.status = "FAILED"
            db.commit()

            print(f"Task {task_id} permanently failed")

            # Send permanently failed task to DLQ
            celery_app.send_task(
                "app.tasks.handle_dead_letter",
                args=[task_id],
            )

    finally:
        db.close()


@celery_app.task
def handle_dead_letter(task_id):
    print(f"Task {task_id} moved to DLQ")
