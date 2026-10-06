import time
from datetime import datetime, UTC

from app.celery_app import celery_app
from app.database import SessionLocal
from app.models import Task

from app.metrics import (
    tasks_completed_total,
    tasks_failed_total,
    tasks_retried_total,
    tasks_processing,
    task_processing_duration_seconds,
)

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

        # Mark task as processing
        task.status = "PROCESSING"
        db.commit()

        # Start measuring actual processing
        tasks_processing.inc()
        start_time = time.perf_counter()

        print(f"Processing task {task_id}")

        try:
            # Simulate actual work
            time.sleep(5)

            # Measure actual processing duration
            duration = time.perf_counter() - start_time

            task_processing_duration_seconds.observe(duration)
            tasks_processing.dec()

            task.status = "COMPLETED"
            task.updated_at = datetime.now(UTC)

            db.commit()

            tasks_completed_total.inc()

            print(f"Task {task_id} completed " f"in {duration:.3f}s")

        except Exception as exc:
            # Current execution has finished,
            # so remove it from active processing.
            tasks_processing.dec()

            task.retry_count += 1
            task.updated_at = datetime.now(UTC)

            # Measure failed attempt duration
            duration = time.perf_counter() - start_time
            task_processing_duration_seconds.observe(duration)

            if task.retry_count <= MAX_RETRIES:
                task.status = "PENDING"
                db.commit()

                tasks_retried_total.inc()

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

            # Permanent failure
            task.status = "FAILED"
            db.commit()

            tasks_failed_total.inc()

            print(
                f"Task {task_id} permanently failed "
                f"after {task.retry_count} retries"
            )

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
