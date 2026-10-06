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
        # ---------------------------------------------------------
        # 1. Fetch task with row-level lock
        # ---------------------------------------------------------
        task = db.query(Task).filter(Task.id == task_id).with_for_update().first()

        if not task:
            print(f"Task {task_id} not found.")
            return

        # ---------------------------------------------------------
        # 2. Idempotency / duplicate protection
        # ---------------------------------------------------------
        if task.status != "PENDING":
            print(f"Task {task_id} is already " f"{task.status}. Skipping.")
            return

        # ---------------------------------------------------------
        # 3. Mark task as PROCESSING
        # ---------------------------------------------------------
        task.status = "PROCESSING"
        task.updated_at = datetime.now(UTC)
        db.commit()

        # ---------------------------------------------------------
        # 4. Start processing metrics
        # ---------------------------------------------------------
        tasks_processing.inc()
        start_time = time.perf_counter()

        print(f"Processing task {task_id}")

        try:
            # -----------------------------------------------------
            # 5. Actual task work
            # -----------------------------------------------------
            # Simulate real processing
            time.sleep(5)

            # -----------------------------------------------------
            # 6. Mark task as COMPLETED
            # -----------------------------------------------------
            task.status = "COMPLETED"
            task.updated_at = datetime.now(UTC)

            db.commit()

            # Increment ONLY after successful completion
            tasks_completed_total.inc()

            duration = time.perf_counter() - start_time

            print(f"Task {task_id} completed " f"in {duration:.3f}s")

        except Exception as exc:
            # -----------------------------------------------------
            # 7. Task execution failed
            # -----------------------------------------------------
            task.retry_count += 1
            task.updated_at = datetime.now(UTC)

            # -----------------------------------------------------
            # 8. Check whether retry is allowed
            # -----------------------------------------------------
            if task.retry_count <= MAX_RETRIES:

                task.status = "PENDING"
                db.commit()

                tasks_retried_total.inc()

                # Exponential backoff:
                # retry 1 -> 2 sec
                # retry 2 -> 4 sec
                # retry 3 -> 8 sec
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

            # -----------------------------------------------------
            # 9. Permanent failure
            # -----------------------------------------------------
            task.status = "FAILED"
            db.commit()

            tasks_failed_total.inc()

            print(
                f"Task {task_id} permanently failed "
                f"after {task.retry_count} retries"
            )

            # -----------------------------------------------------
            # 10. Send permanently failed task to DLQ
            # -----------------------------------------------------
            celery_app.send_task(
                "app.tasks.handle_dead_letter",
                args=[task_id],
            )

        finally:
            # -----------------------------------------------------
            # 11. Metrics cleanup for EVERY processing attempt
            # -----------------------------------------------------
            duration = time.perf_counter() - start_time

            task_processing_duration_seconds.observe(duration)

            tasks_processing.dec()

    finally:
        # ---------------------------------------------------------
        # 12. Always close database connection
        # ---------------------------------------------------------
        db.close()


@celery_app.task
def handle_dead_letter(task_id):
    """
    Handles tasks that permanently failed
    after exhausting all retry attempts.
    """

    print(f"Task {task_id} moved to DLQ")
