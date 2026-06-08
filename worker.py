import time
import random
from datetime import datetime, UTC

from app.database import SessionLocal
from app.redis_client import redis_client
from app import models
from app.logger import logger

from app.metrics import tasks_completed_total, tasks_failed_total, tasks_retried_total

logger.info("Worker started")

WORKER_NAME = "worker-1"

MAX_RETRIES = 5

while True:

    task_id = redis_client.lpop("task_queue")

    if task_id:

        print(f"Processing task {task_id}")

        db = SessionLocal()

        try:

            task = (
                db.query(models.Task)
                .filter(models.Task.id == int(task_id))
                .with_for_update()
                .first()
            )

            if not task:
                continue

            # Idempotency Check
            if task.status == "COMPLETED":
                tasks_completed_total.inc()
                logger.info(
                    f"task_id={task_id} already completed by worker={WORKER_NAME} status=COMPLETED , skiping it "
                )
                continue

            if task.status != "PENDING":
                continue

            task.status = "PROCESSING"
            task.updated_at = datetime.now(UTC)
            db.commit()

            print(f"{WORKER_NAME} processing task {task_id}")

            time.sleep(5)

            success = random.choice([True, False])

            if success:

                task.status = "COMPLETED"
                tasks_completed_total.inc()
                task.updated_at = datetime.now(UTC)
                db.commit()

                logger.info(f"task_id={task_id} worker={WORKER_NAME} status=COMPLETED")

            else:

                task.status = "FAILED"
                task.updated_at = datetime.now(UTC)
                task.retry_count += 1

                db.commit()

                logger.error(
                    f"task_id={task_id} worker={WORKER_NAME} status=FAILED retry_count={task.retry_count}"
                )

                if task.retry_count < MAX_RETRIES:

                    tasks_retried_total.inc()
                    redis_client.rpush("task_queue", task.id)

                    print(f"Task {task_id} pushed back to queue by {WORKER_NAME}")

                else:
                    redis_client.rpush("dead_letter_queue", task.id)
                    print(
                        f"Task {task_id} moved to DLQ after {MAX_RETRIES} retries by {WORKER_NAME}"
                    )

        finally:
            db.close()

    time.sleep(1)
