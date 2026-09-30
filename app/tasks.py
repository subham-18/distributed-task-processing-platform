from app.celery_app import celery_app
from app.database import SessionLocal
from app.models import Task


@celery_app.task
def process_task(task_id):
    db = SessionLocal()

    try:
        task = db.query(Task).filter(Task.id == task_id).first()

        if not task:
            print(f"Task {task_id} not found")
            return

        print(f"Processing task {task_id}")

    finally:
        db.close()