from fastapi import FastAPI
from app.database import engine, Base
from app import models
from app.database import SessionLocal
from app.schemas import TaskCreate
from app.redis_client import redis_client
from prometheus_client import generate_latest
from fastapi.responses import Response

Base.metadata.create_all(bind=engine)

app = FastAPI()


@app.get("/")
def home():
    return {"message": "Backend + PostgreSQL connected"}


@app.get("/tasks")
def get_tasks(status: str = None):

    db = SessionLocal()

    try:

        if status:
            return db.query(models.Task).filter(models.Task.status == status).all()

        return db.query(models.Task).all()

    finally:
        db.close()


@app.post("/tasks")
def create_task(task: TaskCreate):

    db = SessionLocal()

    try:

        new_task = models.Task(task_name=task.task_name)

        db.add(new_task)

        db.commit()

        db.refresh(new_task)
        redis_client.rpush("task_queue", new_task.id)

        return {
            "id": new_task.id,
            "task_name": new_task.task_name,
            "status": new_task.status,
        }

    finally:
        db.close()


@app.get("/metrics")
def metrics():

    return Response(content=generate_latest(), media_type="text/plain")


@app.get("/health")
def health():
    return {"status": "healthy"}
