from celery import Celery

celery_app = Celery(
    "task_engine",
    broker="amqp://guest:guest@rabbitmq:5672//",
    include=["app.tasks"],
)
