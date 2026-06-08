import streamlit as st
import pandas as pd
from app.database import SessionLocal
from app import models
import redis
from streamlit_autorefresh import st_autorefresh

st_autorefresh(interval=5000)

st.title("Distributed Task Processing Platform")

st.subheader("System Overview")

db = SessionLocal()

col1, col2, col3, col4 = st.columns(4)

completed_tasks = (
    db.query(models.Task).filter(models.Task.status == "COMPLETED").count()
)

dashboard_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)

queue_size = dashboard_redis.llen("task_queue")
dlq_size = dashboard_redis.llen("dead_letter_queue")

failed_tasks = db.query(models.Task).filter(models.Task.status == "FAILED").count()

with col1:
    st.metric("Queue Size", queue_size)

with col2:
    st.metric("DLQ Size", dlq_size)


with col3:
    st.metric("Completed", completed_tasks)

with col4:
    st.metric("Failed", failed_tasks)

tasks = db.query(models.Task).order_by(models.Task.id.desc()).limit(20).all()

data = []

for task in tasks:
    data.append(
        {
            "ID": task.id,
            "Task": task.task_name,
            "Status": task.status,
            "Retries": task.retry_count,
        }
    )

df = pd.DataFrame(data)

st.subheader("Recent Tasks")
st.dataframe(df)
