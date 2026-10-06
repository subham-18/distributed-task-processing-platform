import os
import time

import pandas as pd
import redis
import requests
import streamlit as st
from sqlalchemy import create_engine, text

# ============================================================
# CONFIG
# ============================================================

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres123@localhost:5432/task_engine",
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

API_URL = os.getenv(
    "API_URL",
    "http://fastapi:8000",
)


# ============================================================
# CONNECTIONS
# ============================================================


@st.cache_resource
def get_db_engine():
    return create_engine(DATABASE_URL)


@st.cache_resource
def get_redis():
    return redis.Redis(
        host=REDIS_HOST,
        port=6379,
        decode_responses=True,
    )


db = get_db_engine()
redis_client = get_redis()


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Distributed Task Engine",
    page_icon="⚙️",
    layout="wide",
)


# ============================================================
# HEADER
# ============================================================

st.title("⚙️ Distributed Task Processing Engine")
st.caption("FastAPI • PostgreSQL • RabbitMQ • Celery • Redis • Docker")

st.divider()


# ============================================================
# DATABASE METRICS
# ============================================================


def get_task_metrics():

    query = text("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status = 'PENDING') AS pending,
            COUNT(*) FILTER (WHERE status = 'PROCESSING') AS processing,
            COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed,
            COUNT(*) FILTER (WHERE status = 'FAILED') AS failed,
            COALESCE(SUM(retry_count), 0) AS retries
        FROM tasks
        """)

    with db.connect() as connection:
        result = connection.execute(query).mappings().first()

    return result


def get_recent_tasks():

    query = text("""
        SELECT
            id,
            task_name,
            status,
            retry_count,
            time_stamp,
            updated_at
        FROM tasks
        ORDER BY id DESC
        LIMIT 20
        """)

    with db.connect() as connection:
        result = connection.execute(query)

        rows = result.fetchall()
        columns = result.keys()

    return pd.DataFrame(rows, columns=columns)


# ============================================================
# GET CURRENT STATE
# ============================================================

metrics = get_task_metrics()

queue_size = redis_client.llen("task_queue")
dlq_size = redis_client.llen("dead_letter_queue")


total_tasks = metrics["total"]
pending_tasks = metrics["pending"]
processing_tasks = metrics["processing"]
completed_tasks = metrics["completed"]
failed_tasks = metrics["failed"]
retry_count = metrics["retries"]


# ============================================================
# KPI CARDS
# ============================================================

st.subheader("System Overview")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Queue",
        queue_size,
    )

with col2:
    st.metric(
        "Processing",
        processing_tasks,
    )

with col3:
    st.metric(
        "Completed",
        completed_tasks,
    )

with col4:
    st.metric(
        "Failed",
        failed_tasks,
    )


col5, col6, col7, col8 = st.columns(4)

with col5:
    st.metric(
        "Pending",
        pending_tasks,
    )

with col6:
    st.metric(
        "Retries",
        retry_count,
    )

with col7:
    st.metric(
        "DLQ",
        dlq_size,
    )

with col8:
    st.metric(
        "Total Tasks",
        total_tasks,
    )


st.divider()


# ============================================================
# CREATE TASK
# ============================================================

st.subheader("Create Task")

with st.form("create_task_form"):

    task_name = st.text_input(
        "Task Name",
        placeholder="example: email-processing",
    )

    submitted = st.form_submit_button(
        "Create Task",
        type="primary",
    )

    if submitted:

        if not task_name.strip():

            st.warning("Enter a task name.")

        else:

            try:

                response = requests.post(
                    f"{API_URL}/tasks",
                    json={
                        "task_name": task_name,
                    },
                    timeout=10,
                )

                if response.status_code == 200:

                    st.success(f"Task '{task_name}' created successfully.")

                    time.sleep(0.5)
                    st.rerun()

                else:

                    st.error(f"Task creation failed: {response.text}")

            except requests.RequestException as exc:

                st.error(f"Could not connect to API: {exc}")


st.divider()


# ============================================================
# TASK DISTRIBUTION
# ============================================================

st.subheader("Task Distribution")

distribution = pd.DataFrame(
    {
        "Status": [
            "Pending",
            "Processing",
            "Completed",
            "Failed",
        ],
        "Tasks": [
            pending_tasks,
            processing_tasks,
            completed_tasks,
            failed_tasks,
        ],
    }
)

st.bar_chart(distribution.set_index("Status"))


st.divider()


# ============================================================
# QUEUE / WORKER ARCHITECTURE
# ============================================================

st.subheader("Distributed Worker System")

col1, col2 = st.columns(2)

with col1:

    st.markdown("### Queue State")

    st.write(f"**Main Queue:** `{queue_size}` tasks")

    st.write(f"**Dead Letter Queue:** `{dlq_size}` tasks")

    if queue_size > 0:

        st.warning("Tasks are waiting in the queue.")

    else:

        st.success("Queue is currently empty.")


with col2:

    st.markdown("### Worker State")

    st.write(f"**Tasks currently processing:** " f"`{processing_tasks}`")

    if processing_tasks > 0:

        st.info("Workers are actively processing tasks.")

    else:

        st.success("No tasks are currently processing.")


st.divider()


# ============================================================
# SYSTEM HEALTH
# ============================================================

st.subheader("System Health")

col1, col2, col3 = st.columns(3)


with col1:

    try:

        response = requests.get(
            f"{API_URL}/health",
            timeout=5,
        )

        if response.status_code == 200:

            st.success("FastAPI: Healthy")

        else:

            st.error("FastAPI: Unhealthy")

    except requests.RequestException:

        st.error("FastAPI: Unreachable")


with col2:

    try:

        redis_client.ping()

        st.success("Redis: Healthy")

    except redis.RedisError:

        st.error("Redis: Unhealthy")


with col3:

    try:

        with db.connect() as connection:

            connection.execute(text("SELECT 1"))

        st.success("PostgreSQL: Healthy")

    except Exception:

        st.error("PostgreSQL: Unhealthy")


st.divider()


# ============================================================
# RECENT TASKS
# ============================================================

st.subheader("Recent Tasks")

recent_tasks = get_recent_tasks()

if recent_tasks.empty:

    st.info("No tasks found.")

else:

    st.dataframe(
        recent_tasks,
        use_container_width=True,
        hide_index=True,
    )


st.divider()


# ============================================================
# LOAD TEST SECTION
# ============================================================

st.subheader("⚡ Performance Lab")

st.info(
    "Load testing will measure actual system performance. "
    "No throughput numbers are fabricated."
)

col1, col2, col3 = st.columns(3)

with col1:

    load_rate = st.selectbox(
        "Target Rate",
        [
            100,
            1000,
            10000,
            100000,
            1000000,
        ],
        format_func=lambda x: f"{x:,} requests/sec",
    )

with col2:

    duration = st.number_input(
        "Duration (seconds)",
        min_value=1,
        max_value=60,
        value=10,
    )

with col3:

    st.write("")
    st.write("")

    start_test = st.button(
        "🚀 Start Load Test",
        type="primary",
    )


if start_test:

    st.warning(
        "Load-test engine is the next performance-lab component. "
        f"Target selected: {load_rate:,} requests/sec for {duration}s."
    )


st.divider()


# ============================================================
# REFRESH
# ============================================================

st.caption("Dashboard refreshes automatically every 5 seconds.")

time.sleep(5)
st.rerun()
