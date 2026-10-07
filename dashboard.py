import os
import time
from datetime import datetime, UTC

import pandas as pd
import redis
import requests
import streamlit as st
from sqlalchemy import create_engine, text

# ============================================================
# CONFIGURATION
# ============================================================

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres123@localhost:5432/task_engine",
)

REDIS_HOST = os.getenv(
    "REDIS_HOST",
    "localhost",
)

API_URL = os.getenv(
    "API_URL",
    "http://fastapi:8000",
)

LOAD_GENERATOR_URL = os.getenv(
    "LOAD_GENERATOR_URL",
    "http://localhost:9000",
)


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Distributed Task Engine",
    page_icon="⚙️",
    layout="wide",
)


# ============================================================
# DATABASE
# ============================================================


@st.cache_resource
def get_db_engine():
    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
    )


# ============================================================
# REDIS
# ============================================================


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
# SESSION STATE
# ============================================================

if "last_saved_test_id" not in st.session_state:
    st.session_state.last_saved_test_id = None


# ============================================================
# DATABASE FUNCTIONS
# ============================================================


def get_task_metrics():

    query = text("""
        SELECT
            COUNT(*) AS total,

            COUNT(*) FILTER (
                WHERE status = 'PENDING'
            ) AS pending,

            COUNT(*) FILTER (
                WHERE status = 'PROCESSING'
            ) AS processing,

            COUNT(*) FILTER (
                WHERE status = 'COMPLETED'
            ) AS completed,

            COUNT(*) FILTER (
                WHERE status = 'FAILED'
            ) AS failed,

            COALESCE(
                SUM(retry_count),
                0
            ) AS retries

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

    return pd.DataFrame(
        rows,
        columns=columns,
    )


def initialize_load_test_table():

    query = text("""
        CREATE TABLE IF NOT EXISTS load_test_runs (
            id BIGSERIAL PRIMARY KEY,
            test_id VARCHAR(100) NOT NULL UNIQUE,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            users INTEGER NOT NULL,
            spawn_rate INTEGER NOT NULL,
            duration INTEGER NOT NULL,
            requests BIGINT NOT NULL DEFAULT 0,
            failures BIGINT NOT NULL DEFAULT 0,
            success_rate DOUBLE PRECISION NOT NULL DEFAULT 100.0,
            rps DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            avg_latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            p50_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            p95_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            p99_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            max_latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0
        )
    """)

    with db.begin() as connection:
        connection.execute(query)


def save_load_test_run(status):

    test_id = status.get("test_id")

    if not test_id:
        return

    query = text("""
        INSERT INTO load_test_runs (
            test_id,
            started_at,
            completed_at,
            users,
            spawn_rate,
            duration,
            requests,
            failures,
            success_rate,
            rps,
            avg_latency_ms,
            p50_ms,
            p95_ms,
            p99_ms,
            max_latency_ms
        )
        VALUES (
            :test_id,
            :started_at,
            :completed_at,
            :users,
            :spawn_rate,
            :duration,
            :requests,
            :failures,
            :success_rate,
            :rps,
            :avg_latency_ms,
            :p50_ms,
            :p95_ms,
            :p99_ms,
            :max_latency_ms
        )
        ON CONFLICT (test_id) DO NOTHING
    """)

    values = {
        "test_id": test_id,
        "started_at": status.get("started_at"),
        "completed_at": datetime.now(UTC),
        "users": status.get("target_users", 0),
        "spawn_rate": status.get("spawn_rate", 0),
        "duration": status.get("duration", 0),
        "requests": status.get("requests", 0),
        "failures": status.get("failures", 0),
        "success_rate": status.get("success_rate", 100.0),
        "rps": status.get("rps", 0.0),
        "avg_latency_ms": status.get("avg_latency_ms", 0.0),
        "p50_ms": status.get("p50_ms", 0.0),
        "p95_ms": status.get("p95_ms", 0.0),
        "p99_ms": status.get("p99_ms", 0.0),
        "max_latency_ms": status.get("max_latency_ms", 0.0),
    }

    with db.begin() as connection:
        connection.execute(query, values)


def get_load_test_history():

    query = text("""
        SELECT
            test_id AS "Test ID",
            users AS "Users",
            spawn_rate AS "Spawn Rate",
            duration AS "Duration",
            requests AS "Requests",
            failures AS "Failures",
            ROUND(success_rate::numeric, 2) AS "Success %",
            ROUND(rps::numeric, 2) AS "RPS",
            ROUND(avg_latency_ms::numeric, 2) AS "Avg ms",
            ROUND(p50_ms::numeric, 2) AS "P50 ms",
            ROUND(p95_ms::numeric, 2) AS "P95 ms",
            ROUND(p99_ms::numeric, 2) AS "P99 ms",
            ROUND(max_latency_ms::numeric, 2) AS "Max ms",
            started_at AS "Started At",
            completed_at AS "Completed At"
        FROM load_test_runs
        ORDER BY completed_at DESC
        LIMIT 50
    """)

    with db.connect() as connection:
        result = connection.execute(query)
        rows = result.fetchall()
        columns = result.keys()

    return pd.DataFrame(rows, columns=columns)


initialize_load_test_table()


# ============================================================
# LOAD GENERATOR API
# ============================================================


def start_load_test(
    users: int,
    spawn_rate: int,
    duration: int,
):

    try:

        response = requests.post(
            f"{LOAD_GENERATOR_URL}/start",
            json={
                "users": users,
                "spawn_rate": spawn_rate,
                "duration": duration,
            },
            timeout=10,
        )

        return response

    except requests.RequestException as exc:

        st.error(f"Could not connect to load generator: {exc}")

        return None


def stop_load_test():

    try:

        response = requests.post(
            f"{LOAD_GENERATOR_URL}/stop",
            timeout=10,
        )

        return response

    except requests.RequestException as exc:

        st.error(f"Could not stop load test: {exc}")

        return None


def get_load_test_status():

    try:

        response = requests.get(
            f"{LOAD_GENERATOR_URL}/status",
            timeout=5,
        )

        if response.status_code == 200:

            return response.json()

    except requests.RequestException:

        pass

    return None


# ============================================================
# HEADER
# ============================================================

st.title("⚙️ Distributed Task Processing Engine")

st.caption("FastAPI • PostgreSQL • RabbitMQ • Celery • Redis • Docker")

st.divider()


# ============================================================
# TASK METRICS
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
# SYSTEM OVERVIEW
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
                    json={"task_name": task_name},
                    timeout=10,
                )

                if response.status_code == 200:

                    st.success(f"Task '{task_name}' created successfully.")

                    time.sleep(0.5)

                    st.rerun()

                else:

                    st.error(f"Task creation failed: " f"{response.text}")

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
# DISTRIBUTED WORKER SYSTEM
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

col1, col2, col3, col4 = st.columns(4)


# FastAPI
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


# Redis
with col2:

    try:

        redis_client.ping()

        st.success("Redis: Healthy")

    except redis.RedisError:

        st.error("Redis: Unhealthy")


# PostgreSQL
with col3:

    try:

        with db.connect() as connection:

            connection.execute(text("SELECT 1"))

        st.success("PostgreSQL: Healthy")

    except Exception:

        st.error("PostgreSQL: Unhealthy")


# Load Generator
with col4:

    load_status = get_load_test_status()

    if load_status:

        if load_status.get(
            "running",
            False,
        ):

            st.success("Load Generator: Running")

        else:

            st.success("Load Generator: Ready")

    else:

        st.error("Load Generator: Unreachable")


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
# PERFORMANCE LAB
# ============================================================

st.subheader("⚡ Performance Lab")

st.caption("Real Locust load testing against the FastAPI service.")


# ============================================================
# LOAD TEST CONTROLS
# ============================================================

control_col1, control_col2, control_col3 = st.columns(3)


with control_col1:

    users = st.selectbox(
        "Concurrent Users",
        [
            10,
            50,
            100,
            500,
            1000,
        ],
        index=0,
    )


with control_col2:

    spawn_rate = st.selectbox(
        "Spawn Rate",
        [
            1,
            5,
            10,
            25,
            50,
            100,
        ],
        index=1,
    )


with control_col3:

    duration = st.number_input(
        "Duration (seconds)",
        min_value=5,
        max_value=300,
        value=30,
        step=5,
    )


# ============================================================
# START / STOP BUTTONS
# ============================================================

button_col1, button_col2 = st.columns(2)


with button_col1:

    start_clicked = st.button(
        "🚀 Start Load Test",
        type="primary",
        use_container_width=True,
    )


with button_col2:

    stop_clicked = st.button(
        "🛑 Stop Load Test",
        use_container_width=True,
    )


if start_clicked:

    response = start_load_test(
        users=users,
        spawn_rate=spawn_rate,
        duration=duration,
    )

    if response is not None:

        if response.status_code == 200:

            st.success(
                f"Load test started: "
                f"{users} users • "
                f"{spawn_rate}/sec • "
                f"{duration}s"
            )

        else:

            st.error(response.text)

if stop_clicked:

    response = stop_load_test()

    if response is not None:

        if response.status_code == 200:

            st.warning("Load test stopped.")

        else:

            st.error(response.text)


# ============================================================
# LIVE PERFORMANCE MONITOR
# ============================================================

st.markdown("### Live Load Test Metrics")


@st.fragment(run_every="2s")
def live_performance_monitor():

    status = get_load_test_status()

    if not status:

        st.error("Load generator unavailable.")

        return

    # ========================================================
    # TEST STATUS
    # ========================================================

    running = status.get(
        "running",
        False,
    )

    if running:

        st.info("🟢 Load test running")

    else:

        st.success("⚪ Load test idle")

    # ========================================================
    # REQUEST METRICS
    # ========================================================

    requests_count = status.get(
        "requests",
        0,
    )

    failures = status.get(
        "failures",
        0,
    )

    success_rate = status.get(
        "success_rate",
        100.0,
    )

    current_rps = status.get(
        "rps",
        0.0,
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:

        st.metric(
            "Requests",
            f"{requests_count:,}",
        )

    with col2:

        st.metric(
            "RPS",
            f"{current_rps:.2f}",
        )

    with col3:

        st.metric(
            "Failures",
            f"{failures:,}",
        )

    with col4:

        st.metric(
            "Success %",
            f"{success_rate:.2f}%",
        )

    # ========================================================
    # LATENCY
    # ========================================================

    st.markdown("#### Response Latency")

    average_latency = status.get(
        "avg_latency_ms",
        0.0,
    )

    p50 = status.get(
        "p50_ms",
        0.0,
    )

    p95 = status.get(
        "p95_ms",
        0.0,
    )

    p99 = status.get(
        "p99_ms",
        0.0,
    )

    max_latency = status.get(
        "max_latency_ms",
        0.0,
    )

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:

        st.metric(
            "Average",
            f"{average_latency:.2f} ms",
        )

    with col2:

        st.metric(
            "P50",
            f"{p50:.2f} ms",
        )

    with col3:

        st.metric(
            "P95",
            f"{p95:.2f} ms",
        )

    with col4:

        st.metric(
            "P99",
            f"{p99:.2f} ms",
        )

    with col5:

        st.metric(
            "Max",
            f"{max_latency:.2f} ms",
        )

    # ========================================================
    # LOAD
    # ========================================================

    st.markdown("#### Load")

    active_users = status.get(
        "active_users",
        0,
    )

    target_users = status.get(
        "target_users",
        0,
    )

    current_spawn_rate = status.get(
        "spawn_rate",
        0,
    )

    col1, col2, col3 = st.columns(3)

    with col1:

        st.metric(
            "Active Users",
            active_users,
        )

    with col2:

        st.metric(
            "Target Users",
            target_users,
        )

    with col3:

        st.metric(
            "Spawn Rate",
            f"{current_spawn_rate}/sec",
        )

    # ========================================================
    # TEST INFORMATION
    # ========================================================

    started_at = status.get("started_at")

    test_duration = status.get("duration")

    if started_at:

        st.caption(f"Started: {started_at}")

    if test_duration:

        st.caption(f"Duration: {test_duration} seconds")

    # ========================================================
    # SAVE COMPLETED TEST
    # ========================================================

    test_id = status.get("test_id")

    if not running and test_id and test_id != st.session_state.last_saved_test_id:

        save_load_test_run(status)

        st.session_state.last_saved_test_id = test_id

    # ========================================================
    # LOAD TEST HISTORY
    # ========================================================

    st.divider()

    st.subheader("📊 Load Test History")

    history_df = get_load_test_history()

    if not history_df.empty:

        # ----------------------------------------------------
        # TABLE
        # ----------------------------------------------------

        st.dataframe(
            history_df,
            use_container_width=True,
            hide_index=True,
        )

        # ----------------------------------------------------
        # THROUGHPUT
        # ----------------------------------------------------

        st.markdown("### Throughput")

        chart_df = history_df[
            [
                "Test ID",
                "RPS",
            ]
        ].copy()

        chart_df = chart_df.set_index("Test ID")
        chart_df = chart_df.iloc[::-1]

        st.line_chart(chart_df)

        # ----------------------------------------------------
        # LATENCY
        # ----------------------------------------------------

        st.markdown("### Latency")

        latency_df = history_df[
            [
                "Test ID",
                "P50 ms",
                "P95 ms",
                "P99 ms",
            ]
        ].copy()

        latency_df = latency_df.set_index("Test ID")
        latency_df = latency_df.iloc[::-1]

        st.line_chart(latency_df)

    else:

        st.info("No load tests completed yet.")


# ============================================================
# START LIVE MONITOR
# ============================================================

live_performance_monitor()


st.divider()


# ============================================================
# ARCHITECTURE
# ============================================================

st.subheader("🏗️ System Architecture")

st.code(
    """
Browser
   │
   ▼
Streamlit Dashboard :8501
   │
   │ control + metrics
   ▼
Load Generator :9000
   │
   │ Locust HTTP load
   ▼
FastAPI :8000
   │
   ├──────────────► PostgreSQL
   │
   ├──────────────► RabbitMQ
   │                    │
   │                    ▼
   │                 Celery
   │                    │
   │                    ▼
   │                 Workers
   │
   └──────────────► Redis

Dashboard and Locust
run as separate services.
""",
    language="text",
)


# ============================================================
# FOOTER
# ============================================================

st.caption(
    "Performance metrics are collected from real Locust execution. "
    "No throughput or latency values are fabricated."
)
