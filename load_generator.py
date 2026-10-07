import os
import uuid
from datetime import datetime, UTC

import gevent

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from locust import HttpUser, task, between
from locust.env import Environment

# ============================================================
# CONFIG
# ============================================================

TARGET_URL = os.getenv(
    "TARGET_URL",
    "http://fastapi:8000",
)


# ============================================================
# LOCUST USER
# ============================================================


class TaskEngineUser(HttpUser):

    host = TARGET_URL

    wait_time = between(
        0.1,
        0.5,
    )

    @task
    def create_task(self):

        self.client.post(
            "/tasks",
            json={"task_name": "load-test-task"},
        )


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="Task Engine Load Generator",
)


# ============================================================
# REQUEST MODEL
# ============================================================


class StartTestRequest(BaseModel):

    users: int = Field(
        ge=1,
        le=10000,
    )

    spawn_rate: int = Field(
        ge=1,
        le=1000,
    )

    duration: int = Field(
        ge=5,
        le=3600,
    )


# ============================================================
# GLOBAL STATE
# ============================================================

environment = None
runner = None

test_id = None

started_at = None

target_users = 0
spawn_rate = 0
duration = 0


# ============================================================
# CREATE ENVIRONMENT
# ============================================================


def create_test_environment():

    global environment
    global runner

    environment = Environment(
        user_classes=[TaskEngineUser],
        host=TARGET_URL,
    )

    runner = environment.create_local_runner()

    return environment, runner


# ============================================================
# SNAPSHOT STATS
# ============================================================


def get_stats():

    if environment is None:

        return {
            "requests": 0,
            "failures": 0,
            "success_rate": 100.0,
            "rps": 0.0,
            "avg_latency_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "max_latency_ms": 0.0,
            "active_users": 0,
        }

    stats = environment.stats.total

    requests = stats.num_requests

    failures = stats.num_failures

    if requests > 0:

        success_rate = ((requests - failures) / requests) * 100

    else:

        success_rate = 100.0

    return {
        "requests": requests,
        "failures": failures,
        "success_rate": round(
            success_rate,
            2,
        ),
        "rps": round(
            stats.current_rps,
            2,
        ),
        "avg_latency_ms": round(
            stats.avg_response_time,
            2,
        ),
        "p50_ms": round(
            stats.get_response_time_percentile(0.50),
            2,
        ),
        "p95_ms": round(
            stats.get_response_time_percentile(0.95),
            2,
        ),
        "p99_ms": round(
            stats.get_response_time_percentile(0.99),
            2,
        ),
        "max_latency_ms": round(
            stats.max_response_time,
            2,
        ),
        "active_users": (runner.user_count if runner is not None else 0),
    }


# ============================================================
# STOP TEST
# ============================================================


def stop_current_test():

    global runner

    if runner is not None:

        try:

            runner.stop()

        except Exception:

            pass


# ============================================================
# AUTOMATIC STOP
# ============================================================


def stop_after_duration():

    gevent.sleep(duration)

    stop_current_test()


# ============================================================
# START
# ============================================================


@app.post("/start")
def start_test(
    request: StartTestRequest,
):

    global environment
    global runner

    global test_id
    global started_at

    global target_users
    global spawn_rate
    global duration

    # --------------------------------------------------------
    # Prevent overlapping tests
    # --------------------------------------------------------

    if runner is not None and runner.user_count > 0:

        raise HTTPException(
            status_code=409,
            detail="A load test is already running.",
        )

    # --------------------------------------------------------
    # Create fresh environment
    # --------------------------------------------------------

    environment = Environment(
        user_classes=[TaskEngineUser],
        host=TARGET_URL,
    )

    runner = environment.create_local_runner()

    # --------------------------------------------------------
    # Store configuration
    # --------------------------------------------------------

    test_id = str(uuid.uuid4())

    started_at = datetime.now(UTC).isoformat()

    target_users = request.users

    spawn_rate = request.spawn_rate

    duration = request.duration

    # --------------------------------------------------------
    # Start Locust
    # --------------------------------------------------------

    runner.start(
        user_count=request.users,
        spawn_rate=request.spawn_rate,
    )

    # --------------------------------------------------------
    # Automatic stop
    # --------------------------------------------------------

    gevent.spawn(stop_after_duration)

    return {
        "success": True,
        "message": "Load test started.",
        "test_id": test_id,
        "users": request.users,
        "spawn_rate": request.spawn_rate,
        "duration": request.duration,
    }


# ============================================================
# STOP ENDPOINT
# ============================================================


@app.post("/stop")
def stop_test():

    if runner is None:

        return {
            "success": True,
            "message": "No active test.",
        }

    stop_current_test()

    return {
        "success": True,
        "message": "Load test stopped.",
    }


# ============================================================
# STATUS
# ============================================================


@app.get("/status")
def status():

    stats = get_stats()

    running = runner is not None and runner.user_count > 0

    return {
        "running": running,
        "test_id": test_id,
        "started_at": started_at,
        "target_users": target_users,
        "spawn_rate": spawn_rate,
        "duration": duration,
        **stats,
    }


# ============================================================
# HEALTH
# ============================================================


@app.get("/health")
def health():

    return {"status": "healthy"}
