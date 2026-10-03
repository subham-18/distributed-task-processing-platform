# Distributed Task Processing Platform

A fault-tolerant distributed task processing platform built with **FastAPI, PostgreSQL, RabbitMQ, Celery, Redis, Prometheus, Streamlit, Docker, and Docker Compose**.

The project demonstrates how asynchronous tasks can be persisted, distributed across multiple workers, retried after failures, protected against duplicate execution, recovered after worker failures, and monitored through operational metrics.

---

## 1. Project Overview

The platform accepts tasks through a FastAPI API and persists their state in PostgreSQL.

Instead of executing long-running work inside the API request, tasks are dispatched to Celery through RabbitMQ. Multiple Celery workers can process tasks independently.

The system also demonstrates:

- Task state management
- Distributed worker execution
- Database row-level locking
- Idempotency / duplicate-task protection
- Retry with exponential backoff
- Dead-letter handling
- Crash recovery
- Structured logging
- Prometheus metrics
- Operational monitoring
- Docker-based deployment

---

# 2. Architecture

```text
                         ┌────────────────────┐
                         │       Client       │
                         └─────────┬──────────┘
                                   │
                                   ▼
                         ┌────────────────────┐
                         │      FastAPI       │
                         │    REST API        │
                         └─────────┬──────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
                    ▼                             ▼
          ┌──────────────────┐          ┌──────────────────┐
          │    PostgreSQL    │          │      Redis       │
          │                  │          │ Supporting       │
          │ Task State       │          │ Infrastructure   │
          └────────┬─────────┘          └──────────────────┘
                   │
                   ▼
          ┌──────────────────┐
          │      Celery      │
          │  Task Execution  │
          └────────┬─────────┘
                   │
                   ▼
          ┌──────────────────┐
          │     RabbitMQ     │
          │  Message Broker  │
          └────────┬─────────┘
                   │
             ┌─────┴─────┐
             │           │
             ▼           ▼
       ┌──────────┐ ┌──────────┐
       │ Worker 1 │ │ Worker 2 │
       └────┬─────┘ └────┬─────┘
            │             │
            └──────┬──────┘
                   ▼
             Task Execution
                   │
          ┌────────┼────────┐
          │        │        │
          ▼        ▼        ▼
       Success   Failure   Crash
                   │        │
                   ▼        ▼
                Retry    Recovery
                   │
                   ▼
            Exponential
              Backoff
                   │
                   ▼
            Final Failure
                   │
                   ▼
          Dead-Letter Handler


Monitoring:

FastAPI
   │
   ▼
Prometheus Metrics
   │
   ▼
Streamlit Operations Dashboard
```

---

# 3. Task Lifecycle

A task follows the following lifecycle:

```text
                 ┌──────────┐
                 │ PENDING  │
                 └────┬─────┘
                      │
                      ▼
               ┌─────────────┐
               │ PROCESSING  │
               └──────┬──────┘
                      │
             ┌────────┴────────┐
             │                 │
             ▼                 ▼
        ┌───────────┐      Failure
        │ COMPLETED │         │
        └───────────┘         ▼
                           Retry
                             │
                    ┌────────┴────────┐
                    │                 │
                    ▼                 ▼
                 Retry 1           Retry 2
                    │                 │
                    └────────┬────────┘
                             │
                          Retry 3
                             │
                             ▼
                          FAILED
                             │
                             ▼
                    Dead-Letter Handler
```

---

# 4. Core Components

## FastAPI

FastAPI provides the REST API used to create and retrieve tasks.

Example endpoints:

```text
POST /tasks
GET  /tasks
GET  /health
GET  /metrics
```

The API is responsible for accepting requests and creating the persistent task record.

Long-running work is not performed synchronously inside the API request.

---

## PostgreSQL

PostgreSQL acts as the **persistent source of truth for task state**.

The task record contains information such as:

- Task ID
- Task name
- Status
- Creation timestamp
- Updated timestamp
- Retry count

The database is also used for concurrency control through row-level locking.

---

## RabbitMQ

RabbitMQ acts as the **message broker** between Celery and the workers.

Instead of the API directly executing the task:

```text
FastAPI
   ↓
RabbitMQ
   ↓
Worker
```

This allows task execution to be separated from request handling.

---

## Celery

Celery provides distributed task execution.

Multiple workers can consume tasks from RabbitMQ:

```text
             RabbitMQ
              │
       ┌──────┴──────┐
       ▼             ▼
   Worker 1       Worker 2
```

Workers can therefore be scaled horizontally.

---

## Redis

Redis is included as supporting infrastructure.

The project originally used Redis for task queue experimentation and recovery.

The current primary task execution path uses:

```text
Celery → RabbitMQ → Workers
```

rather than the old Redis task queue.

---

# 5. Concurrency Protection

Multiple workers may receive duplicate deliveries of the same task.

For example:

```text
Worker A ──────┐
               ├── Task 100
Worker B ──────┘
```

Without protection, both workers could execute the same task.

The worker therefore uses PostgreSQL row-level locking:

```python
task = (
    db.query(Task)
    .filter(Task.id == task_id)
    .with_for_update()
    .first()
)
```

The worker then verifies:

```python
if task.status != "PENDING":
    return
```

The resulting flow is:

```text
Worker A
   ↓
Acquire row lock
   ↓
PENDING → PROCESSING
   ↓
COMMIT
   ↓
Lock released

Worker B
   ↓
Attempts same task
   ↓
Sees task is no longer PENDING
   ↓
SKIP
```

This provides duplicate-task protection at the database level.

---

# 6. Retry Mechanism

Temporary task failures should not immediately result in permanent failure.

The project uses Celery retry support with exponential backoff.

Example:

```text
Attempt 1
   ↓
Failure
   ↓
Wait 2 seconds

Attempt 2
   ↓
Failure
   ↓
Wait 4 seconds

Attempt 3
   ↓
Failure
   ↓
Wait 8 seconds

Final failure
```

The retry delay is calculated using:

```python
delay = BASE_DELAY ** task.retry_count
```

The system limits the maximum number of retries.

---

# 7. Dead-Letter Handling

When a task exceeds the maximum retry count:

```text
Task
 ↓
Retry 1
 ↓
Retry 2
 ↓
Retry 3
 ↓
Permanent failure
```

the task is marked:

```text
FAILED
```

and sent to the application's dead-letter handling task.

The current implementation uses an **application-level dead-letter handler**:

```python
celery_app.send_task(
    "app.tasks.handle_dead_letter",
    args=[task_id],
)
```

The dead-letter handler can be used to inspect or process permanently failed tasks.

> Note: This implementation does not currently use RabbitMQ's native Dead Letter Exchange (DLX) topology.

---

# 8. Crash Recovery

A worker can crash after changing a task to:

```text
PROCESSING
```

but before completing the work.

This can leave a task stuck.

The recovery worker identifies stale processing tasks:

```text
PROCESSING
     │
     │ Worker crashes
     ▼
Stale PROCESSING task
     │
     ▼
Recovery Worker
     │
     ▼
PENDING
     │
     ▼
Celery
     │
     ▼
RabbitMQ
     │
     ▼
Available Worker
```

The recovery process only considers tasks that have remained in `PROCESSING` beyond the configured timeout.

Recovered tasks are dispatched through Celery:

```python
process_task.delay(task.id)
```

---

# 9. Observability

The project includes operational monitoring.

## Structured Logging

Workers log important lifecycle events:

```text
Processing task
Task completed
Task failed
Retry scheduled
Task permanently failed
Task recovered
```

---

## Prometheus Metrics

The API exposes:

```text
GET /metrics
```

Metrics include task execution information such as:

- Completed tasks
- Failed tasks
- Retried tasks

---

## Health Check

The service exposes:

```text
GET /health
```

Example response:

```json
{
    "status": "healthy"
}
```

---

# 10. Operations Dashboard

A Streamlit dashboard provides operational visibility.

The dashboard can display:

- Task activity
- Queue information
- Completed tasks
- Failed tasks
- Dead-letter information
- Task status
- Operational metrics

Screenshots are stored under:

```text
screenshots/
```

Example:

```markdown
![Operations Dashboard](screenshots/dashboard.png)
```

---

# 11. Technology Stack

| Layer | Technology |
|---|---|
| API | FastAPI |
| Language | Python |
| ORM | SQLAlchemy |
| Database | PostgreSQL |
| Task Execution | Celery |
| Message Broker | RabbitMQ |
| Supporting Infrastructure | Redis |
| Monitoring | Prometheus |
| Dashboard | Streamlit |
| Containerization | Docker |
| Orchestration | Docker Compose |

---

# 12. Docker Architecture

The application is containerized using Docker Compose.

Main services:

```text
postgres
redis
rabbitmq
fastapi
worker
```

Multiple workers can be started using:

```bash
docker compose up -d --scale worker=2
```

This allows horizontal scaling of task execution.

---

# 13. Running the Project

## Clone Repository

```bash
git clone https://github.com/subham-18/distributed-task-processing-platform.git

cd distributed-task-processing-platform
```

## Start Services

```bash
docker compose up --build
```

## API Documentation

```text
http://localhost:8000/docs
```

## Health

```text
http://localhost:8000/health
```

## Metrics

```text
http://localhost:8000/metrics
```

## RabbitMQ Management

```text
http://localhost:15672
```

## Dashboard

```bash
streamlit run dashboard.py
```

---

# 14. API Examples

## Create Task

```http
POST /tasks
```

Request:

```json
{
    "task_name": "example-task"
}
```

The API creates the task in PostgreSQL and dispatches it to Celery.

---

## Get Tasks

```http
GET /tasks
```

---

## Filter Tasks

```http
GET /tasks?status=COMPLETED
```

---

# 15. Engineering Concepts Demonstrated

This project demonstrates practical understanding of:

- Distributed task processing
- Asynchronous execution
- Message brokers
- Worker pools
- Horizontal worker scaling
- Database transactions
- PostgreSQL row-level locking
- Idempotency
- Retry strategies
- Exponential backoff
- Dead-letter handling
- Crash recovery
- Fault tolerance
- Observability
- Metrics
- Structured logging
- Containerization
- Docker Compose

---

# 16. Important Design Decisions

## Why PostgreSQL?

PostgreSQL provides durable persistence and acts as the source of truth for task state.

## Why RabbitMQ?

RabbitMQ separates task production from task execution and provides reliable message transport.

## Why Celery?

Celery provides distributed worker execution and built-in retry/task management capabilities.

## Why multiple workers?

Multiple workers allow the processing workload to scale horizontally and provide better fault isolation.

## Why row-level locking?

Row-level locking prevents concurrent workers from simultaneously claiming the same pending task.

## Why exponential backoff?

Immediate retries can repeatedly hit a temporarily failing dependency. Increasing the delay between attempts reduces repeated pressure on the failing system.

## Why crash recovery?

A worker can terminate unexpectedly after claiming a task. Recovery prevents tasks from remaining permanently stuck in `PROCESSING`.

---

# 17. Failure Scenarios Tested

### Duplicate Task Delivery

```text
Same task
   ↓
Multiple workers
   ↓
Database lock
   ↓
Only valid pending execution proceeds
```

### Worker Failure

```text
PROCESSING
   ↓
Worker failure
   ↓
Stale task detection
   ↓
Recovery
```

### Temporary Task Failure

```text
Failure
 ↓
Retry
 ↓
2s
 ↓
Retry
 ↓
4s
 ↓
Retry
 ↓
8s
```

### Permanent Failure

```text
Retries exhausted
       ↓
FAILED
       ↓
Dead-letter handler
```

---

# 18. Project Status

## Core Implementation: Complete

Implemented and validated:

- FastAPI API
- PostgreSQL persistence
- Celery task execution
- RabbitMQ messaging
- Multiple workers
- Task state machine
- Database row-level locking
- Duplicate-task protection
- Retry mechanism
- Exponential backoff
- Dead-letter handling
- Crash recovery
- Structured logging
- Prometheus metrics
- Streamlit dashboard
- Docker Compose environment

The project is ready for final production-hardening and deployment work.

---

# 19. Future Improvements

Potential production-hardening improvements include:

- RabbitMQ native Dead Letter Exchanges
- Transactional Outbox
- Alembic database migrations
- Worker container running as a non-root user
- Kubernetes deployment
- Grafana dashboards
- Alerting
- Autoscaling
- Authentication and RBAC
- More comprehensive automated tests
- Load testing and performance benchmarking

---

# 20. Author

**Subham**

Backend Engineering | Distributed Systems | High Performance Systems
