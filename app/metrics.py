from prometheus_client import Counter, Gauge, Histogram

tasks_completed_total = Counter("tasks_completed_total", "Total completed tasks")

tasks_failed_total = Counter("tasks_failed_total", "Total failed tasks")

tasks_retried_total = Counter("tasks_retried_total", "Total retried tasks")

tasks_processing = Gauge(
    "tasks_processing", "Number of tasks currently being processed"
)

task_processing_duration_seconds = Histogram(
    "task_processing_duration_seconds", "Task processing duration in seconds"
)
