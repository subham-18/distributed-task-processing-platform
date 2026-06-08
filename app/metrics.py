from prometheus_client import Counter

tasks_completed_total = Counter("tasks_completed_total", "Total completed tasks")

tasks_failed_total = Counter("tasks_failed_total", "Total failed tasks")

tasks_retried_total = Counter("tasks_retried_total", "Total retried tasks")
