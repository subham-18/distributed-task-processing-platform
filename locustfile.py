from locust import HttpUser, task, between


class TaskEngineUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def create_task(self):
        self.client.post("/tasks", json={"task_name": "load-test-task"})
