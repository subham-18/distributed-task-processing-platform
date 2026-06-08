from pydantic import BaseModel


class TaskCreate(BaseModel):
    task_name: str
