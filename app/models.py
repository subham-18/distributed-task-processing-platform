from sqlalchemy import Column, DateTime, Integer, String
from app.database import Base
from datetime import datetime


class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True)

    task_name = Column(String, nullable=False)

    status = Column(String, default="PENDING")

    time_stamp = Column(DateTime, default=datetime.utcnow)

    updated_at = Column(DateTime, default=datetime.utcnow)

    retry_count = Column(Integer, default=0)
