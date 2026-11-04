from enum import Enum
from time import time

from sqlalchemy import Enum as SQLEnum
from sqlalchemy import Float, String
from sqlalchemy.orm import Mapped, mapped_column

from moomle.database.base import Base


class JobStatus(str, Enum):
    PENDING = "PENDING"
    DONE = "DONE"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(String(2048), unique=True, nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        SQLEnum(JobStatus, name="job_status"),
        default=JobStatus.PENDING,
        nullable=False,
    )
    last_update: Mapped[float | None] = mapped_column(
        Float,
        default=time,
        nullable=True,
    )
