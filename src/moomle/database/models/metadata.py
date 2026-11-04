from datetime import datetime

from sqlalchemy import Float, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from moomle.database.base import Base


class PageMetadata(Base):
    __tablename__ = "page_metadata"

    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(String, nullable=False)
    title: Mapped[str | None] = mapped_column(String, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    faviconURL: Mapped[str | None] = mapped_column(String, nullable=True)
    parsedAt: Mapped[float] = mapped_column(Float, nullable=False)