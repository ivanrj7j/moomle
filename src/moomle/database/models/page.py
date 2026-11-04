from datetime import datetime

from sqlalchemy import Float, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from moomle.database.base import Base


class Page(Base):
    __tablename__ = "pages"

    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    title: Mapped[str | None] = mapped_column(String(512))
    content: Mapped[str | None] = mapped_column(Text)
    crawled_at: Mapped[float] = mapped_column(
        Float,
        default=datetime.now,
    )

    page_metadata_id: Mapped[int] = mapped_column(
        ForeignKey("page_metadata.id"),
        nullable=False
    )
    #FOR FOREIGN KEYS ALWAYS WRITE <TABLE_NAME>_<KEY_NAME>