from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from moomle.database.base import Base


class Advertiser(Base):
    """SQLAlchemy model for blocked advertiser hostnames."""

    __tablename__ = "advertisers"

    id: Mapped[int] = mapped_column(primary_key=True)
    hostname: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        nullable=False,
        index=True,
    )
