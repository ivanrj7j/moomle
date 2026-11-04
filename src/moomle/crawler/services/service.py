from abc import ABC, abstractmethod
from typing import Any

from httpx import Response
from sqlalchemy.orm import Session

from moomle.database import Base


class Service(ABC):
    """Abstract base class for all crawler extraction and ingestion services.

    Each service takes an HTTP response, extracts relevant information during `run()`,
    stores the extracted data internally on `self.data`, and provides an `upload(session)`
    method to persist records into the database and return the uploaded data.
    """

    def __init__(self, response: Response):
        """Initializes the service with an HTTP response and table model.

        Args:
            response: HTTP response object containing the page payload and metadata.
        """
        super().__init__()

        self.response = response
        self.table = self._initTable()
        self.data: Any = None
        self.doc: Any = None

    @abstractmethod
    def _initTable(self) -> type[Base] | None:
        """Initializes and returns the SQLAlchemy model class associated with this service.

        Returns:
            The SQLAlchemy model class or None if no direct table maps to this service.
        """
        pass

    @abstractmethod
    def run(self):
        """Extracts data from self.response and stores it internally in self.data."""
        pass

    def upload(self, session: Session) -> Any:
        """Uploads stored data to the database using the provided session and table model.

        Default implementation instantiates self.table with self.data, adds it
        to the session, commits, and returns the persisted model instance.
        Child classes can override this method for custom upload logic.

        Args:
            session: Active SQLAlchemy database session.

        Returns:
            The persisted model instance (self.doc) or None if no table/data exists.
        """
        if self.table is None or self.data is None:
            return None

        self.doc = self.table(**self.data)
        session.add(self.doc)
        session.commit()
        return self.doc
