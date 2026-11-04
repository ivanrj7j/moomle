from abc import ABC, abstractmethod

from httpx import Response

from moomle.database import Base


class Service(ABC):
    def __init__(self, response:Response):
        super().__init__()

        self.response = response
        self.table = self._initTable()

    @abstractmethod
    def _initTable(self) -> Base:
        pass

    @abstractmethod
    def run(self):
        pass