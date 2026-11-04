from typing import Any

from httpx import AsyncClient, Response

from moomle.crawler.services import (
    ContentExtractorService,
    JobExtractorService,
    MetadataExtractorService,
)
from moomle.crawler.services.service import Service
from moomle.database.connection import SessionLocal
from moomle.database.models import Page


class AsyncCrawler:
    """Sends an automatic request asynchronously and orchestrates extraction and ingestion services.

    Attributes:
        url: The URL string of the page to be crawled.
        services: List of service classes to instantiate and run during crawling.
    """

    def __init__(self, url: str):
        """Initializes AsyncCrawler with the target URL and registered services.

        Args:
            url: URL of the page to be crawled.
        """
        self.url = url
        self.services = [
            MetadataExtractorService,
            JobExtractorService,
        ]

    async def _sendRequest(self) -> Response:
        """Sends an asynchronous GET request to the target URL.

        Returns:
            The HTTP response object.
        """
        headers = {
            "User-Agent": "moomle/0.1.0 (educational search engine; https://github.com/ivanrj7j/moomle)"
        }
        async with AsyncClient(headers=headers, follow_redirects=True) as client:
            response = await client.get(self.url)

        return response

    def _runServices(self, response: Response):
        """Executes each registered service using the HTTP response.

        Args:
            response: HTTP response to process.

        Yields:
            Executed Service instances containing extracted data in self.data.
        """
        for serviceClass in self.services:
            service = serviceClass(response)
            service.run()

            yield service

    def _uploadPage(self, services: list[Service], content: dict[str, Any]):
        """Persists service data and the main Page record within a single session.

        Passes the active session to each service's upload() method, maps
        resulting foreign keys to page content, and persists the Page record.

        Args:
            services: Executed services to upload.
            content: Extracted page content dictionary.

        Returns:
            The persisted Page model instance.
        """
        with SessionLocal() as session:
            for service in services:
                uploaded = service.upload(session)
                if service.table and uploaded and hasattr(uploaded, "id"):
                    columnName = f"{service.table.__tablename__}_id"
                    if columnName in Page.__table__.columns:
                        content[columnName] = uploaded.id

            doc = Page(**content)
            session.add(doc)
            session.commit()

            return doc

    async def crawl(self) -> list[Service]:
        """Coordinates the crawling workflow: fetches page, runs services, and uploads data.

        Returns:
            List of executed service instances.
        """
        response = await self._sendRequest()
        services = list(self._runServices(response))

        contentExtractor = ContentExtractorService(response)
        contentExtractor.run()
        content = contentExtractor.data or {}
        content["url"] = str(response.url)

        self._uploadPage(services, content)

        return services