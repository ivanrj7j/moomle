from time import time
from urllib.parse import urldefrag, urljoin, urlparse

from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.orm import Session

from moomle.crawler.services.service import Service
from moomle.database.models import Job, JobStatus


class JobExtractorService(Service):
    """Service responsible for discovering and extracting outgoing job links from HTML pages."""

    def _initTable(self) -> type[Job]:
        """Initializes and returns the Job SQLAlchemy model.

        Returns:
            The Job model class.
        """
        return Job

    @staticmethod
    def _resolveUrl(href: str, baseUrl: str) -> str | None:
        """Resolves relative and protocol-relative URLs against the original page base URL.

        Args:
            href: Raw link URL attribute string.
            baseUrl: Base URL of the page containing the anchor tag.

        Returns:
            Normalized absolute URL string, or None if the link is empty or non-HTTP.
        """
        if not href:
            return None

        cleanHref = href.strip()
        if not cleanHref or cleanHref.startswith(("#", "javascript:", "mailto:", "tel:")):
            return None

        # Strip URL fragments (#section)
        defraggedUrl, _ = urldefrag(cleanHref)
        if not defraggedUrl:
            return None

        parsedUrl = urlparse(defraggedUrl)
        if parsedUrl.scheme:
            if parsedUrl.scheme in ("http", "https"):
                return defraggedUrl
            return None

        return urljoin(baseUrl, defraggedUrl)

    def run(self):
        """Extracts and resolves all valid outgoing URLs from the HTML response into self.data,
        excluding links that match the current page URL.
        """
        content = self.response.text
        soup = BeautifulSoup(content, "html.parser")
        baseUrl = str(self.response.url)
        currentUrl, _ = urldefrag(baseUrl)
        normalizedCurrentUrl = currentUrl.rstrip("/")

        jobs: list[str] = []
        for anchor in soup.find_all("a", href=True):
            resolved = self._resolveUrl(anchor["href"], baseUrl)
            if (
                resolved
                and resolved != currentUrl
                and resolved.rstrip("/") != normalizedCurrentUrl
                and resolved not in jobs
            ):
                jobs.append(resolved)

        self.data = {
            "jobs": jobs
        }

    def upload(self, session: Session) -> list[Job]:
        """Persists newly discovered job URLs into the database, ignoring already existing ones.

        Args:
            session: Active SQLAlchemy database session.

        Returns:
            List of newly created and committed Job instances.
        """
        if not self.data or not self.data.get("jobs"):
            return []

        urls = self.data["jobs"]
        # Deduplicate while preserving order and filter by length
        uniqueUrls = [u for u in dict.fromkeys(urls) if len(u) <= 2048]
        if not uniqueUrls:
            return []

        # Query existing URLs in chunks of 500 to avoid query parameter limit issues
        existingUrls: set[str] = set()
        chunkSize = 500
        for i in range(0, len(uniqueUrls), chunkSize):
            chunk = uniqueUrls[i : i + chunkSize]
            matched = session.scalars(select(Job.url).where(Job.url.in_(chunk))).all()
            existingUrls.update(matched)

        # Instantiate new Job records with explicit status and timestamp
        currentTime = time()
        newJobs = [
            Job(
                url=u,
                status=JobStatus.PENDING,
                last_update=currentTime,
            )
            for u in uniqueUrls
            if u not in existingUrls
        ]

        if newJobs:
            session.add_all(newJobs)
            session.commit()

        self.doc = newJobs
        return newJobs
