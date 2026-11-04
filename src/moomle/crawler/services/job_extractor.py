from time import time
from urllib.parse import urldefrag, urljoin, urlparse

from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.orm import Session

from moomle.crawler.services.service import Service
from moomle.database.models import Advertiser, Job, JobStatus


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

    @staticmethod
    def _extractCandidateHostnames(url: str) -> list[str]:
        """Extracts candidate hostnames and parent domains for a given URL.

        For example, 'sub.ad.doubleclick.net' yields:
        ['sub.ad.doubleclick.net', 'ad.doubleclick.net', 'doubleclick.net']

        Args:
            url: The URL to extract domain components from.

        Returns:
            List of candidate hostname strings.
        """
        parsed = urlparse(url)
        hostname = (parsed.hostname or "").lower()
        if not hostname:
            return []

        parts = hostname.split(".")
        candidates: list[str] = []
        for i in range(len(parts) - 1):
            candidates.append(".".join(parts[i:]))

        return candidates

    @staticmethod
    def _filterAdvertisers(urls: list[str], session: Session) -> list[str]:
        """Filters out URLs whose hostname or parent domain is listed in the advertisers table.

        Args:
            urls: List of URL strings to check.
            session: Active SQLAlchemy database session.

        Returns:
            Filtered list of non-advertiser URL strings.
        """
        if not urls:
            return []

        urlCandidateMap: dict[str, list[str]] = {
            u: JobExtractorService._extractCandidateHostnames(u) for u in urls
        }

        allCandidates: set[str] = set()
        for candidates in urlCandidateMap.values():
            allCandidates.update(candidates)

        if not allCandidates:
            return urls

        blockedHosts: set[str] = set()
        candidateList = list(allCandidates)
        chunkSize = 500
        for i in range(0, len(candidateList), chunkSize):
            chunk = candidateList[i : i + chunkSize]
            matched = session.scalars(
                select(Advertiser.hostname).where(Advertiser.hostname.in_(chunk))
            ).all()
            blockedHosts.update(matched)

        if not blockedHosts:
            return urls

        cleanUrls = [
            u
            for u in urls
            if not any(cand in blockedHosts for cand in urlCandidateMap[u])
        ]
        return cleanUrls

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
        """Persists newly discovered non-advertiser job URLs into the database, ignoring already existing ones.

        Args:
            session: Active SQLAlchemy database session.

        Returns:
            List of newly created and committed Job instances.
        """
        if not self.data or not self.data.get("jobs"):
            return []

        urls = self.data["jobs"]
        uniqueUrls = [u for u in dict.fromkeys(urls) if len(u) <= 2048]
        if not uniqueUrls:
            return []

        # Filter out advertiser URLs
        nonAdvertiserUrls = self._filterAdvertisers(uniqueUrls, session)
        if not nonAdvertiserUrls:
            return []

        # Query existing URLs in chunks of 500 to avoid query parameter limit issues
        existingUrls: set[str] = set()
        chunkSize = 500
        for i in range(0, len(nonAdvertiserUrls), chunkSize):
            chunk = nonAdvertiserUrls[i : i + chunkSize]
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
            for u in nonAdvertiserUrls
            if u not in existingUrls
        ]

        if newJobs:
            session.add_all(newJobs)
            session.commit()

        self.doc = newJobs
        return newJobs
