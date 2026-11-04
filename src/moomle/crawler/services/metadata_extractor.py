from time import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from httpx import URL

from moomle.crawler.services.service import Service
from moomle.database.models import PageMetadata


class MetadataExtractorService(Service):
    """Service responsible for extracting page metadata (title, description, favicon, timestamp)."""

    def _initTable(self) -> type[PageMetadata]:
        """Initializes and returns the PageMetadata SQLAlchemy model.

        Returns:
            The PageMetadata model class.
        """
        return PageMetadata

    @staticmethod
    def _safeGetText(soup: BeautifulSoup, selector: str) -> str | None:
        """Safely extracts stripped text content of a matched CSS selector.

        Args:
            soup: Parsed BeautifulSoup HTML document.
            selector: CSS selector string to query.

        Returns:
            Extracted text string or None if element is not found.
        """
        element = soup.select_one(selector)
        if element:
            return element.text

        return None

    @staticmethod
    def _getFaviconUrl(soup: BeautifulSoup, baseUrl: URL) -> str | None:
        """Finds and resolves the favicon URL from HTML link tags.

        Args:
            soup: Parsed BeautifulSoup HTML document.
            baseUrl: Base URL of the page used to resolve relative paths.

        Returns:
            Absolute favicon URL string, or None if no favicon link is present.
        """
        favicon = soup.select_one('link[rel="icon"]')

        if favicon:
            url = favicon.get("href")
            if not url:
                return None

            parsedUrl = urlparse(url)

            if parsedUrl.scheme:
                return url

            return urljoin(f"{baseUrl.scheme}://{baseUrl.host}", url)

        return None

    def run(self):
        """Extracts page metadata from the response and stores it in self.data."""
        url = self.response.url

        content = self.response.content.decode()
        soup = BeautifulSoup(content, "html.parser")

        title = MetadataExtractorService._safeGetText(soup, "title")
        description = MetadataExtractorService._safeGetText(soup, 'meta[name="description"]')
        faviconUrl = MetadataExtractorService._getFaviconUrl(soup, url)
        parsedAt = time()

        self.data = {
            "url": str(url),
            "title": title,
            "description": description,
            "faviconURL": faviconUrl,
            "parsedAt": parsedAt,
        }
