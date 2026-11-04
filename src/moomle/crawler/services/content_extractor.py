from time import time

from bs4 import BeautifulSoup

from moomle.crawler.services.metadata_extractor import MetadataExtractorService
from moomle.crawler.services.service import Service


class ContentExtractorService(Service):
    """Service responsible for extracting main page content and document title."""

    def _initTable(self):
        """Returns None as page content is persisted into the main Page model."""
        return None

    def run(self):
        """Extracts decoded HTML content, title, and crawl timestamp into self.data."""
        content = self.response.content.decode()
        soup = BeautifulSoup(content, "html.parser")
        title = MetadataExtractorService._safeGetText(soup, "title")
        crawledAt = time()

        self.data = {
            "content": content,
            "title": title,
            "crawled_at": crawledAt,
        }
