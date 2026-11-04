from time import time

from bs4 import BeautifulSoup

from moomle.crawler.services.metadata_extractor import MetadataExtractorService
from moomle.crawler.services.service import Service


class ContentExtractorService(Service):
    def _initTable(self):
        return None

    def run(self):
        content = self.response.content.decode()
        soup = BeautifulSoup(content, "html.parser")
        title = MetadataExtractorService._safeGetText(soup, "title")
        crawled_at = time()

        return {
            "content": content,
            "title": title,
            "crawled_at": crawled_at
        }