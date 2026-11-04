from time import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from httpx import URL

from moomle.crawler.services.service import Service
from moomle.database.models import PageMetadata


class MetadataExtractorService(Service):
    def _initTable(self):
        return PageMetadata
    
    def _safeGetText(self, soup:BeautifulSoup, selector:str):
        element = soup.select_one(selector)
        if element:
            return element.text

        return None

    def _getFaviconURL(self, soup:BeautifulSoup, baseURL:URL):
        favicon = soup.select_one('link[rel="icon"]')

        if favicon:
            url = favicon.get("href")
            if not url:
                return None
            
            parsedURL = urlparse(url)

            if parsedURL.scheme:
                return url
            
            return urljoin(f"{baseURL.scheme}://{baseURL.host}", url)

        return None
    
    def run(self):
        url = self.response.url

        content = self.response.content.decode()
        soup = BeautifulSoup(content, "html.parser")

        title = self._safeGetText(soup, "title")
        description = self._safeGetText(soup, 'meta[name="description"]')
        faviconURL = self._getFaviconURL(soup, url)
        parsedAt = time()

        return {
            "url": str(url),
            "title": title,
            "description":description,
            "faviconURL":faviconURL,
            "parsedAt":parsedAt
        }
        