from typing import Any

from httpx import AsyncClient, Response

from moomle.crawler.services import ContentExtractorService, MetadataExtractorService
from moomle.database import Base
from moomle.database.connection import SessionLocal
from moomle.database.models import Page


class AsyncCrawler:
    """AsyncCrawler
    
    Sends an automatic request asynchronously and orchestrates all the services to extract information

    Keyword arguments:
    url -- url of the page to be crawled
    Return: None
    """
    
    def __init__(self, url:str):
        self.url = url
        self.services = [
            MetadataExtractorService
        ]

    async def _sendRequest(self):
        async with AsyncClient() as client:
            response = await client.get(self.url)

        return response

    def _runServices(self, response:Response):
        for serviceClass in self.services:
            service = serviceClass(response)
            result = service.run()
            table = service.table.__tablename__
            
            yield table, service.table(**result)

    def _uploadData(self, data:tuple[str, Base], content:dict[str, Any]):
        with SessionLocal() as session:
            for table, doc in data:
                session.add(doc)
                session.commit()
                content[f"{table}_id"] = doc.id 

            doc = Page(**content)
            
            session.add(doc)
            session.commit()

    async def crawl(self):
        response = await self._sendRequest()
        data = tuple(self._runServices(response))

        contentExtractor = ContentExtractorService(response)
        content = contentExtractor.run()
        content["url"] = str(response.url)
        
        self._uploadData(data, content)

        return data

if __name__ == "__main__":
    import asyncio

    crawler = AsyncCrawler("https://en.wikipedia.org/wiki/Satoru_Gojo")
    asyncio.run(crawler.crawl())