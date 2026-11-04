from httpx import AsyncClient, Response

from moomle.crawler.services import MetadataExtractorService
from moomle.database import Base
from moomle.database.connection import SessionLocal


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
            
            yield service.table(**result)

    def _uploadData(self, data:tuple[Base]):
        with SessionLocal() as session:
            for doc in data:
                session.add(doc)
                session.commit()

    async def crawl(self):
        response = await self._sendRequest()
        data = tuple(self._runServices(response))
        self._uploadData(data)

        return data

if __name__ == "__main__":
    import asyncio

    crawler = AsyncCrawler("https://github.com/github/gitignore/blob/main/Python.gitignore")
    asyncio.run(crawler.crawl())