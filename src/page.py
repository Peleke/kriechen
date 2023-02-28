"""The Page class provides a way to fetch HTML and parse it with Beautiful Soup."""
from typing import List, Optional
import asyncio

from bs4 import BeautifulSoup
import aiohttp
import bs4


class Page:
    @classmethod
    async def create(cls, url: str, session: aiohttp.ClientSession) -> "Page":
        self = cls(url=url, session=session)
        await self.fetch()
        return self

    def __init__(self, url: str, session: aiohttp.ClientSession):
        self.url = url
        self.session = session
        self.failed = False
        self.response: Optional[aiohttp.client_reqrep.ClientResponse] = None
        self.html: Optional[str] = None
        self.soup: Optional[bs4.BeautifulSoup] = None

    @property
    def links(self) -> List[str]:
        return [] if self.soup is None else [a["href"] for a in getattr(self.soup, "find_all")("a")]

    @property
    def internal_links(self) -> List[str]:
        base_url = self.url.split("/")[2]
        if self.soup is None:
            return []
        return [a["href"] for a in self.soup.find_all("a") if base_url in a["href"]]

    async def fetch(self) -> Optional[aiohttp.client_reqrep.ClientResponse]:
        async with self.session.get(self.url) as response:
            if response.status != 200:
                self.failed = True
            else:
                self.response = response
                self.html = await response.text()
                self.soup = BeautifulSoup(self.html, features="html.parser")
        return self.response


async def main():
    async with aiohttp.ClientSession() as session:
        page = await Page.create(url="https://www.tagesschau.de", session=session)
        print(len(page.links))
        print(len(page.internal_links))
        print(page.internal_links)


if __name__ == "__main__":
    asyncio.run(main())
