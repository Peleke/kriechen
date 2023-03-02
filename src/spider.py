"""SiteCrawler uses Crawler to instantiate Producers and Consumers specifically designed to generate links
and read their content."""
from typing import List
import asyncio
import logging

import aiohttp

from .crawler import Crawler
from .page import Page
from .transformer import Transformer


class Spider:
    @classmethod
    async def create(cls, url: str, session: aiohttp.ClientSession, max_links: int = 100) -> "Spider":
        return cls(
            url=url,
            session=session,
            max_links=max_links,
            crawler=Crawler(
                base_url=url,
                consumer_transformer=Transformer(
                    fn=lambda url: asyncio.create_task(Spider.__create_page(url=url, session=session)),
                    fn_sink=(lambda page: [page.url]),
                ),
                producer_transformer=Transformer(
                    fn=lambda url: asyncio.create_task(Spider.__create_page(url=url, session=session)),
                    fn_sink=(lambda page: page.internal_links),
                ),
                terminate=(lambda self, _: self.done),
                max_links=max_links,
            ),
        )

    @staticmethod
    async def __create_page(url: str, session: aiohttp.ClientSession) -> Page:
        """..."""
        return await Page.create(url=url, session=session)

    def __init__(self, url: str, session: aiohttp.ClientSession, crawler: Crawler, max_links: int = 100):
        self.url = url
        self.max_links = max_links
        self.pages: List[Page] = []
        self.__session = session
        self.__crawler = crawler

    async def crawl(self) -> List[Page]:
        """..."""
        await self.__crawler.crawl()
        return self.pages

    def add_page(self, page: Page) -> Page:
        """Add a page to the list of processed pages.

        :param page: A Page to append to the list.
        :type page: Page

        :return: Returns the appended page.
        :rtype: Page
        """
        self.pages.append(page)
        return self.pages[-1]


async def main():
    async with aiohttp.ClientSession() as session:
        spider = await Spider.create(url="https://www.tagesschau.de", session=session, max_links=10)
        res = await spider.crawl()
        print(res)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    asyncio.run(main())
