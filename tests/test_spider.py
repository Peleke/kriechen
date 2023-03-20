import aiohttp
import pytest

from src.spider import Spider


class TestSpider:
    @pytest.mark.asyncio
    async def test_spider(self):
        async with aiohttp.ClientSession() as session:
            spider = await Spider.create(url="https://www.megacorpone.com/", session=session, max_links=100)

            results = await spider.crawl()

            assert len(results.contents) == 100
