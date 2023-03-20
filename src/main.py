"""A simple entry point to test Crawler and such."""
import asyncio
import logging
import sys

import uvloop

from .crawler import Crawler
from .transformer import Transformer


async def main(base_url: str):
    crawler = Crawler(
        base_url=base_url,
        consumer_transformer=Transformer(
            fn=lambda num: num,
            fn_sink=lambda num: (10, num),
        ),
        producer_transformer=Transformer(
            fn=lambda tup: tup[-1],
        ),
        terminate=((lambda self, _: self.done)),
        source_max=10,
        sink_max=10,
    )

    for i in range(10):
        logging.info(f"Seeding source with {i}...")
        crawler.seed_source(base_url)

    await crawler.crawl()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    if sys.version_info >= (3, 11):
        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            try:
                runner.run(main(base_url="https://tagesschau.de"))
            except asyncio.exceptions.CancelledError:
                pass
    else:
        uvloop.install()
        try:
            asyncio.run(main())
        except asyncio.exceptions.CancelledError:
            pass
