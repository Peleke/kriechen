import aiohttp
import asyncio
import logging
import sys
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup as bs
import uvloop


logging.getLogger().setLevel(logging.INFO)


class Content:

  async def get_text(self, url: str) -> Optional[str]:
    async with aiohttp.ClientSession() as session:
      async with session.get(url) as response:
        if response.status == 200:
          return await response.text()
        else:
          logging.error(f"Response failed with status code: '{response.status}'")

  async def get_article_links(self, refresh: bool=False) -> List[str]:
    if not self.article_links or refresh:
      anchors = []
      if response := await self.get_text(self.url):
        anchors = bs(response, features="html.parser").find_all("a")
        links = [a.get("href") for a in anchors if "https"]
        self.article_links = [l for l in links if l is not None and TagesSchau.HOME in l and l.endswith("html")]
    return self.article_links


class Article(Content):

  @classmethod
  async def create(cls, url: str):
    logging.info(f"Initializing Article...")
    self = cls(url=url)

    async with aiohttp.ClientSession() as session:
      async with session.get(self.url) as response:
        if response.status == 200:
          self.html = await response.text()
          self.soup = bs(self.html, features="html.parser")
          self.text = self.get_paragraphs()
          self.article_links = await self.get_article_links()
        else:
          e = aiohttp.ClientEerror(f"Received non-20x Response: {response.status}")
          logging.error(exc_info=e)
          raise e

    return self

  def __init__(self, url: str):
    self.url = url
    self.article_links = None
    self.html = None
    self.soup = None
    self.text = None

  def get_paragraphs(self) -> List[str]:
    return [p.get_text().replace("\n", "").strip() for p in self.soup.find_all("p")]


class TagesSchau(Content):

  HOME = "https://www.tagesschau.de"

  @classmethod
  async def create(cls, with_article_links: bool=False, with_categories: bool=True):
    logging.info("Initializing...")
    self = cls(url=TagesSchau.HOME)

    if with_article_links:
      self.article_links = await self.get_article_links()
      logging.info("Initialized with articles.")
    if with_categories:
      self.categories = await self.get_categories()
      logging.info("Initialized with categories.")

    logging.info("Initialization complete.")
    return self
  
  def __init__(self, url: Optional[str]=None):
    self.url = url if url else TagesSchau.HOME
    self.article_links = None
    self.categories = None

  async def get_categories(self, refresh: bool=False) -> Dict[str, Any]:
    if not self.categories or refresh:
      relevant = [l.lower() for l in (await self.get_article_links(refresh=False)) if l.startswith("https") and "tagesschau.de" in l]
      category_rows = [l.split("/")[3:] for l in relevant if len(l.split("/")[3:]) > 2]

      category_map = {}
      for row in category_rows:
        category, subcategory = row[:2]
        if category not in category_map:
          category_map[category] = []
        if subcategory != "" and subcategory not in category_map[category]:
          category_map[category].append(subcategory)
      self.categories = category_map
    return self.categories

  async def articles_worker(self, article_link_queue: asyncio.Queue, articles: List[str], id: int, urls: set, max_depth: int=5):
    while True:
      try:
        message = await asyncio.wait_for(article_link_queue.get(), timeout=5)
      except TimeoutError as e:
        logging.error('Hit timeout', exc_info=e)
        break

      url, depth = message[0], message[1]
      logging.info(f"depth: {depth} :: max_depth: {max_depth}")

      try:
        if url not in urls:
          urls.add(url)
          article = await self.__process_article_links(url=url, article_link_queue=article_link_queue, id=id)

        if depth < max_depth:
          logging.info('Adding new links to queue...')
          asyncio.create_task(
            self.add_to_queue(
              article_link_queue=article_link_queue,
              article_links=article.article_links,
              depth=depth
            )
          )
          logging.info('New links added.')

        articles.append(article)
        article_link_queue.task_done()
      except aiohttp.ClientError as e:
        logging.error(exc_info=e)

      logging.info(f"Article received: {article}")

  
  async def add_to_queue(self, article_link_queue: asyncio.Queue, article_links: List[str], depth: int):
    [await article_link_queue.put((l, depth+1)) for l in article_links]


  async def __process_article_links(self, url: str, article_link_queue: asyncio.Queue, id: int) -> None:
    logging.info(f"Fetcher #{id} getting '{url}'...")

    article = await Article.create(url=url)
    logging.info(f"Article created: {article}")

    return article

      # [await article_link_queue.put(l) for l in article.article_links]

async def feed_queue(queue: asyncio.Queue, article_links: List[str]):
  while len(article_links):
    logging.info(f"Links remaining: {len(article_links)}")
    await queue.put((article_links.pop(), 0))
    logging.info(article_links)


async def main():
  article: str = "https://www.tagesschau.de/ausland/europa/russland-ukraine-krieg-nato-sicherheitskonferenz-101.html"
  # res = await get_text(article)
  # paragraphs = TagesSchau.article_paragraphs(await get_text(article))
  import pprint
  ts = await TagesSchau.create(with_article_links=True, with_categories=True)
  queue = asyncio.Queue(10)
  link_producer = asyncio.create_task(feed_queue(queue, ts.article_links[:1]))

  articles = []
  pending = [asyncio.create_task(ts.articles_worker(article_link_queue=queue, articles=articles, id=i, urls=set())) for i in range(10)]
  try:
    # await link_producer
    # await queue.join()
    await asyncio.gather(link_producer, queue.join(), *pending)
    for p in pending:
      logging.warning("Canceling pending task...")
      p.cancel()
      
    # [f.cancel() for f in fetchers]
  except asyncio.exceptions.CancelledError:
    pass

  # await asyncio.gather(link_producer, *fetchers)



  # for i in range(10, 60, 10):

  #   pending = [asyncio.create_task(Article.create(url=ts.article_links[i])) for j in range(i)]
  #   results = []

  #   while pending:
  #     done, pending = await asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED, timeout=10)

  #     for done_task in done:
  #       exc = done_task.exception()
  #       if not exc:
  #         result = await done_task
  #         logging.info(result)
  #         results.append(result)
  #       else:
  #         logging.error(exc_info=exc)

  #     for index, pending_task in enumerate(pending, start=1):
  #       print(f"Cancelling pending task #{index}.")
  #       pending_task.cancel()

  # print(results)


  # tasks = [TagesSchau.article_paragraphs(await ts.get_text(article)) for article in ts.articles]
  # results = asyncio.gather(*tasks)
  # print(results)


if __name__ == "__main__":
  if sys.version_info >= (3, 11):
    with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
      try:
        runner.run(main())
      except asyncio.exceptions.CancelledError:
        pass
  else:
    uvloop.install()
    try:
      asyncio.run(main())
    except asyncio.exceptions.CancelledError:
      pass

# Process articles via queue
# - get text
# - pass to nltk parsing queue
# parse
# - vocab analysis, etc.
# - register processed

# TODO
# Experiment w/ queue logic
# - Producer / Consumer(s) / Orchestrator
# Informal test -- show processes all articles
# Begin architecture/unit tests
# - Sources.TageSchau
# - Content, Article, etc. -- interface/base classes?
# - Queue Wrapper(s)
# - M.APIs, Ch. 7 Data Patterns