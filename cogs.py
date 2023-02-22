import asyncio
import logging
from typing import Any, Callable, Optional, Tuple


logging.getLogger().setLevel(logging.INFO)


class Registry:

  class RegistryFullException(Exception):
    pass

  def __init__(self, max: int=10):
    self.__registry = []
    self.__max = max
  
  def add(self, item: Any) -> Any:
    if self.length == self.max:
      raise Registry.RegistryFullException()

    logging.info(f"Registry Length: {len(self.__registry)}")
    self.__registry.append(item)
    return self.__registry[-1]

  @property
  def full(self):
    return self.length == self.max

  @property
  def length(self):
    return len(self.__registry)

  @property
  def max(self):
    return self.__max



class Transformer:
  NOOP = lambda x: x

  def __init__(self, fn_raw: Callable=Optional[None], fn: Optional[Callable]=None, fn_sink: Optional[Callable]=None):
    self.fn_raw = fn_raw if fn_raw else Transformer.NOOP
    self.fn = fn if fn else Transformer.NOOPE
    self.fn_sink = fn_sink if fn_sink else Transformer.NOOP


class Crawler:
  """Crawler is responsible for instantiating Producers and Consumers, and -- importantly -- managing their
  execution and termination."""

  def __init__(self, source_max: int=10, sink_max: int=10, max_depth: int=3, max_units_to_process: int=100):
    self.source = asyncio.Queue(source_max)
    self.sink = asyncio.PriorityQueue(sink_max)
    self.max_depth = max_depth
    self.max_units_to_process = max_units_to_process

    self.producers = dict() # Producer(source=self.source, sink=self.sink, max_depth=max_depth)
    self.consumers = dict() # Consumer(source=self.sink, sink=self.source)
    self.registry = Registry()

  def producer(self, id: int, fn_raw: Optional[Callable]=None, fn: Optional[Callable]=None, fn_sink: Optional[Callable]=None, terminate: Callable=(lambda x: False)):
    producer = Producer(
      id=id,
      transformer=Transformer(
        fn_raw=fn_raw,
        fn=fn,
        fn_sink=fn_sink,
      ),
      registry=self.registry,
      source=self.source,
      sink=self.sink,
      terminate=terminate,
    )

    index = len(self.producers)
    self.producers[index] = {
      "producer": producer
    }

    # task = asyncio.create_task(producer.pipe(fn_raw=fn_raw, fn=fn, fn_sink=fn_sink))
    # self.producers[index]["task"] = task

    # return task
    return producer

  def consumer(self, id: int, fn_raw: Callable=(lambda x: x), fn: Callable=(lambda x: x), fn_sink: Callable=(lambda x: x), terminate: Callable=(lambda x: False)):
    consumer = Consumer(
      id=id,
      registry=self.registry,
      source=self.sink,
      sink=self.source,
      terminate=terminate,
      transformer=Transformer(
        fn_raw=fn_raw,
        fn=fn,
        fn_sink=fn_sink,
      ),
    )

    index = len(self.consumers)
    self.consumers[index] = {
      "consumer": consumer
    }

    # task = asyncio.create_task(consumer.pull(fn_raw=fn_raw, fn=fn, fn_sink=fn_sink))
    # self.consumers[index]["task"] = task

    # return task
    return consumer

  def seed_source(self, input_: Tuple[int, Any]):
    self.source.put_nowait(input_)

class Producer:
  """Producer is responsible for generating arbitrary streams of data and placing it onto a provided queue.
  Producer is designed to execute indefinitely until terminated by a parent Crawler."""

  def __init__(self, id: int, source: asyncio.PriorityQueue, sink: asyncio.Queue, transformer: Transformer, registry: Registry, terminate: Callable=(lambda x: False)):
    self.id = id
    self.registry = registry
    self.source = source
    self.sink = sink
    self.terminate = terminate
    self.transformer = transformer

  async def pipe(self):
    while True:
      # Read from Source
      raw_element = await self.source.get()
      logging.info(f"Got '{raw_element}' from source.")
      # Process
      logging.info(f"Processing...")
      result = self.transformer.fn(self.transformer.fn_raw(raw_element))
      logging.info(f"Input processed.")
      # Put on Sink
      logging.info(f"Placing processed input on sink...")
      await self.sink.put(self.transformer.fn_sink(result))
      logging.info(f"Placed processed input on sink.")
      self.source.task_done()
      # Terminate if Done
      if self.terminate(self, raw_element): # raw_element[0] == self.max_depth and self.source.empty():
        logging.info(f"Terminating Producer #{self.id}")
        break

class Consumer:
  """Consumer is responsible for accepting and processing arbitrary streams of data from a provided queue.
  Consumer is designed to execute indefinitely until terminated by a parent Crawler."""

  def __init__(self, source: asyncio.Queue, sink: asyncio.PriorityQueue, registry: Registry, transformer: Transformer, terminate: Callable, id: int):
    self.id = id
    self.registry = registry
    self.source = source
    self.sink = sink
    self.terminate = terminate
    self.transformer = transformer
  
  async def pull(self):
    while True:
      # Read from Source
      raw_element = await self.source.get()
      if not raw_element:
        continue
      await asyncio.sleep(0.3)
      logging.warning(f"Got '{raw_element}' from source.")
      # Process
      logging.warning("Processing...")
      result = self.transformer.fn(self.transformer.fn_raw(raw_element))
      logging.warning("Input processed.")
      # Put on Sink
      logging.warning("Placing processed input on sink...")
      await self.sink.put(self.transformer.fn_sink(result))
      logging.warning("Placed processed input on sink.")

      self.source.task_done()
      if self.terminate(self, raw_element):
        logging.warning(f"Terminating Consumer #{self.id}")
        break
      else:
        self.registry.add(raw_element)


async def main():
  crawler = Crawler()
  # producer = Producer()
  for i in range(10):
    logging.info(f"Seeding source with {i}...")
    if i == 9:
      crawler.seed_source((3, i))
    else:
      crawler.seed_source((0, i))

  producers = [crawler.producer(id=i, fn=lambda tup: tup[-1], terminate=lambda self, _: self.registry.full) for i in range(2)]
  consumers = [crawler.consumer(id=i, fn=lambda num: num, fn_sink= lambda num: (10, num), terminate=lambda self, _: self.registry.full) for i in range(2)]
  logging.info(consumers[0])
  logging.info(producers[0])

  ptasks = [asyncio.create_task(producer.pipe()) for producer in producers]
  ctasks = [asyncio.create_task(consumer.pull()) for consumer in consumers]

  await asyncio.gather(*ptasks)
  await crawler.sink.join()

  # for c in consumers:
  #   c.cancel()

  # await asyncio.gather(
  #   crawler.produce(fn=lambda tup: tup[-1], terminate=lambda self, element: element[0] == self.max_depth and self.source.empty()),
  #   crawler.consume(fn=lambda num: num, fn_sink= lambda num: (3, num))
  # )
  # consumer_task = asyncio.create_task(crawler.consume(fn=lambda num: num, fn_sink= lambda num: (3, num)))
  # producer_task = crawler.produce(fn=lambda tup: tup[-1], terminate=lambda self, element: element[0] == self.max_depth and self.source.empty())

  # print(producer_task)
  # print(consumer_task)

  # await asyncio.gather(producer_task, consumer_task)
  # await crawler.source.join()

  # await asyncio.gather(producer_task)
  # await crawler.sink.join()
  # await crawler.produce(fn=lambda tup: tup[-1])
  # await crawler.consume(fn=lambda num: num)
  # await asyncio.gather(crawler.source.join(), crawler.sink.join())


  # t1 = asyncio.create_task(Producer.pipe(fn=(lambda tup: tup[1])))
  # await t1


if __name__ == '__main__':
  asyncio.run(main())

  # crawler = Crawler(base_url=...)
  # crawler.crawl(max_depth=3)