import asyncio
import logging
from typing import Any, Callable, Optional, Tuple


logging.getLogger().setLevel(logging.INFO)


class Registry:

  class RegistryFullException(Exception):
    pass

  def __init__(self, max: int=100):
    self.__registry = []
    self.__overflow = []
    self.__max = max
  
  def add(self, item: Any) -> bool:
    if self.length == self.max:
      logging.info(f"Registry Overflowed.\n- Registry Length: {len(self.__registry)}\n- Overflow Length: {len(self.__overflow)}")
      self.__overflow.append(item)
      return False
    else:
      logging.info(f"Registry Length: {len(self.__registry)}")
      self.__registry.append(item)
      return True

  @property
  def full(self):
    return self.length == self.max

  @property
  def length(self):
    return len(self.__registry)

  @property
  def max(self):
    return self.__max

  @property
  def head(self):
    return self.__registry[0]

  @property
  def tail(self):
    return self.__registry[-1]



class Transformer:
  NOOP = lambda x: x

  def __init__(self, fn_raw: Optional[Callable]=None, fn: Optional[Callable]=None, fn_sink: Optional[Callable]=None):
    self.fn_raw = fn_raw if fn_raw else Transformer.NOOP
    self.fn = fn if fn else Transformer.NOOPE
    self.fn_sink = fn_sink if fn_sink else Transformer.NOOP


class Crawler:
  """Crawler is responsible for instantiating Producers and Consumers, and -- importantly -- managing their
  execution and termination."""


  @classmethod
  def create(cls, producer_transformer: Transformer, consumer_transformer: Transformer, producer_terminate: Callable=(lambda *_: False), consumer_terminate: Callable=(lambda *_: False), source_max: int=10, sink_max: int=10, producer_count: int=10, consumer_count: int=10, init: bool=True):
    self = cls(source_max=source_max, sink_max=sink_max, producer_count=producer_count, consumer_count=consumer_count, producer_transformer=producer_transformer, consumer_transformer=consumer_transformer, producer_terminate=producer_terminate, consumer_terminate=consumer_terminate)

    if init:
      self.generate_producers()
      self.generate_consumers()


  def __init__(self, producer_transformer: Transformer, consumer_transformer: Transformer, producer_terminate: Callable, consumer_terminate: Callable, source_max: int=10, sink_max: int=10, producer_count: int=10, consumer_count: int=10):
    self.consumer_transformer = consumer_transformer
    self.consumer_terminate = consumer_terminate
    self.consumer_count = consumer_count

    self.producer_count = producer_count
    self.producer_terminate = producer_terminate
    self.producer_transformer = producer_transformer

    self.source = asyncio.Queue(source_max)
    self.sink = asyncio.Queue(sink_max)

    self.producers = dict() # Producer(source=self.source, sink=self.sink)
    self.consumers = dict() # Consumer(source=self.sink, sink=self.source)
    self.registry = Registry()

  async def crawl(self):
    # Generate producers/consumers
    producers = await self.generate_producers()
    consumers = await self.generate_consumers()

    # Create/Schedule tasks
    for producer_data in producers.values():
      producer_data["task"] = asyncio.create_task(producer_data["producer"].pipe())

    for consumer_data in consumers.values():
      consumer_data["task"] = asyncio.create_task(consumer_data["consumer"].pull())

    # Gather `producers`` + Join Consumer Input Queue
    await asyncio.gather(*self.producer_tasks)
    await self.sink.join()

    # Cancel Hanging Consumers
    for index, ctask in enumerate(self.consumer_tasks):
      logging.info(f"Canceling Consumer #{index}...")
      ctask.cancel()

  def producer(self, id: int, transformer: Transformer, terminate: Callable=(lambda *_: False)):
    producer = Producer(
      id=id,
      transformer=transformer,
      registry=self.registry,
      source=self.source,
      sink=self.sink,
      terminate=terminate,
    )

    self.producers[producer.id] = {
      "producer": producer,
      "task": None,
    }

    return producer

  async def generate_producers(self):
    while len(self.producers) < self.producer_count:
      self.producer(
        id=len(self.producers),
        transformer=self.producer_transformer,
        terminate=self.producer_terminate,
      )
    return self.producers

  @property
  def producer_tasks(self):
    return [p["task"] for p in self.producers.values()]

  def consumer(self, id: int, transformer: Transformer, terminate: Callable=(lambda *_: False)):
    consumer = Consumer(
      id=id,
      registry=self.registry,
      source=self.sink,
      sink=self.source,
      terminate=terminate,
      transformer=transformer,
    )

    self.consumers[consumer.id] = {
      "consumer": consumer,
      "task": None,
    }

    return consumer

  async def generate_consumers(self):
    while len(self.consumers) < self.consumer_count:
      self.consumer(
        id=len(self.consumers),
        transformer=self.consumer_transformer,
        terminate=self.consumer_terminate,
      )
    return self.consumers

  @property
  def consumer_tasks(self):
    return [c["task"] for c in self.consumers.values()]

  def seed_source(self, input_: Tuple[int, Any]):
    self.source.put_nowait(input_)

class Producer:
  """Producer is responsible for generating arbitrary streams of data and placing it onto a provided queue.
  Producer is designed to execute indefinitely until terminated by a parent Crawler."""

  def __init__(self, id: int, source: asyncio.Queue, sink: asyncio.Queue, transformer: Transformer, registry: Registry, terminate: Callable=(lambda *_: False)):
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
      logging.error(self.transformer.fn)
      result = self.transformer.fn(self.transformer.fn_raw(raw_element))
      logging.info(f"Input processed.")
      # Put on Sink
      logging.info(f"Placing processed input on sink...")
      await self.sink.put(self.transformer.fn_sink(result))
      logging.info(f"Placed processed input on sink.")
      self.source.task_done()
      # Terminate 
      if self.registry.full or self.terminate(self, raw_element): # raw_element[0] == self.max_depth and self.source.empty():
        logging.info(f"Producer #{self.id} draining queues...")
        await self.__drain(all=True)
        logging.info(f"Terminating Producer #{self.id}")
        break

  async def __drain(self, all: bool=False, sink: bool=False, source: bool=False):
    queues = []
    if all:
      queues = [self.source, self.sink]
    elif sink:
      queues = [self.sink]
    elif source:
      queues = [self.source]
    
    while True:
      for queue in queues:
        try:
          _ = await asyncio.wait_for(queue.get(), timeout=1)
          queue.task_done()
        except TimeoutError:
          return

class Consumer:
  """Consumer is responsible for accepting and processing arbitrary streams of data from a provided queue.
  Consumer is designed to execute indefinitely until terminated by a parent Crawler."""

  def __init__(self, source: asyncio.Queue, sink: asyncio.Queue, registry: Registry, transformer: Transformer, terminate: Callable, id: int):
    self.id = id
    self.registry = registry
    self.source = source
    self.sink = sink
    self.terminate = terminate
    self.transformer = transformer
  
  async def pull(self):
    draining = False
    while True:
      # Read from Source
      raw_element = await self.source.get()
      logging.warning(f"Consumer #{self.id} got '{raw_element}' from source.")
      if not raw_element:
        self.source.task_done()
        continue
      else:
        _ = await self.__process(raw_element, draining=draining)
        self.source.task_done()

      if not self.registry.add(raw_element) or self.terminate(self, raw_element):
        draining = True
        logging.warning(f"Spinning down Consumer #{self.id}, now just draining...")

        if self.source.empty():
          logging.warning(f"Terminating Consumer #{self.id}.")
          break
      
  async def tick(self):
    while True:
      await asyncio.sleep(1)
      logging.info(f"Length of Consumer.source: {self.source.qsize()}")
      logging.info(f"Length of Consumer.sink: {self.sink.qsize()}")

  async def __process(self, raw_element: Any, draining: bool) -> Any:
    # Process
    logging.warning(f"Consumer #{self.id} processing raw input...")
    result = self.transformer.fn(self.transformer.fn_raw(raw_element))
    logging.warning(f"Consumer #{self.id} processed raw input.")

    # Put on Sink
    if not draining:
      logging.warning(f"Consumer #{self.id} placing processed input on sink...")
      await self.sink.put(self.transformer.fn_sink(result))
      logging.warning(f"Consumer #{self.id} placed processed input on sink.")
    else:
      logging.warning(f"Consumer #{self.id} is draining, not putting additional items on queue...")

    return result


async def main():
  crawler = Crawler(
    consumer_transformer=Transformer(
      fn=lambda num: num,
      fn_sink=lambda num: (10, num),
    ),
    consumer_terminate=(lambda self, _: self.registry.full),
    producer_transformer=Transformer(
      fn=lambda tup: tup[-1],
    ),
    producer_terminate=(lambda self, _: self.registry.full),
    source_max=100,
    sink_max=100,
  )

  for i in range(90):
    logging.info(f"Seeding source with {i}...")
    if i == 9:
      crawler.seed_source((3, i))
    else:
      crawler.seed_source((0, i))

  await crawler.crawl()

if __name__ == '__main__':
  asyncio.run(main())