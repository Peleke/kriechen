"""Crawler is responsible for instantiating Producers and Consumers, and -- importantly -- managing their execution and
termination."""
from typing import Any, Callable, List, Tuple
import asyncio
import logging

from consumer import Consumer
from events import CrawlerEvents
from event_bus import EventBus
from producer import Producer
from registry import Registry
from transformer import Transformer


class Listeners:

  def __init__(self, event_bus: EventBus, listeners: List[Tuple[str, Callable]]):
    self.event_bus = event_bus
    self.listeners = {k:v for k, v in listeners}

    for event_name, callback in self.listeners.items():
      setattr(self, str(event_name), callback)
      self.event_bus.add_listener(event_name, callback)

  @property
  def listener_names(self):
    return list(self.listeners.keys())


class Crawler:

  @classmethod
  def create(cls, producer_transformer: Transformer, consumer_transformer: Transformer, producer_terminate: Callable=(lambda *_: False), consumer_terminate: Callable=(lambda *_: False), source_max: int=10, sink_max: int=10, producer_count: int=10, consumer_count: int=10, init: bool=True):
    self = cls(source_max=source_max, sink_max=sink_max, producer_count=producer_count, consumer_count=consumer_count, producer_transformer=producer_transformer, consumer_transformer=consumer_transformer, producer_terminate=producer_terminate, consumer_terminate=consumer_terminate)

    if init:
      self.generate_producers()
      self.generate_consumers()

  def __init__(self, producer_transformer: Transformer, consumer_transformer: Transformer, terminate: Callable=(lambda *_: False), source_max: int=10, sink_max: int=10, producer_count: int=10, consumer_count: int=10):
    self.consumer_transformer = consumer_transformer
    self.consumer_count = consumer_count
    self.consumers = dict()

    self.producer_count = producer_count
    self.producer_transformer = producer_transformer
    self.producers = dict()

    self.source = asyncio.Queue(source_max)
    self.sink = asyncio.Queue(sink_max)
    self.terminate = terminate

    self.event_bus = EventBus()
    self.registry = Registry(event_bus=self.event_bus)
    self.__shutting_down = False

    self.destructor = Destructor(
      crawler=self,
      event_bus=self.event_bus,
      terminate=self.terminate,
    )
    self.listeners = Listeners(
      event_bus=self.event_bus,
      listeners=[
        (CrawlerEvents.Producer.PROCESSED_ITEM, self.update_registry),
        (CrawlerEvents.Consumer.PROCESSED_ITEM, self.update_registry),
        (CrawlerEvents.Registry.UPDATED, self.destructor.watch),
        (CrawlerEvents.Crawler.TERMINATE, self.destructor.shutdown),
      ]
    )

  ##########################
  # METADATA & BOOKKEEPING #
  ##########################
  @property
  def done(self) -> bool:
    return self.registry.full

  @property
  def shutting_down(self) -> bool:
    return self.__shutting_down

  def shut_down(self) -> bool:
    self.__shutting_down = True
    return self.__shutting_down

  async def update_registry(self, element: Any) -> Any:
    self.registry.add(element)
    self.event_bus.emit(
      CrawlerEvents.Registry.UPDATED,
      {
        "registry": self.registry,
        "operation": "add",
        "raw_element": self.registry.tail
    })
    return self.registry.tail

  ###############
  # MAIN METHOD #
  ###############
  async def crawl(self):
    # Generate producers/consumers
    producers = await self.generate_producers()
    consumers = await self.generate_consumers()

    # Create/Schedule tasks
    for agents, agent_type in [(producers, "producer"), (consumers, "consumer")]:
      for data in agents.values():
        data["task"] = asyncio.create_task(data[agent_type].run())

    # Gather `producers` + Join Consumer Input Queue
    await asyncio.gather(*self.producer_tasks)
    await self.sink.join()

    # Cancel Hanging Consumers
    if not self.__shutting_down:
      for index, ctask in enumerate(self.consumer_tasks):
        logging.info(f"Terminating Consumer #{index}...")
        ctask.cancel()


  ##################
  # PRODUCER LOGIC #
  ##################
  @property
  def producer_tasks(self):
    return [p["task"] for p in self.producers.values()]

  async def generate_producers(self):
    while len(self.producers) < self.producer_count:
      self.producer(
        id=len(self.producers),
        transformer=self.producer_transformer,
      )
    return self.producers

  def producer(self, id: int, transformer: Transformer):
    producer = Producer(
      id=id,
      transformer=transformer,
      event_bus=self.event_bus,
      source=self.source,
      sink=self.sink,
    )

    self.producers[producer.id] = {
      "producer": producer,
      "task": None,
    }

    return producer

  ##################
  # CONSUMER LOGIC #
  ##################
  @property
  def consumer_tasks(self):
    return [c["task"] for c in self.consumers.values()]

  async def generate_consumers(self):
    while len(self.consumers) < self.consumer_count:
      self.consumer(
        id=len(self.consumers),
        transformer=self.consumer_transformer,
      )
    return self.consumers

  def consumer(self, id: int, transformer: Transformer):
    consumer = Consumer(
      id=id,
      event_bus=self.event_bus,
      source=self.sink,
      sink=self.source,
      transformer=transformer,
    )

    self.consumers[consumer.id] = {
      "consumer": consumer,
      "task": None,
    }

    return consumer

  #############
  # THROWAWAY #
  #############
  def seed_source(self, input_: Tuple[int, Any]):
    self.source.put_nowait(input_)

class Destructor:

  def __init__(self, crawler: Crawler, event_bus: EventBus, terminate: Callable):
    self.crawler = crawler
    self.event_bus = event_bus
    self.terminate = terminate

  async def shutdown(self, _: Any) -> None:
    self.__terminate_workers()
    drain_tasks = [asyncio.create_task(self.__drain(q)) for q in ["sink", "source"]]
    self.crawler.shut_down()
    await asyncio.gather(*drain_tasks)

  async def watch(self, event: Any) -> None:
    if self.terminate(self.crawler, event):
      self.event_bus.emit(CrawlerEvents.Crawler.TERMINATE)

  async def __drain(self, queue_name: str) -> None:
    queue = getattr(self.crawler, queue_name)
    while True:
        try:
          _ = await asyncio.wait_for(queue.get(), timeout=1)
          queue.task_done()
        except TimeoutError:
          break

  def __terminate_workers(self, producers: bool=True, consumers: bool=True):
    if self.crawler.shutting_down:
      return
    else:
      if producers:
        for producer_id, producer_data in self.crawler.producers.items():
          logging.warning(f"Cancelling Producer #{producer_id}...")
          producer_data["task"].cancel()
      if consumers:
        for consumer_id, consumer_data in self.crawler.consumers.items():
          logging.warning(f"Cancelling Consumer #{consumer_id}...")
          consumer_data["task"].cancel()


async def main():
  crawler = Crawler(
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
    if i == 9:
      crawler.seed_source((3, i))
    else:
      crawler.seed_source((0, i))

  await crawler.crawl()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)

  asyncio.run(main())