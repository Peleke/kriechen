"""The Consumer class is responsible for accepting and processing arbitrary streams of data from a provided queue.
Consumer is designed to execute indefinitely until terminated by a parent Crawler."""
import logging
from typing import Any, Callable
import asyncio

from events import CrawlerEvents
from event_bus import EventBus
from registry import Registry
from transformer import Transformer

class Consumer:

  def __init__(self, id: int, source: asyncio.Queue, sink: asyncio.Queue, transformer: Transformer, event_bus: EventBus):
    self.id = id
    self.event_bus = event_bus
    self.source = source
    self.sink = sink
    self.transformer = transformer
  
  async def run(self):
    draining = False
    while True:
      try:
        # Read from Source
        raw_element = await self.source.get()
        logging.info(f"Consumer #{self.id} got '{raw_element}' from source.")

        if raw_element:
          logging.info(f"Consumer #{self.id} processing raw input...")
          result = self.transformer.fn(self.transformer.fn_raw(raw_element))
          logging.info(f"Consumer #{self.id} processed raw input.")

          # Put on Sink
          if not draining:
            logging.info(f"Consumer #{self.id} placing processed input on sink...")
            await self.sink.put(self.transformer.fn_sink(result))
            logging.info(f"Consumer #{self.id} placed processed input on sink.")
          else:
            logging.info(f"Consumer #{self.id} is draining, not putting additional items on queue...")

        self.source.task_done()
        self.event_bus.emit(CrawlerEvents.Consumer.PROCESSED_ITEM, {"consumer_id": self.id, "raw_element": raw_element})
      except asyncio.exceptions.CancelledError:
        break
      