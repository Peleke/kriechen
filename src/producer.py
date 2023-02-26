"""Producer is responsible for generating arbitrary streams of data and placing it onto a provided queue. Producer is
designed to execute indefinitely until terminated by a parent Crawler."""
import logging
import asyncio

from .events import CrawlerEvents
from .event_bus import EventBus
from .transformer import Transformer


class Producer:
    def __init__(
        self,
        id: int,
        source: asyncio.Queue,
        sink: asyncio.Queue,
        transformer: Transformer,
        event_bus: EventBus,
    ):
        self.id = id
        self.event_bus = event_bus
        self.source = source
        self.sink = sink
        self.transformer = transformer

    async def run(self):
        while True:
            try:
                # Read from Source
                raw_element = await self.source.get()
                logging.info(f"Got '{raw_element}' from source.")
                # Process
                logging.info("Processing...")
                logging.error(self.transformer.fn)
                result = self.transformer.fn(self.transformer.fn_raw(raw_element))
                logging.info("Input processed.")
                # Put on Sink
                logging.info("Placing processed input on sink...")
                await self.sink.put(self.transformer.fn_sink(result))
                logging.info("Placed processed input on sink.")
                # Complete Processing
                self.source.task_done()
                self.event_bus.emit(
                    CrawlerEvents.Producer.PROCESSED_ITEM,
                    {"producer_id": self.id, "raw_element": raw_element},
                )
            except asyncio.exceptions.CancelledError:
                break
