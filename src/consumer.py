"""The Consumer class is responsible for accepting and processing arbitrary streams of data from a provided queue.
Consumer is designed to execute indefinitely until terminated by a parent Crawler."""
from typing import Any, List
import logging
import asyncio

from .events import CrawlerEvents
from .event_bus import EventBus
from .transformer import Transformer


class Consumer:
    def __init__(
        self,
        id: int,
        source: asyncio.Queue,
        sink: asyncio.Queue,
        transformer: Transformer,
        event_bus: EventBus,
    ):
        """Constructor method

        Creates a new Consumer instance.

        :param id: Integer representing the ID of this consumer. Used for logging and termination logic.
        :type id: int

        :param source: asyncio.Queue instance from which to read data to consume.
        :type source: asyncio.Queue

        :param sink: asyncio.Queue instance to which to feed back transformations of consumed data, e.g., to allow a
            Consumer of content on web pages to pass those links back to the Producer, so they can be scheduled for
            processing by other Consumers.
        :type source: asyncio.Queue

        :param transformer: Transformer instance containing the read, input, and output transformations to use when
            reading from the source, processing raw elements, and writing back to the sink, respectively.

        :param event_bus: EventBus intended to have been created by the parent Crawler. Used to emit messages
            whenever an item has been successfully processed.
        :type event_bus: EventBus
        """
        self.id = id
        self.event_bus = event_bus
        self.source = source
        self.sink = sink
        self.transformer = transformer

    async def run(self):
        """Run the Consumer — i.e, read from the `self.source`; process items; and feed back to `self.sink` until
        terminated by parent Crawler."""
        draining = False
        while True:
            try:
                # Read from Source
                raw_element: Any = await self.source.get()
                logging.info(f"Consumer #{self.id} got '{raw_element}' from source.")

                if raw_element:
                    logging.info(f"Consumer #{self.id} processing raw input...")
                    result = await self.transformer.fn(self.transformer.fn_raw(raw_element))
                    logging.info(f"Consumer #{self.id} processed raw input.")

                    # Put on Sink
                    if not draining:
                        logging.info(f"Consumer #{self.id} placing processed input on sink...")
                        await self.spread(results=self.transformer.fn_sink(result))
                        logging.info(f"Consumer #{self.id} placed processed input on sink.")
                    else:
                        logging.info(f"Consumer #{self.id} is draining, not putting additional items on queue...")

                self.source.task_done()
                self.event_bus.emit(
                    CrawlerEvents.Consumer.PROCESSED_ITEM,
                    {"consumer_id": self.id, "raw_element": result},
                )
            except asyncio.exceptions.CancelledError:
                break

    async def spread(self, results: List[Any]):
        """...

        :param ...:
        :type ...:
        """
        for result in results:
            logging.info(result)
            await self.sink.put(result)
