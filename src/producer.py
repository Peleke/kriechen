"""Producer is responsible for generating arbitrary streams of data and placing it onto a provided queue. Producer is
designed to execute indefinitely until terminated by a parent Crawler."""
from typing import Any, List, Set
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
        """Constructor method

        Creates a new Producer instance.

        :param id: Integer representing the ID of this consumer. Used for logging and termination logic.
        :type id: int

        :param source: asyncio.Queue instance from which to read data to consume.
        :type source: asyncio.Queue

        :param sink: asyncio.Queue instance to which to feed forward transformations of consumed data, e.g., to allow a
            Producer of links to web pages to pass those links forward to the Consumer, so they can be parsed for
            content and further links to be passed back to the Producer.
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
        self.__seen: Set[Any] = set()

    async def run(self):
        """Run the Producer — i.e, read from the `self.source`; process items; and feed back to `self.sink` until
        terminated by parent Crawler."""
        while True:
            try:
                # Read from Source
                raw_element: Any = await self.source.get()
                logging.info(f"Producer #{self.id} got '{raw_element}' from source.")
                # Process
                logging.info(f"Producer #{self.id} processing input...")
                result = await self.transformer.fn(self.transformer.fn_raw(raw_element))
                logging.info(f"Producer #{self.id} done processing input.")
                # Put on Sink
                logging.info(f"Producer #{self.id} placing processed input on sink...")
                await self.spread(results=self.transformer.fn_sink(result))
                logging.info(f"Producer #{self.id} placed processed input on sink.")
                # Complete Processing
                self.source.task_done()
                self.event_bus.emit(
                    CrawlerEvents.Producer.PROCESSED_ITEM,
                    {"producer_id": self.id, "raw_element": raw_element},
                )
            except asyncio.exceptions.CancelledError:
                break

    async def spread(self, results: List[Any]) -> None:
        """Place each link in the list of `results` onto the input queue for currently running Consumers.

        :param results: A list of elements collected from the previously processed element.
        :type results: List[str]

        :return: Void.
        :rtype: None
        """
        for result in results:
            logging.info(result)
            if result not in self.__seen:
                self.__seen.add(result)
                await self.sink.put(result)
