"""Crawler is responsible for instantiating Producers and Consumers, and -- importantly -- managing their execution and
termination."""
from typing import Any, Callable, Dict, Tuple
import asyncio
import logging

from .consumer import Consumer
from .events import CrawlerEvents
from .event_bus import EventBus
from .listeners import Listeners
from .producer import Producer
from .registry import Registry
from .transformer import Transformer


class Crawler:
    @classmethod
    def create(
        cls,
        base_url: str,
        consumer_transformer: Transformer,
        producer_transformer: Transformer,
        terminate: Callable,
        init: bool = True,
        producer_count: int = 10,
        consumer_count: int = 10,
        sink_max: int = 10,
        source_max: int = 10,
    ):
        self = cls(
            base_url=base_url,
            consumer_count=consumer_count,
            consumer_transformer=consumer_transformer,
            producer_count=producer_count,
            producer_transformer=producer_transformer,
            source_max=source_max,
            sink_max=sink_max,
            terminate=terminate,
        )

        if init:
            self.generate_producers()
            self.generate_consumers()

    def __init__(
        self,
        base_url: str,
        consumer_transformer: Transformer,
        producer_transformer: Transformer,
        consumer_count: int = 10,
        producer_count: int = 10,
        max_links: int = 10,
        sink_max: int = 10,
        source_max: int = 10,
        terminate: Callable = (lambda *_: False),
    ):
        self.base_url = base_url
        self.max_links = max_links

        self.consumer_transformer = consumer_transformer
        self.consumer_count = consumer_count
        self.consumers: Dict[int, Dict[str, Any]] = dict()

        self.producer_count = producer_count
        self.producer_transformer = producer_transformer
        self.producers: Dict[int, Dict[str, Any]] = dict()

        self.source: asyncio.Queue = asyncio.Queue(source_max)
        self.sink: asyncio.Queue = asyncio.Queue(sink_max)
        self.terminate = terminate

        self.event_bus = EventBus()
        self.registry = Registry(event_bus=self.event_bus, max=max_links)
        self.__shutting_down = False

        self.destructor = self.Destructor(
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
            ],
        )

    #############
    # THROWAWAY #
    #############
    def seed_source(self, input_: str):
        self.source.put_nowait(input_)

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
                "raw_element": self.registry.tail,
            },
        )
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

        # Seed
        self.seed_source(input_=self.base_url)

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

    ###################
    # TIGHTLY COUPLED #
    ###################
    class Destructor:
        def __init__(
            self,
            crawler: "Crawler",
            event_bus: EventBus,
            terminate: Callable,
            drain_timeout: int = 1,
        ):
            self.crawler = crawler
            self.drain_timeout = drain_timeout
            self.event_bus = event_bus
            self.terminate = terminate

        async def shutdown(self, _: Any) -> None:
            self.terminate_workers()
            drain_tasks = [asyncio.create_task(self.drain(q)) for q in ["sink", "source"]]
            self.crawler.shut_down()
            await asyncio.gather(*drain_tasks)

        async def watch(self, event: Any) -> None:
            logging.info("Checking if done...")
            logging.info(self.crawler.done)
            if self.terminate(self.crawler, event):
                self.event_bus.emit(CrawlerEvents.Crawler.TERMINATE)

        async def drain(self, queue_name: str) -> None:
            queue = getattr(self.crawler, queue_name)
            while True:
                try:
                    _, pending = await asyncio.wait([asyncio.create_task(queue.get())], timeout=self.drain_timeout)
                    if pending:
                        for pending_task in pending:
                            pending_task.cancel()
                    if getattr(self.crawler, queue_name)._unfinished_tasks > 0:
                        queue.task_done()
                except ValueError as e:
                    logging.warning(
                        msg="Got error while draining Queue",
                        exc_info=e,
                    )
                    break

        def terminate_workers(self, producers: bool = True, consumers: bool = True):
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
