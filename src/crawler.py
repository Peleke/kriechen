"""Crawler is responsible for instantiating Producers and Consumers, and -- importantly -- managing their execution and
termination."""
from typing import Any, Callable, Dict
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
        """Factory method

        The `create` method is used to create a new Crawler and generate its Producers/Consumers in one shot.

        :param base_url: The initial 'seed' URL used to kick off crawling.
        :type base_url: str

        :param consumer_transformer: The Transformer instance used to create Consumers.
        :type consumer_transformer: Transformer

        :param producer_transformer: The Transformer instance used to create Producers.
        :type producer_transformer: Transformer

        :param terminate: The function used to determine when the Crawler should initiate shutdown.
        :type terminate: Callable

        :param init: Boolean indicating whether to generate Producers/Consumers upon creation.
        :type init: bool

        :param producer_count: Number of Producers to configure.
        :type producer_count: int

        :param consumer_count: Number of Consumers to configure.
        :type consumer_count: int

        :param sink_max: Maximum number of items that cana be placed in underlying `sink` queue.
        :type sink_max: int

        :param source_max: Maximum number of items that cana be placed in underlying `source` queue.
        :type source_max: int

        :return: Crawler instance. If `init == True`, the returned instance will have its `producers`
            and `consumers` properties populated immediately.
        :rtype: Crawler
        """
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
        """Constructor method"""
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
    def seed_source(self, input_: str) -> None:
        """Place an initial 'seed' element onto the source queue to initiate crawling.

        :param input_: Initial element to place on queue.
        :type input_: str
        """
        self.source.put_nowait(input_)

    ##########################
    # METADATA & BOOKKEEPING #
    ##########################
    @property
    def done(self) -> bool:
        """Boolean representing whether underlying registry is full.

        :return: `True` if `self.registry` is full, `False` otherwise.
        :rtype: bool
        """
        return self.registry.full

    @property
    def shutting_down(self) -> bool:
        """Boolean representing whether Crawler is in the process of draining its queues and shutting down.

        :return: `True` if Crawler is draining and shutting down, `False` otherwise.
        :rtype: bool
        """
        return self.__shutting_down

    def shut_down(self) -> bool:
        """Method to initiate a shutdown. This flags `self.__shutting_down` as `True`, and begins draining the
        `self.source` and `self.sink` queues.

        :return: Returns the value of `self.__shutting_down`, which will always be `True` after invoking this method.
        :rtype: bool
        """
        self.__shutting_down = True
        return self.__shutting_down

    async def update_registry(self, element: Any) -> Any:
        """Meethod to add an element to the underlying registry and emit an event indicating that the registry has
        been updated. The event is 'heard' by the Destructor, which initiates shutdown when it is full.

        :param element: Element added to registry.
        :type element: Any

        :return: Returns the element added to the underlying Registry.
        :rtype: Any
        """
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
    async def crawl(self) -> None:
        """This method initiates the crawling process. It:
        - Generates a series of Producers and Consumers
        - Seeds the input queue
        - Initiates the Producer/Consumer Tasks
        - Waits for Producers/Consumers to finish, _or_ for Crawler to initiate shutdown
        - Waits for `Queue.join` to complete
        - Cancels hanging Consumers
        """
        # Generate producers/consumers
        producers = await self.generate_producers()
        consumers = await self.generate_consumers()

        # Seed Queue
        self.seed_source(input_=self.base_url)

        # Create/Schedule tasks
        async with asyncio.TaskGroup() as tg:
            for agents, agent_type in [(producers, "producer"), (consumers, "consumer")]:
                for data in agents.values():
                    data["task"] = tg.create_task(data[agent_type].run())

        # Gather `producers` + Join Consumer Input Queue
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
        """Returns a list of Producer tasks."""
        return [p["task"] for p in self.producers.values()]

    async def generate_producers(self):
        """Generate Producers. These are 'placeholders' used by `crawl` to initiate the actual Production process."""
        while len(self.producers) < self.producer_count:
            self.producer(
                id=len(self.producers),
                transformer=self.producer_transformer,
            )
        return self.producers

    def producer(self, id: int, transformer: Transformer) -> Producer:
        """Create a Producer instance using the provided `transformer`. All Producers generated by a given `Crawler`
        will use the same `transformer`.

        :param transformer: A Transformer instance used to configure the generated Producer.
        :type transformer: Transformer

        :return: An instance of Producer configured with the provided `transformer`.
        :rtype: Producer
        """
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
        """Returns a list of Consumer tasks."""
        return [c["task"] for c in self.consumers.values()]

    async def generate_consumers(self):
        """Generate Consumers. These are 'placeholders' used by `crawl` to initiate the actual Consumption process."""
        while len(self.consumers) < self.consumer_count:
            self.consumer(
                id=len(self.consumers),
                transformer=self.consumer_transformer,
            )
        return self.consumers

    def consumer(self, id: int, transformer: Transformer):
        """Create a Consumer instance using the provided `transformer`. All Consumers generated by a given `Crawler`
        will use the same `transformer`.

        :param transformer: A Transformer instance used to configure the generated Consumer.
        :type transformer: Transformer

        :return: An instance of Consumer configured with the provided `transformer`.
        :rtype: Consumer
        """
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
            """Constructor method

            The `Destructor` must be configured with:
            - A parent Crawler
            - The parent Crawler's `event_bus` and `terminate` properties
            - A `drain_timeout` indicating how long to wait for async `put` methods to compelte before terminating
              the drain

            The Destructor's methods are invoked via event emissions rather than imperative invocation.
            """
            self.crawler = crawler
            self.drain_timeout = drain_timeout
            self.event_bus = event_bus
            self.terminate = terminate

        async def shutdown(self, _: Any) -> None:
            """Initiate a shutdown on the parent Crawler by terminating all running workers and draining the
            underlying queues.

            :param _: A generic parameter used to accept the Event, which, by this implementation, remains unused.
            :type _: Any
            """
            self.terminate_workers()
            drain_tasks = [asyncio.create_task(self.drain(q)) for q in ["sink", "source"]]
            self.crawler.shut_down()
            await asyncio.gather(*drain_tasks)

        async def watch(self, event: Any) -> None:
            """A method invoked on eveery `CrawlerEvents.Registry.UPDATED` event, and used to determine if the crawler
            is done. If so, it emits `CrawlerEvents.Crawler.TERMINATE` to trigger a shutdown on the parent Crawler.

            :param event: The event triggering invocation of `watch`.
            :type event: Any
            """
            logging.info(self.crawler.done)
            if self.terminate(self.crawler, event):
                self.event_bus.emit(CrawlerEvents.Crawler.TERMINATE)

        async def drain(self, queue_name: str) -> None:
            """Removes all items from the underlying sink/soure queues to trigger completion of the `Queue.join`
            call in the parent Crawler and terminate the run.

            :param queue_name: Name of the queue to drain. Should be either `"source"` or `"sink"`.
            :type queue_name: str
            """
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
                    logging.error(
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
