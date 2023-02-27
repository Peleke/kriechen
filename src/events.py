"""This module contains all events emitted and recognized throughout the package."""
from enum import Enum


class CrawlerEvents:
    class Consumer(str, Enum):
        """Events associated with the Consumer. Currently, the only events are:
        - `PROCESSED_ITEM`: Indicates that a Consumer has finished transforming and processing a Queue item.
        """

        PROCESSED_ITEM = "CONSUMER_PROCESSED_ITEM"

    class Producer(str, Enum):
        """Events associated with the Producer. Currently, the only events are:
        - `PROCESSED_ITEM`: Indicates that a Producer has finished transforming and processing a Queue item.
        """

        PROCESSED_ITEM = "PRODUCER_PROCESSED_ITEM"

    class Registry(str, Enum):
        """Events associated with the Registry. Currently, the only events are:
        - `REGISTRY_UPDATED`: Indicates that an entity has added an element to the Registry.
        """

        UPDATED = "REGISTRY_UPDATED"

    class Crawler(str, Enum):
        """Events associated with the Crawler. Currently, the only events are:
        - `TERMINATE`: Indicates that the Crawler's termination condition has been reached, and should be stopped.
        """

        TERMINATE = "TERMINATE_CRAWLER"
