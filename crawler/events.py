"""This module contains all events emitted and recognized throughout the package."""
import enum


class CrawlerEvents:
    class Consumer(enum.Enum):
        PROCESSED_ITEM = "CONSUMER_PROCESSED_ITEM"

    class Producer(enum.Enum):
        PROCESSED_ITEM = "PRODUCER_PROCESSED_ITEM"

    class Registry(enum.Enum):
        UPDATED = "REGISTRY_UPDATED"

    class Crawler(enum.Enum):
        TERMINATE = "TERMINATE_CRAWLER"
