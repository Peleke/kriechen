"""This module contains all events emitted and recognized throughout the package."""
import enum


class CrawlerEvents:
    class Consumer(str, enum.Enum):
        PROCESSED_ITEM = "CONSUMER_PROCESSED_ITEM"

    class Producer(str, enum.Enum):
        PROCESSED_ITEM = "PRODUCER_PROCESSED_ITEM"

    class Registry(str, enum.Enum):
        UPDATED = "REGISTRY_UPDATED"

    class Crawler(str, enum.Enum):
        TERMINATE = "TERMINATE_CRAWLER"
