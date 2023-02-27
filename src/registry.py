"""The Registry class is used to keep track of the entities processed by the Crawler's producers and consumers."""
from typing import Any, List
import logging

from .events import CrawlerEvents
from .event_bus import EventBus


class Registry:
    class RegistryFullException(Exception):
        """Indicates that the registry is full. Better to have this in registry.exceptions, but for simplicity's
        sake..."""

        pass

    def __init__(self, event_bus: EventBus, max: int = 100):
        """Constructor method

        :param event_bus: An instance of EventBus, intended to have been created by a parent Crawler.
        :type event_bus: EventBus

        :param max: Integer representing the largest number of items allowed in the Registry. In effect, this allows
            one to bound the number of items processed by the parent Crawler.
        :type max: int
        """
        self.__registry: List[Any] = []
        self.__overflow: List[Any] = []
        self.__max = max
        self.__event_bus = event_bus

    def add(self, item: Any) -> bool:
        """Adds an item to the underlying registry container.

        :param item: Element to add to underlying registry container.
        :type item: Any

        :return: Returns a boolean indicating whether the registry still has space, or if more items than `self.max`
            have been processed.
        :rtype: bool
        """
        to_return = False
        if self.length == self.max:
            logging.info(
                f"Registry Overflowed.\n- Registry Length: {len(self.__registry)}\n"
                + f"- Overflow Length: {len(self.__overflow)}"
            )
            self.__overflow.append(item)
            to_return = False
        else:
            logging.info(f"Registry Length: {len(self.__registry)}")
            self.__registry.append(item)
            to_return = True

        self.__event_bus.emit(CrawlerEvents.Registry.UPDATED, self)
        return to_return

    @property
    def full(self):
        """Indicates whether the length of the underlying registry container equals `self.max`."""
        return self.length == self.max

    @property
    def length(self):
        """Returns the length of the underlying registry container."""
        return len(self.__registry)

    @property
    def max(self):
        """Returns the intended max length of the underlying registry container."""
        return self.__max

    @property
    def head(self):
        """Returns the first element of the underlying registry container."""
        return self.__registry[0]

    @property
    def tail(self):
        """Returns the last element of the underlying registry container."""
        return self.__registry[-1]
