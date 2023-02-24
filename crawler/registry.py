"""The Registry class is used to keep track of the entities processed by the Crawler's producers and consumers."""
from typing import Any
import logging

from events import CrawlerEvents
from event_bus import EventBus


class Registry:

  class RegistryFullException(Exception):
    pass

  def __init__(self, event_bus: EventBus, max: int=100):
    self.__registry = []
    self.__overflow = []
    self.__max = max
    self.__event_bus = event_bus
  
  def add(self, item: Any) -> bool:
    to_return =False 
    if self.length == self.max:
      logging.info(f"Registry Overflowed.\n- Registry Length: {len(self.__registry)}\n- Overflow Length: {len(self.__overflow)}")
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
    return self.length == self.max

  @property
  def length(self):
    return len(self.__registry)

  @property
  def max(self):
    return self.__max

  @property
  def head(self):
    return self.__registry[0]

  @property
  def tail(self):
    return self.__registry[-1]