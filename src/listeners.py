"""The Listeners class provides a convenience container for the listeners and callbacks relevant to a given
entities pub/sub implementation."""
from typing import Callable, List, Tuple

from event_bus import EventBus


class Listeners:
    def __init__(self, event_bus: EventBus, listeners: List[Tuple[str, Callable]]):
        self.event_bus = event_bus
        self.listeners = {k: v for k, v in listeners}

        for event_name, callback in self.listeners.items():
            setattr(self, str(event_name), callback)
            self.event_bus.add_listener(event_name, callback)

    @property
    def listener_names(self):
        return list(self.listeners.keys())
