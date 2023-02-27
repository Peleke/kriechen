"""The Listeners class provides a convenience container for the listeners and callbacks relevant to a given
entities pub/sub implementation."""
from typing import Any, Callable, List, Tuple

from .event_bus import EventBus


class Listeners:
    """The Listeners class is simply a container for the functions a parent class wishes to associate with an
    EventBus. It provides no significant architectural benefits, but rather serves to help organize data and
    simplify the parent's constructor."""

    def __init__(self, event_bus: EventBus, listeners: List[Tuple[str, Callable[..., Any]]]):
        """Constructor method

        :param event_bus: An EventBus instance, meant to be instantiated by the parent manager.
        :type event_bus: EventBus

        :param listeners: List of tuples specifying which listeners to associate with which event names.
        :type listeners: List[Tuple[str, Callable[..., Any]]]
        """
        self.event_bus = event_bus
        self.listeners = {k: v for k, v in listeners}

        for event_name, callback in self.listeners.items():
            setattr(self, str(event_name), callback)
            self.event_bus.add_listener(event_name, callback)

    @property
    def event_names(self):
        """Returns a list of event names against which listeners are registered."""
        return list(self.listeners.keys())
