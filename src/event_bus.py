from typing import Any, Callable, Dict, Optional, Set
import asyncio


class EventBus:
    def __init__(self):
        self.listeners: Dict[str, Set[Callable[..., Any]]] = {}

    def add_listener(self, event_name: str, listener: Callable):
        if not self.listeners.get(event_name, None):
            self.listeners[event_name] = {listener}
        else:
            self.listeners[event_name].add(listener)

    def remove_listener(self, event_name: str, listener: Callable):
        self.listeners[event_name].remove(listener)
        if len(self.listeners[event_name]) == 0:
            del self.listeners[event_name]

    def emit(self, event_name: str, event: Optional[Any] = None):
        listeners: Set[Callable[..., Any]] = self.listeners.get(event_name, set())
        for listener in listeners:
            asyncio.create_task(listener(event))
