from typing import Any, Callable, Dict, Optional, Set
import asyncio


class EventBus:
    def __init__(self):
        """Create a new EventBus. Under the hood, an EventBus is just a Dict mapping Event name strings to Sets
        containing async functions registered as listeners."""
        self.listeners: Dict[str, Set[Callable[..., Any]]] = {}

    def add_listener(self, event_name: str, listener: Callable[..., Any]) -> None:
        """This method adds `listener` to the set associated with `event_name`.

        :param event_name: The name of the event with which to associate `listener`.
        :type event_name: str

        :param listener: The function to be called whenever `event_name` is emitted.
        :type listener: Callable[..., Any]

        :return: None
        :rtype: None
        """
        if not self.listeners.get(event_name, None):
            self.listeners[event_name] = {listener}
        else:
            self.listeners[event_name].add(listener)

    def remove_listener(self, event_name: str, listener: Callable[..., Any]) -> None:
        """This method removes `listener` from the set associated with `event_name`, and deletes the `evet_name` key
        if `listener` was the last listener registered to it.

        :param event_name: The name of the event with which `listener` is associated.
        :type event_name: str

        :param listener: The function associated with `event_name` to remove from the list of listeners.
        :type listener: Callable[..., Any]

        :return: None
        :rtype: None
        """
        self.listeners[event_name].remove(listener)
        if len(self.listeners[event_name]) == 0:
            del self.listeners[event_name]

    def emit(self, event_name: str, event: Optional[Any] = None) -> None:
        """Creates an asyncio.Task to invoke every listener associated with `event_name` whenever a publisher invokes
        `emit` (this method) with said `event_name`. Each listener will be passed an optional `event` object containing
        data about the event.

        :param event_name: Name of the event whose listeners to trigger.
        :type event_name: str

        :event: Arbitrary event data.
        :type: Optional[Any]

        :return: None
        :rtype: None
        """
        listeners: Set[Callable[..., Any]] = self.listeners.get(event_name, set())
        for listener in listeners:
            asyncio.create_task(listener(event))
