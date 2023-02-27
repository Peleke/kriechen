import asyncio
from typing import Any, List

import pytest

from assets import dummy_listener, TEST_EVENT_NAME
from src.event_bus import EventBus


class TestEventBus:
    def test_adds_listener(self, event_bus):
        event_bus.add_listener(TEST_EVENT_NAME, dummy_listener)

        assert len(event_bus.listeners)
        assert dummy_listener in event_bus.listeners[TEST_EVENT_NAME]

    def test_removes_listener(self, event_bus):
        event_bus.add_listener(TEST_EVENT_NAME, dummy_listener)

        assert len(event_bus.listeners)
        assert dummy_listener in event_bus.listeners[TEST_EVENT_NAME]

        event_bus.remove_listener(TEST_EVENT_NAME, dummy_listener)

        assert not len(event_bus.listeners)
        assert not event_bus.listeners.get(TEST_EVENT_NAME)

    @pytest.mark.asyncio
    async def test_fires_listener(self, event_bus):
        collector: List[Any] = []
        input_data = {"collector": collector, "element": "data"}

        event_bus.add_listener(TEST_EVENT_NAME, dummy_listener)
        event_bus.emit(TEST_EVENT_NAME, input_data)
        await asyncio.sleep(0.1)

        assert len(input_data["collector"]) == 1
        assert input_data["collector"][0] == "data"
