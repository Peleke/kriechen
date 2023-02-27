import pytest

from src.event_bus import EventBus


@pytest.fixture(name="event_bus", scope="function")
def event_bus():
    yield EventBus()
