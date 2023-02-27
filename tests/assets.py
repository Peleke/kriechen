"""This module contains variables shared by multiple tests, annotated by the test file that inspired their creation."""
from typing import Any, Dict, List, Union

# test_event_bus
TEST_EVENT_NAME = "EVENT NAME"


async def dummy_listener(event: Dict[str, Union[List[Any], Any]]) -> Any:
    """'Dummy' async method that can be added as a listener to an EventBus.

    :param event: Object expected to have "collector" and "element" keys of the following types:
        collector: List[Any]
        element: Any
      The invoking (parent) function shares access to `collector`, and this function appends `element`, allowing
      the parent to verify async invocation of `test_listener` by checking for a increment in `len(collector)`.
    :type event: Dict[str, Any]

    :param element: Arbitrary element to add to `collector`. Exists mostly just to increment `len` count.
    :type element: Any

    :return: Returns last element added to `collector`.
    :rtype: Any
    """
    event["collector"].append(event["element"])
    return event["collector"][-1]
