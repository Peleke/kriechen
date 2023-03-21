"""The Transformer class contains references to the functions used to transform data being read from
or placed upon a queue."""
from typing import Any, Callable, Optional


class Transformer:
    NOOP: Callable[..., Any] = lambda x: x

    def __init__(
        self,
        fn_raw: Optional[Callable[..., Any]] = None,
        fn: Optional[Callable[..., Any]] = None,
        fn_sink: Optional[Callable[..., Any]] = None,
    ):
        """Constructor method

        :param fn_raw: Callable invoked to transform raw elements taken from queue before passing them to `fn`
            for processing. Can be used to, e.g., separate data from its priority.
        :type fn_raw: Callable[..., Any]

        :param fn: Callable invoked to transform an element read from the queue, e.g., fetch the HTML from a URL.
        :type fn: Callable[..., Any]

        :param fn_sink: Callable invoked to transform the result of processing an element into an output to be placed
            on the sink queue/fed back into the production/consumption loop. E.g., use `fn` to read all text from a
            URL, but `fn_sink` to read all URLs from that page and feed them back into the loop.
        :type fn_sink: Callable[..., Any]
        """
        self.fn_raw = fn_raw if fn_raw else Transformer.NOOP
        self.fn = fn if fn else Transformer.NOOP
        self.fn_sink = fn_sink if fn_sink else Transformer.NOOP
