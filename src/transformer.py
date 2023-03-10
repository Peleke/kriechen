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
        self.fn_raw = fn_raw if fn_raw else Transformer.NOOP
        self.fn = fn if fn else Transformer.NOOP
        self.fn_sink = fn_sink if fn_sink else Transformer.NOOP
