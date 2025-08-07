# utils/retryConfig.py

import time
from functools import wraps
from typing import (
    Any,
    Callable,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

F = TypeVar("F", bound=Callable[..., Any])

class RetryConfig:
    """
    Configurable retry decorator with exponential backoff.

    Usage:

        from utils.retryConfig import RetryConfig

        @RetryConfig.decorator(max_attempts=5, backoff=2.0, exceptions=(IOError,))
        def flaky_call(...):
            ...

    Or:

        retry = RetryConfig(max_attempts=4, backoff=1.5)
        @retry
        def another_op(...):
            ...
    """

    def __init__(
        self,
        max_attempts: int = 3,
        backoff: float = 1.0,
        exceptions: Union[Type[BaseException], Tuple[Type[BaseException], ...]] = Exception,
    ) -> None:
        self.max_attempts = max_attempts
        self.backoff = backoff
        if isinstance(exceptions, tuple):
            self.exceptions: Tuple[Type[BaseException], ...] = exceptions
        else:
            self.exceptions = (exceptions,)

    def __call__(self, func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except self.exceptions:
                    attempt += 1
                    if attempt >= self.max_attempts:
                        raise
                    time.sleep(self.backoff * (2 ** (attempt - 1)))
        return cast(F, wrapper)

    @staticmethod
    def decorator(
        max_attempts: int = 3,
        backoff: float = 1.0,
        exceptions: Union[Type[BaseException], Tuple[Type[BaseException], ...]] = Exception,
    ) -> Callable[[F], F]:
        return RetryConfig(max_attempts, backoff, exceptions)