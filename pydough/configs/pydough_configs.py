"""
Definitions of configuration settings for PyDough.
"""

__all__ = ["PyDoughConfigs"]

from typing import Any, Generic, TypeVar

T = TypeVar("T")


class ConfigProperty(Generic[T]):
    """
    A type-generic property class to be used as a descriptor inside of the
    PyDoughConfigs class. An invocation of `ConfigProperty` looks as follows:

    ```
    class ClassName:
        ...
        foo = ConfigProperty[str]("")
        bar = ConfigProperty[int](0)
    ```

    In this example, every instance of the class `ClassName` now has two
    properties: `foo` has type `str` and a default of `""`, and `bar`
    has type `int` and has a default value of `0`. Both properties have
    standard getters and setters usable via `.foo` and `.bar`.
    """

    def __init__(self, default: T):
        self._default: T = default

    def __set_name__(self, owner, name):
        self._name = name

    def __get__(self, instance, owner) -> T:
        if instance is None:
            return self._default
        return instance.__dict__.get(self._name, self._default)

    def __set__(self, instance, value: T):
        instance.__dict__[self._name] = value

    def __repr__(self) -> str:
        return f"config:{self._name}"


class PyDoughConfigs:
    """
    Class used to store information about various configuration settings of
    PyDough.
    """

    sum_default_zero = ConfigProperty[bool](True)
    """
    If True, then the `SUM` function always defaults to 0 if there are no
    records to be summed up. If False, the output could be `NULL`. The default
    is True.
    """

    avg_default_zero = ConfigProperty[bool](False)
    """
    If True, then the `AVG` function always defaults to 0 if there are no
    records to be averaged. If False, the output could be `NULL`. The default
    is False.
    """

    def __setattr__(self, name: str, value: Any) -> None:
        if name not in dir(self):
            raise AttributeError(f"Unrecognized PyDough config name: {name}")
        super().__setattr__(name, value)
