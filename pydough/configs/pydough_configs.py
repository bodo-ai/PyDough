"""
Definitions of configuration settings for PyDough.
"""

__all__ = ["DayOfWeek", "PyDoughConfigs"]

from enum import Enum
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class DayOfWeek(Enum):
    """
    An enum to represent the day of the week.
    """

    SUNDAY = "SUNDAY"
    MONDAY = "MONDAY"
    TUESDAY = "TUESDAY"
    WEDNESDAY = "WEDNESDAY"
    THURSDAY = "THURSDAY"
    FRIDAY = "FRIDAY"
    SATURDAY = "SATURDAY"

    @property
    def pandas_dow(self) -> int:
        match self:
            case DayOfWeek.SUNDAY:
                return 6
            case DayOfWeek.MONDAY:
                return 0
            case DayOfWeek.TUESDAY:
                return 1
            case DayOfWeek.WEDNESDAY:
                return 2
            case DayOfWeek.THURSDAY:
                return 3
            case DayOfWeek.FRIDAY:
                return 4
            case DayOfWeek.SATURDAY:
                return 5


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

    collation_default_asc = ConfigProperty[bool](True)
    """
    If True, then the collation will default to `ASC`. If False, then the
    collation will default to `DESC`. The default is True.
    """

    propagate_collation = ConfigProperty[bool](False)
    """
    If True, each term, which does not have an explicit collation, will inherit
    the collation (ASC/DESC) from the previous specified term. If False, terms
    without an explicit collation will use the default from
    `collation_default_asc`. The default is False.
    """

    start_of_week = ConfigProperty[DayOfWeek](DayOfWeek.SUNDAY)
    """
    The day of the week that is considered the start of the week. The default
    is Sunday.
    """

    start_week_as_zero = ConfigProperty[bool](True)
    """
    If True, then the week starts at zero. If False, then the week starts at one.
    The default is True.
    """

    def __setattr__(self, name: str, value: Any) -> None:
        if name not in dir(self):
            raise AttributeError(f"Unrecognized PyDough config name: {name}")
        super().__setattr__(name, value)
