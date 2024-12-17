"""
Definition of the base class for all PyDough types.
"""

__all__ = ["PyDoughType"]

from abc import ABC, abstractmethod
from typing import Optional


class PyDoughType(ABC):
    """
    The abstract base class describing all PyDough types. Each implementation
    class must define the following:

    - A constructor
    - An eval-friendly `__repr__` such that two PyDough types with the same repr
      must be the same type.
    - `json_string` property
    - `parse_from_string` staticmethod
    """

    def __init__(self):
        raise NotImplementedError(
            f"PyDough type class {self.__class__.__name__} does not have an __init__ defined"
        )

    def __repr__(self):
        raise NotImplementedError(
            f"PyDough type class {self.__class__.__name__} does not have a __repr__ defined"
        )

    def __eq__(self, other):
        return isinstance(other, PyDoughType) and repr(self) == repr(other)

    def __hash__(self):
        return hash(repr(self))

    @property
    @abstractmethod
    def json_string(self) -> str:
        """
        The PyDough type as represented in JSON via a string that can be parsed
        back into an identical type object with parse_from_string.
        """

    @staticmethod
    @abstractmethod
    def parse_from_string(type_string: str) -> Optional["PyDoughType"]:
        """
        Creates a new type object from a string such that its `json_string`
        property reconstructs the original strings. Returns None if the string
        cannot be parsed as the type.
        """
