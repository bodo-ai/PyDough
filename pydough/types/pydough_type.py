"""
TODO: add file-level docstring
"""

from abc import ABC, abstractmethod


class PyDoughType(ABC):
    """
    TODO: add class docstring
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
        raise hash(repr(self))

    @abstractmethod
    def as_json_string(self) -> str:
        """
        TODO: add function docstring
        """

    @abstractmethod
    def parse_from_string(type_string: str) -> "PyDoughType":
        """
        TODO: add function docstring
        """
