"""
Base definition of PyDough QDAG collection type for accesses to a user defined
collection of the current context.
"""

from abc import ABC, abstractmethod

from pydough.types.pydough_type import PyDoughType

__all__ = ["PyDoughUserGeneratedCollection"]


class PyDoughUserGeneratedCollection(ABC):
    """
    Abstract base class for a user defined table collection.
    This class defines the interface for accessing a user defined table collection
    directly, without any specific implementation details.
    It is intended to be subclassed by specific implementations that provide
    the actual behavior and properties of the collection.
    """

    def __init__(self, name: str, columns: list[str]) -> None:
        self._name = name
        self._columns = columns

    def __eq__(self, other) -> bool:
        return isinstance(other, PyDoughUserGeneratedCollection) and repr(self) == repr(
            other
        )

    def __repr__(self) -> str:
        return self.to_string()

    def __hash__(self) -> int:
        return hash(repr(self))

    def __str__(self) -> str:
        return self.to_string()

    @property
    def name(self) -> str:
        """Return the name used for the collection."""
        return self._name

    @property
    def columns(self) -> list[str]:
        """Return column names."""
        return self._columns

    @property
    @abstractmethod
    def column_names_and_types(self) -> list[tuple[str, PyDoughType]]:
        """Return column names and their types."""

    @abstractmethod
    def is_empty(self) -> bool:
        """Check if the collection is empty."""

    @abstractmethod
    def to_string(self) -> str:
        """Return a string representation of the collection."""
