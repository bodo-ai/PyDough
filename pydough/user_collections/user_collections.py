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

    def __init__(self, name: str, columns: list[str], types: list[PyDoughType]) -> None:
        self._name = name
        self._columns = columns
        self._types = types

    def __eq__(self, other) -> bool:
        return self.equals(other)

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

    @property
    @abstractmethod
    def unique_column_names(self) -> list[str]:
        """Return the set of unique column names in the collection."""

    @abstractmethod
    def always_exists(self) -> bool:
        """Check if the collection is always non-empty."""

    @abstractmethod
    def is_singular(self) -> bool:
        """Returns True if the collection is guaranteed to contain at most one row."""

    @abstractmethod
    def to_string(self) -> str:
        """Return a string representation of the collection."""

    @abstractmethod
    def equals(self, other) -> bool:
        """
        Check if this collection is equal to another collection.
        Two collections are considered equal if they have the same name and columns.
        """

    def get_expression_position(self, expr_name: str) -> int:
        """
        Get the position of an expression in the collection.
        This is used to determine the order of expressions in the collection.
        """
        if expr_name not in self.columns:
            raise ValueError(
                f"Expression {expr_name!r} not found in collection {self.name!r}"
            )
        return self.columns.index(expr_name)
