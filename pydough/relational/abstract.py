"""
TODO: Add file description.
"""

from abc import ABC, abstractmethod


class Relational(ABC):
    """
    TODO: Add docstring.
    """

    @property
    @abstractmethod
    def inputs(self):
        """
        TODO: Add docstring + type annotations.
        """

    @property
    @abstractmethod
    def traits(self):
        """
        TODO: Add docstring + type annotations.
        """

    @property
    @abstractmethod
    def columns(self):
        """
        TODO: Add docstring + type annotations.
        """

    @abstractmethod
    def to_sql_glot(self):
        """
        TODO: Add docstring + type annotations.
        """

    @abstractmethod
    def to_string(self):
        """
        TODO: Add docstring + type annotations.
        """

    @abstractmethod
    def can_merge(other: "Relational") -> bool:
        """
        TODO: Add docstring + type annotations.
        """

    @abstractmethod
    def merge(self, other: "Relational") -> "Relational":
        """
        TODO: Add docstring + type annotations.
        """


class Expression(ABC):
    """
    TODO: Add docstring.
    """

    @abstractmethod
    def to_string(self):
        """
        TODO: Add docstring + type annotations.
        """
