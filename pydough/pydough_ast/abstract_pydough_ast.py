"""
TODO: add file-level docstring
"""

__all__ = ["PyDoughAST"]

from abc import ABC, abstractmethod


class PyDoughAST(ABC):
    """
    Base class used for PyDough collection, expression, and opertor AST
    nodes. Mostly exists for isinstance checks & type annotation.
    """

    def __eq__(self, other):
        return self.equals(other)

    def __hash__(self):
        return hash(id(self))

    @abstractmethod
    def equals(self, other: object) -> bool:
        """
        Returns true if two PyDoughAST objects are equal.

        Args:
            `other`: the candidate object being compared to `self`.

        Returns:
            Whether `other` is equal to `self`.
        """
