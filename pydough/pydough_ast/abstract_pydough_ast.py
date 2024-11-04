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

    @abstractmethod
    def equals(self, other: object) -> bool:
        """
        Returns true if two PyDoughAST objects are equal. Implementation
        classes are expected to extend this base check.

        Args:
            `other`: the candidate object being compared to `self`.

        Returns:
            Whether `other` is equal to `self`.
        """
        return type(self) is type(other)
