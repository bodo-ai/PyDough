"""
TODO: add file-level docstring
"""

from abc import ABC, abstractmethod


class PyDoughAST(ABC):
    """
    TODO: add class docstring
    """

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            f"{self.__class__.__name__} does not have a constructor defined"
        )

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
