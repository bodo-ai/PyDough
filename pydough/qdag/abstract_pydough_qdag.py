"""
Base class for all PyDough QDAG nodes, collection or expression.
"""

__all__ = ["PyDoughQDAG"]

from abc import ABC, abstractmethod
from hashlib import sha256


class PyDoughQDAG(ABC):
    """
    Base class used for PyDough collection and expression nodes.
    Mostly exists for isinstance checks & type annotation.
    """

    def __eq__(self, other):
        return self.equals(other)

    def __hash__(self):
        # TODO: investigate whether this is too slow, and if so find something
        # faster that has the same benefits (good hash function, possibly
        # consistent)
        return int(sha256(self.key.encode()).hexdigest(), base=16)

    @property
    @abstractmethod
    def key(self) -> str:
        """
        An string used to distinguish `self` to hash on so that QDAG nodes can
        have cached methods. This key string is not necessarily unique for
        objects even if they are not equal.
        """

    @abstractmethod
    def equals(self, other: object) -> bool:
        """
        Returns true if two PyDoughQDAG objects are equal.

        Args:
            `other`: the candidate object being compared to `self`.

        Returns:
            Whether `other` is equal to `self`.
        """
