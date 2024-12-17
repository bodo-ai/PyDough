"""
Base definition of PyDough collection QDAG classes that access a child context.
"""

__all__ = ["ChildAccess"]


from abc import abstractmethod

from pydough.qdag.expressions.collation_expression import CollationExpression

from .collection_qdag import PyDoughCollectionQDAG


class ChildAccess(PyDoughCollectionQDAG):
    """
    The QDAG node implementation class representing an access to a child node.
    either directly or as a subcollection of another collection.
    """

    def __init__(
        self,
        ancestor: PyDoughCollectionQDAG,
    ):
        self._ancestor: PyDoughCollectionQDAG = ancestor

    @abstractmethod
    def clone_with_parent(self, new_ancestor: PyDoughCollectionQDAG) -> "ChildAccess":
        """
        Copies `self` but with a new ancestor node that presumably has the
        original ancestor in its predecessor chain.

        Args:
            `new_ancestor`: the node to use as the new parent of the clone.

        Returns:
            The cloned version of `self`.
        """

    @property
    def ancestor_context(self) -> PyDoughCollectionQDAG:
        return self._ancestor

    @property
    def preceding_context(self) -> PyDoughCollectionQDAG | None:
        return None

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return None

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ChildAccess)
            and self.ancestor_context == other.ancestor_context
        )
