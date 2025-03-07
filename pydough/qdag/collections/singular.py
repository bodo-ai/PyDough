"""
Definition of PyDough QDAG collection type for making the current collection
singular.
"""

__all__ = ["Singular"]


from functools import cache

from .augmenting_child_operator import AugmentingChildOperator
from .collection_qdag import PyDoughCollectionQDAG


class Singular(AugmentingChildOperator):
    """
    The QDAG node implementation class representing a SINGULAR operator.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionQDAG,
    ):
        super().__init__(predecessor, [])

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.SINGULAR"

    @property
    @cache
    def standalone_string(self) -> str:
        return "SINGULAR"

    @property
    def tree_item_string(self) -> str:
        return "Singular"

    def equals(self, other: object) -> bool:
        return super().equals(other) and isinstance(other, Singular)

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        return True
