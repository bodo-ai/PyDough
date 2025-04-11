"""
Definition of PyDough QDAG collection type for ordering the current collection
by certain collation keys and picking the top records according to the
ordering.
"""

__all__ = ["TopK"]


from functools import cache

from .collection_qdag import PyDoughCollectionQDAG
from .order_by import OrderBy


class TopK(OrderBy):
    """
    The QDAG node implementation class representing a TOP K clause.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionQDAG,
        children: list[PyDoughCollectionQDAG],
        records_to_keep: int,
    ):
        super().__init__(predecessor, children)
        self._records_to_keep = records_to_keep

    @property
    def records_to_keep(self) -> int:
        """
        The number of rows kept by the TOP K clause.
        """
        return self._records_to_keep

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.TOPK"

    @property
    @cache
    def standalone_string(self):
        collation_str: str = ", ".join([expr.to_string() for expr in self.collation])
        return f"TOP_K({self.records_to_keep}, {collation_str})"

    @property
    def tree_item_string(self) -> str:
        collation_str: str = ", ".join(
            [expr.to_string(True) for expr in self.collation]
        )
        return f"TopK[{self.records_to_keep}, {collation_str}]"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, TopK)
            and self._records_to_keep == other._records_to_keep
            and super().equals(other)
        )
