"""
TODO: add file-level docstring
"""

__all__ = ["TopK"]


from collections.abc import MutableSequence

from .collection_ast import PyDoughCollectionAST
from .order_by import OrderBy


class TopK(OrderBy):
    """
    The AST node implementation class representing a TOP K clause.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        children: MutableSequence[PyDoughCollectionAST],
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
    def calc_terms(self) -> set[str]:
        return self.preceding_context.calc_terms

    @property
    def standalone_string(self) -> str:
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
            super().equals(other)
            and isinstance(other, TopK)
            and self._records_to_keep == other._records_to_keep
        )
