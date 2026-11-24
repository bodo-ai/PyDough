"""
Internal QDAG marker identifying the end of a CROSS right-hand operand.
"""

__all__ = ["CrossRHSBoundary"]

from .augmenting_child_operator import AugmentingChildOperator
from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm


class CrossRHSBoundary(AugmentingChildOperator):
    """
    Transparent collection operator marking the end of the RHS of CROSS.

    Preserves the predecessor's terms and cardinality.
    """

    def __init__(self, predecessor: PyDoughCollectionQDAG, cross_id: int):
        super().__init__(predecessor, [])
        self.cross_id: int = cross_id

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.CROSS_RHS_BOUNDARY"

    @property
    def standalone_string(self) -> str:
        return self.preceding_context.standalone_string

    @property
    def tree_item_string(self) -> str:
        return self.preceding_context.tree_item_string

    def to_string(self) -> str:
        # The boundary is an internal bookkeeping node, not a user operator.
        return self.preceding_context.to_string()

    def to_tree_form_isolated(self, is_last: bool) -> CollectionTreeForm:
        return self.preceding_context.to_tree_form_isolated(is_last)

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        return self.preceding_context.to_tree_form(is_last)

    def equals(self, other: object) -> bool:
        return isinstance(other, CrossRHSBoundary) and super().equals(other)
