"""
Base definition of PyDough QDAG collection types that can contain child
collections that are referenced instead of stepped into.
"""

__all__ = ["ChildOperator"]


from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm


class ChildOperator(PyDoughCollectionQDAG):
    """
    Base class for PyDough collection QDAG nodes that have access to
    child collections, such as CALCULATE or WHERE.
    """

    def __init__(
        self,
        children: list[PyDoughCollectionQDAG],
    ):
        self._children: list[PyDoughCollectionQDAG] = children

    @property
    def children(self) -> list[PyDoughCollectionQDAG]:
        """
        The child collections accessible from the operator used to derive
        expressions in terms of a subcollection.
        """
        return self._children

    def to_tree_form_isolated(self, is_last: bool) -> CollectionTreeForm:
        tree_form: CollectionTreeForm = CollectionTreeForm(
            self.tree_item_string,
            0,
            has_predecessor=True,
        )
        for idx, child in enumerate(self.children):
            child_tree: CollectionTreeForm = child.to_tree_form(
                idx == (len(self.children) - 1)
            )
            tree_form.nested_trees.append(child_tree)
        return tree_form
