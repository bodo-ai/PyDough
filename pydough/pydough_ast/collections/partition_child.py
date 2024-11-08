"""
TODO: add file-level docstring
"""

__all__ = ["PartitionChild"]


from .child_access import ChildAccess
from .child_operator_child_access import ChildOperatorChildAccess
from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm


class PartitionChild(ChildOperatorChildAccess):
    """
    Special wrapper around a ChildAccess instance that denotes it as a
    reference to the input to a Partition node, for the purposes of
    stringification.
    """

    def __init__(
        self,
        child_access: PyDoughCollectionAST,
        partition_child_name: str,
    ):
        super().__init__(child_access, True)
        self._partition_child_name: str = partition_child_name

    def clone_with_parent(self, new_ancestor: PyDoughCollectionAST) -> ChildAccess:
        return self

    @property
    def partition_child_name(self) -> str:
        """
        The name that the PartitionBy node gives to the ChildAccess.
        """
        return self._partition_child_name

    @property
    def standalone_string(self) -> str:
        return self.partition_child_name

    @property
    def tree_item_string(self) -> str:
        return f"PartitionChild[{self.standalone_string}]"

    def to_tree_form(self) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = CollectionTreeForm(
            self.tree_item_string,
            0,
            has_predecessor=True,
            has_children=True,
            has_successor=not self.is_last,
        )
        return CollectionTreeForm(
            self.tree_item_string, predecessor.depth + 1, predecessor=predecessor
        )
