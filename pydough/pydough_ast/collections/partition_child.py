"""
TODO: add file-level docstring
"""

__all__ = ["PartitionChild"]


from pydough.pydough_ast.expressions.collation_expression import CollationExpression

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
        ancestor: PyDoughCollectionAST,
    ):
        super(ChildOperatorChildAccess, self).__init__(ancestor)
        self._child_access = child_access
        self._is_last = True
        self._partition_child_name: str = partition_child_name
        self._ancestor: PyDoughCollectionAST = ancestor

    def clone_with_parent(self, new_ancestor: PyDoughCollectionAST) -> ChildAccess:
        return PartitionChild(
            self.child_access, self.partition_child_name, new_ancestor
        )

    @property
    def partition_child_name(self) -> str:
        """
        The name that the PartitionBy node gives to the ChildAccess.
        """
        return self._partition_child_name

    @property
    def key(self) -> str:
        return f"{self.ancestor_context.key}.{self.partition_child_name}"

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return self._child_access.ordering

    @property
    def standalone_string(self) -> str:
        return self.partition_child_name

    def to_string(self) -> str:
        return f"{self.ancestor_context.to_string()}.{self.standalone_string}"

    @property
    def tree_item_string(self) -> str:
        return f"PartitionChild[{self.standalone_string}]"

    def to_tree_form_isolated(self, is_last: bool) -> CollectionTreeForm:
        return CollectionTreeForm(
            self.tree_item_string,
            0,
            has_predecessor=True,
        )

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        ancestor: CollectionTreeForm = self.ancestor_context.to_tree_form(True)
        ancestor.has_children = True
        tree_form: CollectionTreeForm = self.to_tree_form_isolated(is_last)
        tree_form.predecessor = ancestor
        tree_form.depth = ancestor.depth + 1
        return tree_form
