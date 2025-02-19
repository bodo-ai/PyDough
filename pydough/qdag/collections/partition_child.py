"""
Definition of PyDough QDAG collection type for accesses to the data that was
partitioned in a PARTITION clause.
"""

__all__ = ["PartitionChild"]


from functools import cache

from pydough.qdag.expressions import BackReferenceExpression, CollationExpression

from .child_access import ChildAccess
from .child_operator_child_access import ChildOperatorChildAccess
from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm


class PartitionChild(ChildOperatorChildAccess):
    """
    Special wrapper around a ChildAccess instance that denotes it as a
    reference to the input to a Partition node, for the purposes of
    stringification.
    """

    def __init__(
        self,
        child_access: PyDoughCollectionQDAG,
        partition_child_name: str,
        ancestor: PyDoughCollectionQDAG,
    ):
        super(ChildOperatorChildAccess, self).__init__(ancestor)
        self._child_access = child_access
        self._is_last = True
        self._partition_child_name: str = partition_child_name
        self._ancestor: PyDoughCollectionQDAG = ancestor
        self._ancestral_mapping: dict[str, int] = {
            name: level + 1 for name, level in ancestor.ancestral_mapping.items()
        }

    def clone_with_parent(self, new_ancestor: PyDoughCollectionQDAG) -> ChildAccess:
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
    def ancestral_mapping(self) -> dict[str, int]:
        return self._ancestral_mapping

    @property
    def all_terms(self) -> set[str]:
        return self.child_access.all_terms | set(self.ancestral_mapping)

    @cache
    def get_term(self, term_name: str):
        if term_name in self.ancestral_mapping:
            return BackReferenceExpression(
                self, term_name, self.ancestral_mapping[term_name]
            )
        return super().get_term(term_name)

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        # The child of a PARTITION BY clause is always presumed to be plural
        # since PyDough must assume that multiple records can be grouped
        # together into the same bucket.
        return False

    @property
    def standalone_string(self) -> str:
        return self.partition_child_name

    @cache
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
