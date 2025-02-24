"""
Definition of PyDough QDAG collection type for accesses to the data that was
partitioned in a PARTITION clause.
"""

__all__ = ["PartitionChild"]


from pydough.qdag.expressions.collation_expression import CollationExpression

from .child_access import ChildAccess
from .child_operator_child_access import ChildOperatorChildAccess
from .collection_qdag import PyDoughCollectionQDAG


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

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        # The child of a PARTITION BY clause is always presumed to be plural
        # since PyDough must assume that multiple records can be grouped
        # together into the same bucket.
        return False

    @property
    def standalone_string(self) -> str:
        return self.partition_child_name

    def to_string(self) -> str:
        return f"{self.ancestor_context.to_string()}.{self.standalone_string}"

    @property
    def tree_item_string(self) -> str:
        return f"PartitionChild[{self.standalone_string}]"
