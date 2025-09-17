"""
Definition of PyDough QDAG collection type for a PARTITION operation that
buckets its input data on certain keys, creating a new parent collection whose
child is the input data.
"""

__all__ = ["PartitionBy"]


from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.expressions import (
    BackReferenceExpression,
    ChildReferenceExpression,
    CollationExpression,
    PartitionKey,
)

from .child_operator import ChildOperator
from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm
from .partition_child import PartitionChild


class PartitionBy(ChildOperator):
    """
    The QDAG node implementation class representing a PARTITION BY clause.
    """

    def __init__(
        self,
        ancestor: PyDoughCollectionQDAG,
        child: PyDoughCollectionQDAG,
        name: str,
        keys: list[ChildReferenceExpression],
    ):
        super().__init__([child])
        self._ancestor_context: PyDoughCollectionQDAG = ancestor
        self._child: PyDoughCollectionQDAG = child
        self._name: str = name
        self._key_name_indices: dict[str, int] = {}
        self._ancestral_mapping: dict[str, int] = {
            name: level + 1 for name, level in ancestor.ancestral_mapping.items()
        }
        self._calc_terms: set[str] = set()
        self._all_terms: set[str] = set(self.ancestral_mapping) | {self.child.name}
        self._keys = [PartitionKey(self, key) for key in keys]
        for idx, ref in enumerate(keys):
            self._key_name_indices[ref.term_name] = idx
            self._calc_terms.add(ref.term_name)
        self.all_terms.update(self._calc_terms)
        self.verify_singular_terms(self._keys)

    @property
    def name(self) -> str:
        return self._name

    @property
    def ancestor_context(self) -> PyDoughCollectionQDAG:
        return self._ancestor_context

    @property
    def preceding_context(self) -> PyDoughCollectionQDAG | None:
        return None

    @property
    def keys(self) -> list[PartitionKey]:
        """
        The partitioning keys for the PARTITION BY clause.
        """
        return self._keys

    @property
    def key_name_indices(self) -> dict[str, int]:
        """
        The names of the partitioning keys for the PARTITION BY clause and the
        index they have in a CALCULATE.
        """
        return self._key_name_indices

    @property
    def child(self) -> PyDoughCollectionQDAG:
        """
        The input collection that is being partitioned.
        """
        return self._child

    @property
    def key(self) -> str:
        return f"{self.ancestor_context.key}.PARTITION({self.child.key})"

    @property
    def calc_terms(self) -> set[str]:
        return self._calc_terms

    @property
    def all_terms(self) -> set[str]:
        return self._all_terms

    @property
    def ancestral_mapping(self) -> dict[str, int]:
        return self._ancestral_mapping

    @property
    def inherited_downstreamed_terms(self) -> set[str]:
        return self.ancestor_context.inherited_downstreamed_terms

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return None

    @property
    def unique_terms(self) -> list[str]:
        return [key.expr.term_name for key in self.keys]

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        # It is presumed that PARTITION BY always creates a plural
        # subcollection of the ancestor context containing 1+ bins of data
        # from the child collection.
        return False

    @property
    def standalone_string(self) -> str:
        keys_str: str
        if len(self.keys) == 1:
            keys_str = self.keys[0].expr.term_name
        else:
            keys_str = str(tuple([expr.expr.term_name for expr in self.keys]))
        return f"Partition({self.child.to_string()}, name={self.name!r}, by={keys_str})"

    def to_string(self) -> str:
        return f"{self.ancestor_context.to_string()}.{self.standalone_string}"

    @property
    def tree_item_string(self) -> str:
        keys_str: str
        if len(self.keys) == 1:
            keys_str = self.keys[0].expr.term_name
        else:
            keys_str = f"({', '.join([expr.expr.term_name for expr in self.keys])})"
        return f"Partition[name={self.name!r}, by={keys_str}]"

    def get_expression_position(self, expr_name: str) -> int:
        return self.key_name_indices[expr_name]

    def get_term(self, term_name: str) -> PyDoughQDAG:
        if term_name in self.ancestral_mapping:
            return BackReferenceExpression(
                self, term_name, self.ancestral_mapping[term_name]
            )
        elif term_name == self.child.name:
            return PartitionChild(self.child, self.child.name, self)
        else:
            self.verify_term_exists(term_name)
            assert term_name in self._key_name_indices
            term: PartitionKey = self.keys[self._key_name_indices[term_name]]
            return term

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = self.ancestor_context.to_tree_form(is_last)
        predecessor.has_children = True
        tree_form: CollectionTreeForm = self.to_tree_form_isolated(is_last)
        tree_form.depth = predecessor.depth + 1
        tree_form.predecessor = predecessor
        return tree_form

    def equals(self, other: object) -> bool:
        return isinstance(other, PartitionBy) and self._keys == other._keys
