"""
Definition of PyDough AST collection type for a PARTITION operation that
buckets its input data on certain keys, creating a new parent collection whose
child is the input data.
"""

__all__ = ["PartitionBy"]


from functools import cache

from pydough.qdag.abstract_pydough_qdag import PyDoughAST
from pydough.qdag.errors import PyDoughASTException
from pydough.qdag.expressions import (
    ChildReferenceExpression,
    CollationExpression,
    PartitionKey,
)

from .child_operator import ChildOperator
from .collection_qdag import PyDoughCollectionAST
from .partition_child import PartitionChild


class PartitionBy(ChildOperator):
    """
    The AST node implementation class representing a PARTITION BY clause.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        child: PyDoughCollectionAST,
        child_name: str,
    ):
        super().__init__(predecessor, [child])
        self._child: PyDoughCollectionAST = child
        self._child_name: str = child_name
        self._keys: list[PartitionKey] | None = None
        self._key_name_indices: dict[str, int] = {}

    def with_keys(self, keys: list[ChildReferenceExpression]) -> "PartitionBy":
        """
        Specifies the references to the keys that should be used to partition
        the child node.

        Args:
            `keys`: the list of references to the keys to partition on.

        Returns:
            The mutated PARTITION BY node (which has also been modified in-place).

        Raises:
            `PyDoughASTException` if the keys have already been added to
            the PARTITION BY node.
        """
        if self._keys is not None:
            raise PyDoughASTException(
                "Cannot call `with_keys` more than once per PARTITION BY node"
            )
        self._keys = [PartitionKey(self, key) for key in keys]
        for idx, ref in enumerate(keys):
            self._key_name_indices[ref.term_name] = idx
        self.verify_singular_terms(self._keys)
        return self

    @property
    def ancestor_context(self) -> PyDoughCollectionAST | None:
        # For PARTITION_BY, the "preceding context" is actually the ancestor
        # context, but has been referred to as the preceding context so that
        # it can inherit from ChildOperator and behave correctly.
        return self.preceding_context

    @property
    def starting_predecessor(self) -> PyDoughCollectionAST:
        return self

    @property
    def keys(self) -> list[PartitionKey]:
        """
        The partitioning keys for the PARTITION BY clause.
        """
        if self._keys is None:
            raise PyDoughASTException(
                "Cannot access `keys` of an PARTITION BY node before calling `with_keys`"
            )
        return self._keys

    @property
    def key_name_indices(self) -> dict[str, int]:
        """
        The names of the partitioning keys for the PARTITION BY clause and the
        index they have in a CALC.
        """
        if self._keys is None:
            raise PyDoughASTException(
                "Cannot access `keys` of an PARTITION BY node before calling `with_keys`"
            )
        return self._key_name_indices

    @property
    def child(self) -> PyDoughCollectionAST:
        """
        The input collection that is being partitioned.
        """
        return self._child

    @property
    def child_name(self) -> str:
        """
        The name that should be used to refer to the input collection that is
        being partitioned.
        """
        return self._child_name

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.PARTITION({self.child.key})"

    @property
    def calc_terms(self) -> set[str]:
        return set(self._key_name_indices)

    @property
    def all_terms(self) -> set[str]:
        return self.calc_terms | {self.child_name}

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return None

    def is_singular(self, context: PyDoughCollectionAST) -> bool:
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
        return f"Partition({self.child.to_string()}, name={self.child_name!r}, by={keys_str})"

    def to_string(self) -> str:
        return f"{self.preceding_context.to_string()}.{self.standalone_string}"

    @property
    def tree_item_string(self) -> str:
        keys_str: str
        if len(self.keys) == 1:
            keys_str = self.keys[0].expr.term_name
        else:
            keys_str = str(tuple([expr.expr.term_name for expr in self.keys]))
        return f"Partition[name={self.child_name!r}, by={keys_str}]"

    def get_expression_position(self, expr_name: str) -> int:
        return self.preceding_context.get_expression_position(expr_name)

    @cache
    def get_term(self, term_name: str) -> PyDoughAST:
        if term_name in self._key_name_indices:
            term: PartitionKey = self.keys[self._key_name_indices[term_name]]
            return term
        elif term_name == self.child_name:
            return PartitionChild(self.child, self.child_name, self)
        else:
            raise PyDoughASTException(f"Unrecognized term: {term_name!r}")

    def equals(self, other: object) -> bool:
        if self._keys is None:
            raise PyDoughASTException(
                "Cannot invoke `equals` before calling `with_keys`"
            )
        return (
            super().equals(other)
            and isinstance(other, PartitionBy)
            and self._keys == other._keys
        )
