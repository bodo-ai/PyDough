"""
Definition of PyDough QDAG collection type for a CROSS operation that links
the ancestor context to an independently derived child context.
"""

__all__ = ["Cross"]


import pydough
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.expressions import (
    BackReferenceExpression,
    ChildReferenceExpression,
    CollationExpression,
)

from .child_access import ChildAccess
from .child_operator import ChildOperator
from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm


class Cross(ChildOperator):
    """
    The QDAG node implementation class representing a CROSS operation.
    """

    def __init__(
        self,
        ancestor: PyDoughCollectionQDAG,
        child: PyDoughCollectionQDAG,
    ):
        super().__init__([child])
        self._ancestor_context: PyDoughCollectionQDAG = ancestor
        self._child: PyDoughCollectionQDAG = child
        self._ancestral_mapping: dict[str, int] = {
            name: level + 1 for name, level in ancestor.ancestral_mapping.items()
        }
        self._calc_terms: set[str] = set()
        self._calc_term_indices: dict[str, int] = {}
        for idx, ref in enumerate(child.calc_terms):
            self._calc_term_indices[ref] = idx
            self._calc_terms.add(ref)
        for name in child.ancestral_mapping:
            if name in ancestor.ancestral_mapping:
                raise pydough.active_session.error_builder.downstream_conflict(
                    collection=self, term_name=name
                )
            self._ancestral_mapping[name] = 0

        self._all_terms: set[str] = set(self.ancestral_mapping) | self._child.all_terms

    @property
    def name(self) -> str:
        return self.child.name

    @property
    def ancestor_context(self) -> PyDoughCollectionQDAG:
        return self._ancestor_context

    @property
    def preceding_context(self) -> PyDoughCollectionQDAG | None:
        return None

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
        return (
            self.ancestor_context.inherited_downstreamed_terms
            | self.child.inherited_downstreamed_terms
        )

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return None

    @property
    def unique_terms(self) -> list[str]:
        return self.child.unique_terms

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        # It is presumed that CROSS always creates a plural
        # subcollection.
        return False

    @property
    def standalone_string(self) -> str:
        return f"CROSS({self.child.to_string()})"

    def to_string(self) -> str:
        return f"{self.ancestor_context.to_string()}.{self.standalone_string}"

    @property
    def tree_item_string(self) -> str:
        return "Cross"

    def get_expression_position(self, expr_name: str) -> int:
        return self._calc_term_indices[expr_name]

    def get_term(self, term_name: str) -> PyDoughQDAG:
        if (
            term_name in self.ancestral_mapping
            and self.ancestral_mapping[term_name] > 0
        ):
            # Verify there is no name conflict between the ancestral terms and
            # the child terms.
            if term_name in self._calc_term_indices:
                raise pydough.active_session.error_builder.downstream_conflict(
                    collection=self, term_name=term_name
                )
            return BackReferenceExpression(
                self, term_name, self.ancestral_mapping[term_name]
            )
        else:
            self.child.verify_term_exists(term_name)
            term = self.child.get_term(term_name)
            if isinstance(term, PyDoughCollectionQDAG):
                assert isinstance(term, ChildAccess)
                return term.clone_with_parent(self)
            else:
                return ChildReferenceExpression(self.child, 0, term_name)

    def to_tree_form_isolated(self, is_last: bool) -> CollectionTreeForm:
        result: CollectionTreeForm = super().to_tree_form_isolated(is_last)
        child_root: CollectionTreeForm = result.nested_trees[0]
        while child_root.predecessor is not None:
            child_root = child_root.predecessor
        child_root.has_predecessor = True
        return result

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = self.ancestor_context.to_tree_form(is_last)
        predecessor.has_children = True
        tree_form: CollectionTreeForm = self.to_tree_form_isolated(is_last)
        tree_form.depth = predecessor.depth + 1
        tree_form.predecessor = predecessor
        return tree_form

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, Cross)
            and self._ancestor_context == other._ancestor_context
            and self._child == other._child
        )
