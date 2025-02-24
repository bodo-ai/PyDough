"""
Definition of PyDough QDAG collection type for accesses to a subcollection of an
ancestor of the current context.
"""

__all__ = ["Prev"]


from functools import cache

from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.errors import PyDoughQDAGException
from pydough.qdag.expressions import CollationExpression, Reference

from .child_access import ChildAccess
from .collection_qdag import PyDoughCollectionQDAG


class Prev(ChildAccess):
    """
    The QDAG node implementation class representing the Prev operator.
    """

    def __init__(
        self,
        ancestor: PyDoughCollectionQDAG,
        n_behind: int,
        levels: int | None,
        collation: list[CollationExpression],
    ):
        self._n_behind: int = n_behind
        self._levels: int | None = levels
        self._collation: list[CollationExpression] = collation
        if len(collation) == 0:
            raise PyDoughQDAGException("Collation cannot be empty for Prev operator")
        for elem in collation:
            if not isinstance(elem.expr, Reference):
                raise PyDoughQDAGException(
                    f"Unsupported collation expression for Prev operator: {elem.expr}"
                )
        super().__init__(ancestor)

    def clone_with_parent(self, new_ancestor: PyDoughCollectionQDAG) -> ChildAccess:
        raise NotImplementedError

    @property
    def n_behind(self) -> int:
        """
        TODO
        """
        return self._n_behind

    @property
    def levels(self) -> int | None:
        """
        TODO
        """
        return self._levels

    @property
    def collation(self) -> list[CollationExpression]:
        """
        TODO
        """
        return self._collation

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return self.ancestor_context.ordering

    @cache
    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        relative_ancestor: PyDoughCollectionQDAG = (
            self.ancestor_context.starting_predecessor
        )
        return (context == relative_ancestor) or relative_ancestor.is_singular(context)

    @property
    def key(self) -> str:
        return self.standalone_string

    @property
    def calc_terms(self) -> set[str]:
        return self.ancestor_context.calc_terms

    @property
    def all_terms(self) -> set[str]:
        return self.ancestor_context.all_terms

    def get_expression_position(self, expr_name: str) -> int:
        return self.ancestor_context.get_expression_position(expr_name)

    @cache
    def get_term(self, term_name: str) -> PyDoughQDAG:
        from pydough.qdag.expressions import PyDoughExpressionQDAG, Reference

        term: PyDoughQDAG = self.ancestor_context.get_term(term_name)
        if isinstance(term, ChildAccess):
            term = term.clone_with_parent(self)
        elif isinstance(term, PyDoughExpressionQDAG):
            term = Reference(self.ancestor_context, term_name)
        return term

    @property
    def unique_terms(self) -> list[str]:
        return self.ancestor_context.unique_terms

    @property
    def standalone_string(self) -> str:
        levels_str = f", levels={self.levels}" if self.levels is not None else ""
        collation_str: str = ", ".join([expr.to_string() for expr in self.collation])
        return f"PREV({self.n_behind}{levels_str}, by=({collation_str}))"

    def to_string(self) -> str:
        return self.standalone_string

    @property
    def tree_item_string(self) -> str:
        levels_str = f", levels={self.levels}" if self.levels is not None else ""
        collation_str: str = ", ".join([expr.to_string() for expr in self.collation])
        return f"Prev[{self.n_behind}{levels_str}, by=({collation_str})]"

    # def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
    #     ancestor: CollectionTreeForm = self.ancestor_context.to_tree_form(True)
    #     ancestor.has_children = True
    #     tree_form: CollectionTreeForm = self.to_tree_form_isolated(is_last)
    #     tree_form.predecessor = ancestor
    #     tree_form.depth = ancestor.depth + 1
    #     return tree_form

    def equals(self, other: object) -> bool:
        return (
            super().equals(other)
            and isinstance(other, Prev)
            and self.n_behind == other.n_behind
            and self.levels == other.levels
            and self.collation == other.collation
        )
