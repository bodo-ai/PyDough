"""
TODO: add file-level docstring
"""

__all__ = ["Calc"]


from typing import Dict, List, Tuple, Set

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.expressions import PyDoughExpressionAST
from .collection_ast import PyDoughCollectionAST
from .calc_child_collection import CalcChildCollection


class Calc(PyDoughCollectionAST):
    """
    The AST node implementation class representing a CALC expression.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        children: List[CalcChildCollection],
    ):
        self._predecessor: PyDoughCollectionAST = predecessor
        self._children: List[CalcChildCollection] = children
        # Not initialized until with_terms is called
        self._calc_term_indices: Dict[str, Tuple[int, PyDoughExpressionAST]] | None = (
            None
        )
        self._all_terms: Dict[str, PyDoughExpressionAST] = None

    def with_terms(self, terms: List[Tuple[str, PyDoughExpressionAST]]) -> "Calc":
        """
        TODO: add function docstring
        """
        if self._calc_term_indices is not None:
            raise PyDoughCollectionAST(
                "Cannot call `with_terms` more than once per Calc node"
            )
        self._calc_term_indices = {name: idx for idx, (name, _) in enumerate(terms)}
        # Include terms from the predecessor, with the terms from this CALC
        # added in (overwriting any preceding properties with the same name)
        self._all_terms = {}
        for name in self.preceding_context.all_terms:
            self._all_terms[name] = self.preceding_context.get_term(name)
        for name, property in terms:
            self._all_terms[name] = property
        return self

    @property
    def children(self) -> List[CalcChildCollection]:
        """
        The child collections accessible from the CALC used to derive
        expressions in terms of a subcollection.
        """
        return self._children

    @property
    def calc_term_indices(self) -> Dict[str, Tuple[int, PyDoughExpressionAST]]:
        """
        Mapping of each named expression of the CALC to a tuple (idx, expr)
        where idx is the ordinal position of the property when included
        in a CALC and property is the AST node representing the property.
        """
        if self._calc_term_indices is None:
            raise PyDoughCollectionAST(
                "Cannot invoke `calc_term_indices` before calling `with_terms`"
            )
        return self._calc_term_indices

    @property
    def ancestor_context(self) -> PyDoughCollectionAST | None:
        return self._predecessor.ancestor_context

    @property
    def preceding_context(self) -> PyDoughCollectionAST | None:
        return self._predecessor

    @property
    def calc_terms(self) -> Set[str]:
        return set(self.calc_term_indices)

    @property
    def all_terms(self) -> Set[str]:
        if self._all_terms is None:
            raise PyDoughCollectionAST(
                "Cannot invoke `all_terms` before calling `with_terms`"
            )
        return set(self._all_terms)

    def get_expression_position(self, expr_name: str) -> int:
        if expr_name not in self.calc_term_indices:
            raise PyDoughASTException(f"Unrecognized CALC term: {expr_name!r}")
        return self.calc_term_indices[expr_name]

    def get_term(self, term_name: str) -> PyDoughAST:
        if term_name not in self.all_terms:
            raise PyDoughASTException(f"Unrecognized term: {term_name!r}")
        return self._all_terms[term_name]

    def to_string(self) -> str:
        kwarg_strings: List[str] = []
        for name in self._calc_term_indices:
            expr: PyDoughExpressionAST = self.get_term(name)
            kwarg_strings.append(f"{name}={expr.to_string()}")
        return f"{self.preceding_context.to_string()}({', '.join(kwarg_strings)})"

    def to_tree_form(self) -> None:
        raise NotImplementedError

    def equals(self, other: "Calc") -> bool:
        if self._all_terms is None:
            raise PyDoughCollectionAST(
                "Cannot invoke `equals` before calling `with_terms`"
            )
        return (
            super().equals(other)
            and self.preceding_context == other.preceding_context
            and self._calc_term_indices == other._calc_term_indices
        )
