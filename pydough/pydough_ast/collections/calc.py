"""
TODO: add file-level docstring
"""

__all__ = ["Calc"]


from typing import Dict, List, Tuple, Set

from pydough.metadata import CollectionMetadata
from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.expressions import PyDoughExpressionAST
from .collection_ast import PyDoughCollectionAST


class Calc(PyDoughCollectionAST):
    """
    The AST node implementation class representing a CALC expression.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        terms: List[Tuple[str, PyDoughExpressionAST]],
    ):
        self._predecessor: PyDoughCollectionAST = predecessor
        self._calc_term_indices: Dict[str, Tuple[int, PyDoughExpressionAST]] = {
            name: idx for idx, (name, _) in enumerate(terms)
        }
        # Include terms from the predecessor, with the terms from this CALC
        # added in (overwriting any preceding properties with the same name)
        self._all_terms: Dict[str, PyDoughExpressionAST] = {}

        for name in predecessor.all_terms:
            self._all_terms[name] = predecessor.get_term(name)
        for name, property in terms:
            self._all_terms[name] = property

    @property
    def collection(self) -> CollectionMetadata:
        """
        The table that is being referenced by the collection node.
        """
        return self._collection

    @property
    def calc_term_indices(self) -> Dict[str, Tuple[int, PyDoughExpressionAST]]:
        """
        Mapping of each named expression of the CALC to a tuple (idx, expr)
        where idx is the ordinal position of the property when included
        in a CALC and property is the AST node representing the property.
        """
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

    def to_tree_string(self) -> str:
        raise NotImplementedError

    def equals(self, other: "Calc") -> bool:
        return (
            super().equals(other)
            and self.preceding_context == other.preceding_context
            and self.terms == other.terms
        )
