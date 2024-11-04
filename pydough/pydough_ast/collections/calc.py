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
from .child_operator import ChildOperator


class Calc(ChildOperator):
    """
    The AST node implementation class representing a CALC expression.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        children: List[CalcChildCollection],
    ):
        super().__init__(predecessor, children)
        self._predecessor: PyDoughCollectionAST = predecessor
        self._children: List[CalcChildCollection] = children
        # Not initialized until with_terms is called
        self._calc_term_indices: Dict[str, Tuple[int, PyDoughExpressionAST]] | None = (
            None
        )
        self._all_terms: Dict[str, PyDoughExpressionAST] = {}

    def with_terms(self, terms: List[Tuple[str, PyDoughExpressionAST]]) -> "Calc":
        """
        Specifies the terms that are calculated inside of a CALC node,
        returning the mutated CALC node afterwards. This is called after the
        CALC node is created so that the terms can be expressions that
        reference child nodes of the CALC. However, this must be called
        on the CALC node before any properties are accessed by `calc_terms`,
        `all_terms`, `to_string`, etc.

        Args:
            `terms`: the list of terms calculated in the CALC node as a list of
            tuples in the form `(name, expression)`. Each `expression` can
            contain `ChildReference` instances that refer to an property of one
            of the children of the CALC node.

        Returns:
            The mutated CALC node (which has also been modified in-place).

        Raises:
            `PyDoughASTException` if the terms have already been added to the
            CALC node.
        """
        if self._calc_term_indices is not None:
            raise PyDoughASTException(
                "Cannot call `with_terms` on a CALC node more than once"
            )
        self._calc_term_indices = {name: idx for idx, (name, _) in enumerate(terms)}
        # Include terms from the predecessor, with the terms from this CALC
        # added in (overwriting any preceding properties with the same name)
        self._all_terms = {}
        for name in self.propagated_properties:
            self._all_terms[name] = self.propagated_properties[name]
        for name, property in terms:
            self._all_terms[name] = property
        return self

    @property
    def calc_term_indices(self) -> Dict[str, Tuple[int, PyDoughExpressionAST]]:
        """
        Mapping of each named expression of the CALC to a tuple (idx, expr)
        where idx is the ordinal position of the property when included
        in a CALC and property is the AST node representing the property.
        """
        if self._calc_term_indices is None:
            raise PyDoughASTException(
                "Cannot access `calc_term_indices` of a Calc node before adding calc terms with `with_terms`"
            )
        return self._calc_term_indices

    @property
    def calc_terms(self) -> Set[str]:
        return set(self.calc_term_indices)

    @property
    def all_terms(self) -> Set[str]:
        if self._calc_term_indices is None:
            raise PyDoughASTException(
                "Cannot access `all_terms` of a Calc node before adding calc terms with `with_terms`"
            )
        return set(self._all_terms)

    def get_expression_position(self, expr_name: str) -> int:
        if expr_name not in self.calc_term_indices:
            raise PyDoughASTException(f"Unrecognized CALC term: {expr_name!r}")
        return self.calc_term_indices[expr_name]

    def get_term(self, term_name: str) -> PyDoughAST:
        if term_name not in self.all_terms:
            # breakpoint()
            raise PyDoughASTException(f"Unrecognized term: {term_name!r}")
        return self._all_terms[term_name]

    def to_string(self) -> str:
        kwarg_strings: List[str] = []
        for name in self._calc_term_indices:
            expr: PyDoughExpressionAST = self.get_term(name)
            kwarg_strings.append(f"{name}={expr.to_string()}")
        return f"{self.preceding_context.to_string()}({', '.join(kwarg_strings)})"

    @property
    def tree_item_string(self) -> str:
        kwarg_strings: List[str] = []
        for name in self._calc_term_indices:
            expr: PyDoughExpressionAST = self.get_term(name)
            kwarg_strings.append(f"{name}={expr.to_string(tree_form=True)}")
        return f"Calc[{', '.join(kwarg_strings)}]"

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
