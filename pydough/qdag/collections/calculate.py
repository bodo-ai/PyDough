"""
Definition of PyDough QDAG collection type for a CALCULATE, which defines new
expressions of the current context (or overrides existing definitions) that are
all singular with regards to it.
"""

__all__ = ["Calculate"]


from pydough.errors import PyDoughQDAGException
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.expressions import (
    PyDoughExpressionQDAG,
)
from pydough.qdag.has_hasnot_rewrite import has_hasnot_rewrite

from .augmenting_child_operator import AugmentingChildOperator
from .collection_qdag import PyDoughCollectionQDAG


class Calculate(AugmentingChildOperator):
    """
    The QDAG node implementation class representing a CALCULATE expression.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionQDAG,
        children: list[PyDoughCollectionQDAG],
        terms: list[tuple[str, PyDoughExpressionQDAG]],
    ):
        super().__init__(predecessor, children)
        self._calc_term_indices: dict[str, int] = {}
        self._calc_term_values: dict[str, PyDoughExpressionQDAG] = {}
        self._all_term_names: set[str] = set()
        self._ancestral_mapping: dict[str, int] = dict(
            predecessor.ancestral_mapping.items()
        )
        self._calc_terms: set[str] = set()

        # Include terms from the predecessor, with the terms from this
        # CALCULATE added in.
        for idx, (name, value) in enumerate(terms):
            self._calc_term_indices[name] = idx
            self._calc_term_values[name] = has_hasnot_rewrite(value, False)
            self._all_term_names.add(name)
            self._calc_terms.add(name)
            self.ancestral_mapping[name] = 0
        self.all_terms.update(self.preceding_context.all_terms)
        self.verify_singular_terms(self._calc_term_values.values())

    @property
    def calc_term_indices(
        self,
    ) -> dict[str, int]:
        """
        Mapping of each named expression of the CALCULATE to the index of the
        ordinal position of the property when included in a CALCULATE.
        """
        return self._calc_term_indices

    @property
    def calc_term_values(
        self,
    ) -> dict[str, PyDoughExpressionQDAG]:
        """
        Mapping of each named expression of the CALCULATE to the QDAG node for
        that expression.
        """
        return self._calc_term_values

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.CALCULATE"

    @property
    def calc_terms(self) -> set[str]:
        return self._calc_terms

    @property
    def all_terms(self) -> set[str]:
        return self._all_term_names

    @property
    def ancestral_mapping(self) -> dict[str, int]:
        return self._ancestral_mapping

    def get_expression_position(self, expr_name: str) -> int:
        if expr_name not in self.calc_terms:
            raise PyDoughQDAGException(f"Unrecognized CALCULATE term: {expr_name!r}")
        return self.calc_term_indices[expr_name]

    def get_term(self, term_name: str) -> PyDoughQDAG:  # type: ignore
        self.verify_term_exists(term_name)
        if term_name in self.calc_term_values:
            return self.calc_term_values[term_name]

        return super().get_term(term_name)

    def calc_kwarg_strings(self, tree_form: bool) -> str:
        """
        Converts the terms of a CALCULATE into a string in the form
        `"x=1, y=phone_number, z=STARTSWITH(LOWER(name), 'a')"`

        Args:
            `tree_form` whether to convert the arguments to strings for a tree
            form or not.

        Returns:
            The string representation of the arguments.
        """
        kwarg_strings: list[str] = []
        for name in sorted(
            self.calc_terms, key=lambda name: self.get_expression_position(name)
        ):
            expr: PyDoughExpressionQDAG = self.get_expr(name)
            kwarg_strings.append(f"{name}={expr.to_string(tree_form)}")
        return ", ".join(kwarg_strings)

    @property
    def standalone_string(self) -> str:
        return f"CALCULATE({self.calc_kwarg_strings(False)})"

    @property
    def tree_item_string(self) -> str:
        return f"Calculate[{self.calc_kwarg_strings(True)}]"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, Calculate)
            and self._calc_term_indices == other._calc_term_indices
            and self._calc_term_values == other._calc_term_values
            and super().equals(other)
        )
