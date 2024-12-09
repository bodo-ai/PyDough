"""
TODO: add file-level docstring
"""

__all__ = ["Calc"]

from collections.abc import MutableMapping, MutableSequence
from functools import cache

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.expressions import PyDoughExpressionAST
from pydough.pydough_ast.has_hasnot_rewrite import has_hasnot_rewrite

from .child_operator import ChildOperator
from .collection_ast import PyDoughCollectionAST


class Calc(ChildOperator):
    """
    The AST node implementation class representing a CALC expression.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        children: MutableSequence[PyDoughCollectionAST],
    ):
        super().__init__(predecessor, children)
        # Not initialized until with_terms is called
        self._calc_term_indices: dict[str, int] | None = None
        self._calc_term_values: MutableMapping[str, PyDoughExpressionAST] | None = None
        self._all_term_names: set[str] = set()

    def with_terms(
        self, terms: MutableSequence[tuple[str, PyDoughExpressionAST]]
    ) -> "Calc":
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
            contain `ChildReferenceExpression` instances that refer to an property of one
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
        # Include terms from the predecessor, with the terms from this CALC
        # added in.
        self._calc_term_indices = {}
        self._calc_term_values = {}
        for idx, (name, value) in enumerate(terms):
            self._calc_term_indices[name] = idx
            self._calc_term_values[name] = has_hasnot_rewrite(value, False)
            self._all_term_names.add(name)
        self.all_terms.update(self.preceding_context.all_terms)
        self.verify_singular_terms(self._calc_term_values.values())
        return self

    @property
    def calc_term_indices(
        self,
    ) -> dict[str, int]:
        """
        Mapping of each named expression of the CALC to the index of the
        ordinal position of the property when included in a CALC.
        """
        if self._calc_term_indices is None:
            raise PyDoughASTException(
                "Cannot access `calc_term_indices` of a Calc node before adding calc terms with `with_terms`"
            )
        return self._calc_term_indices

    @property
    def calc_term_values(
        self,
    ) -> MutableMapping[str, PyDoughExpressionAST]:
        """
        Mapping of each named expression of the CALC to the AST node for
        that expression.
        """
        if self._calc_term_values is None:
            raise PyDoughASTException(
                "Cannot access `_calc_term_values` of a Calc node before adding calc terms with `with_terms`"
            )
        return self._calc_term_values

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.CALC"

    @property
    def calc_terms(self) -> set[str]:
        return set(self.calc_term_indices)

    @property
    def all_terms(self) -> set[str]:
        return self._all_term_names

    def get_expression_position(self, expr_name: str) -> int:
        if expr_name not in self.calc_terms:
            raise PyDoughASTException(f"Unrecognized CALC term: {expr_name!r}")
        return self.calc_term_indices[expr_name]

    @cache
    def get_term(self, term_name: str) -> PyDoughAST:
        if term_name in self.calc_term_values:
            return self.calc_term_values[term_name]
        else:
            return super().get_term(term_name)

    def calc_kwarg_strings(self, tree_form: bool) -> str:
        """
        Converts the terms of a CALC into a string in the form
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
            expr: PyDoughExpressionAST = self.get_expr(name)
            kwarg_strings.append(f"{name}={expr.to_string(tree_form)}")
        return ", ".join(kwarg_strings)

    @property
    def standalone_string(self) -> str:
        return f"({self.calc_kwarg_strings(False)})"

    def to_string(self) -> str:
        assert self.preceding_context is not None
        return f"{self.preceding_context.to_string()}{self.standalone_string}"

    @property
    def tree_item_string(self) -> str:
        return f"Calc[{self.calc_kwarg_strings(True)}]"

    def equals(self, other: object) -> bool:
        if self._calc_term_indices is None:
            raise PyDoughASTException(
                "Cannot invoke `equals` before calling `with_terms`"
            )
        return (
            super().equals(other)
            and isinstance(other, Calc)
            and self._calc_term_indices == other._calc_term_indices
            and self._calc_term_values == other._calc_term_values
        )
