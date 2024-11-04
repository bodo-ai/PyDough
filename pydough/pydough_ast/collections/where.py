"""
TODO: add file-level docstring
"""

__all__ = ["Where"]


from typing import List, Set

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.expressions import PyDoughExpressionAST
from .collection_ast import PyDoughCollectionAST
from .calc_child_collection import CalcChildCollection
from .child_operator import ChildOperator


class Where(ChildOperator):
    """
    The AST node implementation class representing a WHERE filter.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        children: List[CalcChildCollection],
    ):
        super().__init__(predecessor, children)
        self._condition: PyDoughExpressionAST | None = None

    def with_condition(self, condition: PyDoughExpressionAST) -> "Where":
        """
        Specifies the terms that are calculated inside of a CALC node,
        returning the mutated CALC node afterwards. This is called after the
        CALC node is created so that the terms can be expressions that
        reference child nodes of the CALC. However, this must be called
        on the CALC node before any properties are accessed by `calc_terms`,
        `all_terms`, `to_string`, etc.

        Args:
            `condition`: the expression node for the condition being
            calculated.

        Returns:
            The mutated WHERE node (which has also been modified in-place).

        Raises:
            `PyDoughASTException` if the condition has already been added to
            the WHERE node.
        """
        if self._condition is not None:
            raise PyDoughCollectionAST(
                "Cannot call `with_condition` more than once per Where node"
            )
        self._condition = condition

    @property
    def condition(self) -> PyDoughExpressionAST:
        """
        The predicate expression for the WHERE clause.
        """
        return self._condition

    @property
    def calc_terms(self) -> Set[str]:
        return self.preceding_context.calc_terms

    @property
    def all_terms(self) -> Set[str]:
        return self.preceding_context.all_terms

    def get_expression_position(self, expr_name: str) -> int:
        return self.preceding_context.get_expression_position(expr_name)

    def get_term(self, term_name: str) -> PyDoughAST:
        return self.preceding_context.get_term(term_name)

    def to_string(self) -> str:
        return (
            f"{self.preceding_context.to_string()}.WHERE({self.condition.to_string()})"
        )

    @property
    def tree_item_string(self) -> str:
        kwarg_strings: List[str] = []
        for name in self._calc_term_indices:
            expr: PyDoughExpressionAST = self.get_term(name)
            kwarg_strings.append(f"{name}={expr.to_string(tree_form=True)}")
        return f"WHERE[{self.condition.to_string()}]"

    def equals(self, other: "Where") -> bool:
        if self._condition is None:
            raise PyDoughCollectionAST(
                "Cannot invoke `equals` before calling `with_condition`"
            )
        return super().equals(other) and self._condition == other._condition
