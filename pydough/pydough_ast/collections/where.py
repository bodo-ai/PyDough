"""
TODO: add file-level docstring
"""

__all__ = ["Where"]


from collections.abc import MutableSequence

from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .child_operator import ChildOperator
from .collection_ast import PyDoughCollectionAST


class Where(ChildOperator):
    """
    The AST node implementation class representing a WHERE filter.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        children: MutableSequence[PyDoughCollectionAST],
    ):
        super().__init__(predecessor, children)
        self._condition: PyDoughExpressionAST | None = None

    def with_condition(self, condition: PyDoughExpressionAST) -> "Where":
        """
        Specifies the condition that should be used by the WHERE node. This is
        called after the WHERE node is created so that the condition can be an
        expressions that reference child nodes of the WHERE. However, this must
        be called on the WHERE node before any properties are accessed by
        `to_string`, `equals`, etc.

        Args:
            `condition`: the expression used to filter.

        Returns:
            The mutated WHERE node (which has also been modified in-place).

        Raises:
            `PyDoughASTException` if the condition has already been added to
            the WHERE node.
        """
        if self._condition is not None:
            raise PyDoughASTException(
                "Cannot call `with_condition` more than once per Where node"
            )
        self._condition = condition
        return self

    @property
    def condition(self) -> PyDoughExpressionAST:
        """
        The predicate expression for the WHERE clause.
        """
        if self._condition is None:
            raise PyDoughASTException(
                "Cannot access `condition` of a Calc node before adding calc terms with `with_condition`"
            )
        return self._condition

    @property
    def calc_terms(self) -> set[str]:
        return self.preceding_context.calc_terms

    @property
    def all_terms(self) -> set[str]:
        return self.preceding_context.all_terms

    def to_string(self) -> str:
        assert self.preceding_context is not None
        return (
            f"{self.preceding_context.to_string()}.WHERE({self.condition.to_string()})"
        )

    @property
    def tree_item_string(self) -> str:
        return f"Where[{self.condition.to_string()}]"

    def equals(self, other: object) -> bool:
        if self._condition is None:
            raise PyDoughASTException(
                "Cannot invoke `equals` before calling `with_condition`"
            )
        return (
            super().equals(other)
            and isinstance(other, Where)
            and self._condition == other._condition
        )
