"""
TODO: add file-level docstring
"""

__all__ = ["OrderBy"]


from collections.abc import MutableSequence

from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.expressions import CollationExpression

from .child_operator import ChildOperator
from .collection_ast import PyDoughCollectionAST


class OrderBy(ChildOperator):
    """
    The AST node implementation class representing an ORDER BY clause.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        children: MutableSequence[PyDoughCollectionAST],
    ):
        super().__init__(predecessor, children)
        self._collation: list[CollationExpression] | None = None

    def with_collation(self, collation: list[CollationExpression]) -> "OrderBy":
        """
        Specifies the expressions that are used to do the ordering in an
        ORDERBY node returning the mutated ORDERBY node afterwards. This is
        called after the ORDERBY node is created so that the terms can be
        expressions that reference child nodes of the ORDERBY. However, this
        must be called on the ORDERBY node before any properties are accessed
        by `calc_terms`, `all_terms`, `to_string`, etc.

        Args:
            `collation`: the list of collation nodes to order by.

        Returns:
            The mutated ORDERBY node (which has also been modified in-place).

        Raises:
            `PyDoughASTException` if the condition has already been added to
            the WHERE node.
        """
        if self._collation is not None:
            raise PyDoughASTException(
                "Cannot call `with_collation` more than once per ORDERBY node"
            )
        self._collation = collation
        self.verify_singular_terms(collation)
        return self

    @property
    def collation(self) -> list[CollationExpression]:
        """
        The ordering keys for the ORDERBY clause.
        """
        if self._collation is None:
            raise PyDoughASTException(
                "Cannot access `collation` of an ORDERBY node before calling `with_collation`"
            )
        return self._collation

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.ORDERBY"

    @property
    def calc_terms(self) -> set[str]:
        return self.preceding_context.calc_terms

    @property
    def all_terms(self) -> set[str]:
        return self.preceding_context.all_terms

    @property
    def ordering(self) -> list[CollationExpression]:
        return self.collation

    @property
    def standalone_string(self) -> str:
        collation_str: str = ", ".join([expr.to_string() for expr in self.collation])
        return f"ORDER_BY({collation_str})"

    def to_string(self) -> str:
        assert self.preceding_context is not None
        return f"{self.preceding_context.to_string()}.{self.standalone_string}"

    @property
    def tree_item_string(self) -> str:
        collation_str: str = ", ".join(
            [expr.to_string(True) for expr in self.collation]
        )
        return f"OrderBy[{collation_str}]"

    def equals(self, other: object) -> bool:
        if self._collation is None:
            raise PyDoughASTException(
                "Cannot invoke `equals` before calling `with_collation`"
            )
        return (
            super().equals(other)
            and isinstance(other, OrderBy)
            and self._collation == other._collation
        )
