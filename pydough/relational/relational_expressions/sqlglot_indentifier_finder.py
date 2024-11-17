"""
Handle determining the identifiers that have been
used for a given Relational Expression.
"""

import copy

from sqlglot.expressions import Identifier

from .abstract_expression import RelationalExpression
from .call_expression import CallExpression
from .column_reference import ColumnReference
from .literal_expression import LiteralExpression
from .relational_expression_visitor import RelationalExpressionVisitor
from .sqlglot_relational_expression_visitor import SQLGlotRelationalExpressionVisitor

__all__ = ["SQLGlotIdentifierFinder"]


class SQLGlotIdentifierFinder(RelationalExpressionVisitor):
    """
    The visitor for finding the identifiers used in a relational expression.
    We opt to use identifiers to ensure
    """

    def __init__(self) -> None:
        self._identifiers: set[Identifier] = set()

    def reset(self) -> None:
        """
        Reset just resets our set.
        """
        self._identifiers = set()

    def visit_call_expression(self, call_expression: CallExpression) -> None:
        # Just visit the inputs.
        for arg in call_expression.inputs:
            arg.accept(self)

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        # No-Op
        pass

    def visit_column_reference(self, column_reference: ColumnReference) -> None:
        self._identifiers.add(
            SQLGlotRelationalExpressionVisitor.generate_column_reference_identifier(
                column_reference
            )
        )

    def find_identifiers(self, expr: RelationalExpression) -> set[Identifier]:
        """
        Return the set of all identifiers used in the relational expression.

        Args:
            expr (RelationalExpression): The relational expression to check entirely.

        Returns:
            SQLGlotExpression: The set of identifiers as they would be generated
            if we were to convert the relational expression to SQLGlot.
        """
        self.reset()
        expr.accept(self)
        # Be conservative and call copy. This shouldn't be necessary in general.
        return copy.copy(self._identifiers)
