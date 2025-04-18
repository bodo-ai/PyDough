"""
The basic Visitor pattern to perform operations across the
expression components of a Relational tree. The primary motivation of
this module is to allow associating lowering the Relational expressions
into a specific backend in a single class, but this can also be
used for any other tree based operations (e.g. string generation).
"""

from abc import ABC, abstractmethod

from .call_expression import CallExpression
from .column_reference import ColumnReference
from .correlated_reference import CorrelatedReference
from .literal_expression import LiteralExpression
from .window_call_expression import WindowCallExpression

__all__ = ["RelationalExpressionVisitor"]


class RelationalExpressionVisitor(ABC):
    """
    Representations of a visitor pattern across the relational
    expressions when building a relational tree.
    """

    @abstractmethod
    def reset(self) -> None:
        """
        Clear any internal state to allow reusing this visitor.
        """

    @abstractmethod
    def visit_call_expression(self, call_expression: CallExpression) -> None:
        """
        Visit a CallExpression node.

        Args:
            call_expression (CallExpression): The call expression node to visit.
        """

    @abstractmethod
    def visit_window_expression(self, window_expression: WindowCallExpression) -> None:
        """
        Visit a WindowCallExpression node.

        Args:
            window_expression (WindowCallExpression): The window call
            expression node to visit.
        """

    @abstractmethod
    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        """
        Visit a LiteralExpression node.

        Args:
            literal_expression (LiteralExpression): The literal expression node to visit.
        """

    @abstractmethod
    def visit_column_reference(self, column_reference: ColumnReference) -> None:
        """
        Visit a ColumnReference node.

        Args:
            column_reference (ColumnReference): The column reference node to visit.
        """

    @abstractmethod
    def visit_correlated_reference(
        self, correlated_reference: CorrelatedReference
    ) -> None:
        """
        Visit a CorrelatedReference node.

        Args:
            correlated_reference (CorrelatedReference): The correlated reference node to visit.
        """
