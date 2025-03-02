"""
The basic Visitor pattern to perform operations across the
expression components of a Relational tree. The primary motivation of
this module is to allow associating lowering the Relational expressions
into a specific backend in a single class, but this can also be
used for any other tree based operations (e.g. string generation).

TODO: (gh #172) Fix type annotations. Disabled due to circular imports.
"""

from abc import ABC, abstractmethod

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
    def visit_call_expression(self, call_expression) -> None:
        """
        Visit a CallExpression node.

        Args:
            call_expression (CallExpression): The call expression node to visit.
        """

    @abstractmethod
    def visit_window_expression(self, window_expression) -> None:
        """
        Visit a WindowCallExpression node.

        Args:
            window_expression (WindowCallExpression): The window call
            expression node to visit.
        """

    @abstractmethod
    def visit_literal_expression(self, literal_expression) -> None:
        """
        Visit a LiteralExpression node.

        Args:
            literal_expression (LiteralExpression): The literal expression node to visit.
        """

    @abstractmethod
    def visit_column_reference(self, column_reference) -> None:
        """
        Visit a ColumnReference node.

        Args:
            column_reference (ColumnReference): The column reference node to visit.
        """

    @abstractmethod
    def visit_correlated_reference(self, correlated_reference) -> None:
        """
        Visit a CorrelatedReference node.

        Args:
            correlated_reference (CorrelatedReference): The correlated reference node to visit.
        """
