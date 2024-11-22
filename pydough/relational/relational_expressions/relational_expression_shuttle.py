"""
Specialized form of the visitor pattern that returns a RelationalExpression.
This is used to handle the common case where we need to modify a type of
input.

TODO: Fix type annotations. Disabled due to circular imports
"""

from abc import ABC, abstractmethod

__all__ = ["RelationalExpressionShuttle"]


class RelationalExpressionShuttle(ABC):
    """
    Representations of a shuttle that returns a RelationalExpression
    at the end of each visit.
    """

    @abstractmethod
    def visit_call_expression(self, call_expression):
        """
        Visit a CallExpression node.

        Args:
            call_expression (CallExpression): The call expression node to visit.
        Returns:
            RelationalExpression: The new node resulting from visiting this node.
        """

    @abstractmethod
    def visit_literal_expression(self, literal_expression):
        """
        Visit a LiteralExpression node.

        Args:
            literal_expression (LiteralExpression): The literal expression node to visit.
        Returns:
            RelationalExpression: The new node resulting from visiting this node.
        """

    @abstractmethod
    def visit_column_reference(self, column_reference):
        """
        Visit a ColumnReference node.

        Args:
            column_reference (ColumnReference): The column reference node to visit.
        Returns:
            RelationalExpression: The new node resulting from visiting this node.
        """
