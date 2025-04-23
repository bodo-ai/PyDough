"""
Specialized form of the visitor pattern that returns a RelationalExpression.
This is used to handle the common case where we need to modify a type of
input. Shuttles are defined to be stateless by default.
"""

from abc import ABC, abstractmethod

from .abstract_expression import RelationalExpression
from .call_expression import CallExpression
from .column_reference import ColumnReference
from .correlated_reference import CorrelatedReference
from .expression_sort_info import ExpressionSortInfo
from .literal_expression import LiteralExpression
from .window_call_expression import WindowCallExpression

__all__ = ["RelationalExpressionShuttle"]


class RelationalExpressionShuttle(ABC):
    """
    Representations of a shuttle that returns a RelationalExpression
    at the end of each visit.
    """

    def visit_call_expression(
        self, call_expression: CallExpression
    ) -> RelationalExpression:
        """
        Visit a CallExpression node. This is the default implementation that visits
        all children of the call expression and returns a new call expression with
        the modified children.

        Args:
            call_expression (CallExpression): The call expression node to visit.
        Returns:
            RelationalExpression: The new node resulting from visiting this node.
        """
        from .call_expression import CallExpression

        args = [args.accept_shuttle(self) for args in call_expression.inputs]
        return CallExpression(call_expression.op, call_expression.data_type, args)

    def visit_window_expression(
        self, window_expression: WindowCallExpression
    ) -> RelationalExpression:
        args = [arg.accept_shuttle(self) for arg in window_expression.inputs]
        partition_args = [
            arg.accept_shuttle(self) for arg in window_expression.partition_inputs
        ]
        order_args = [
            ExpressionSortInfo(
                arg.expr.accept_shuttle(self), arg.ascending, arg.nulls_first
            )
            for arg in window_expression.order_inputs
        ]
        return WindowCallExpression(
            window_expression.op,
            window_expression.data_type,
            args,
            partition_args,
            order_args,
            window_expression.kwargs,
        )

    @abstractmethod
    def visit_literal_expression(
        self, literal_expression: LiteralExpression
    ) -> RelationalExpression:
        """
        Visit a LiteralExpression node.

        Args:
            literal_expression (LiteralExpression): The literal expression node to visit.
        Returns:
            RelationalExpression: The new node resulting from visiting this node.
        """

    @abstractmethod
    def visit_column_reference(
        self, column_reference: ColumnReference
    ) -> RelationalExpression:
        """
        Visit a ColumnReference node.

        Args:
            column_reference (ColumnReference): The column reference node to visit.
        Returns:
            RelationalExpression: The new node resulting from visiting this node.
        """

    @abstractmethod
    def visit_correlated_reference(
        self, correlated_reference: CorrelatedReference
    ) -> RelationalExpression:
        """
        Visit a CorrelatedReference node.

        Args:
            correlated_reference (CorrelatedReference): The correlated reference node to visit.
        Returns:
            RelationalExpression: The new node resulting from visiting this node.
        """
