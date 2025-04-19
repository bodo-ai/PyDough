"""
The representation of a column function call for use in a relational tree.
"""

__all__ = ["WindowCallExpression"]


from pydough.pydough_operators import ExpressionWindowOperator
from pydough.types import PyDoughType

from .abstract_expression import RelationalExpression
from .expression_sort_info import ExpressionSortInfo


class WindowCallExpression(RelationalExpression):
    """
    The Expression implementation for calling a function
    on a relational node.
    """

    def __init__(
        self,
        op: ExpressionWindowOperator,
        return_type: PyDoughType,
        inputs: list[RelationalExpression],
        partition_inputs: list[RelationalExpression],
        order_inputs: list[ExpressionSortInfo],
        kwargs: dict[str, object],
    ) -> None:
        super().__init__(return_type)
        self._op: ExpressionWindowOperator = op
        self._inputs: list[RelationalExpression] = inputs
        self._partition_inputs: list[RelationalExpression] = partition_inputs
        self._order_inputs: list[ExpressionSortInfo] = order_inputs
        self._kwargs: dict[str, object] = kwargs

    @property
    def op(self) -> ExpressionWindowOperator:
        """
        The operation this call expression represents.
        """
        return self._op

    @property
    def is_aggregation(self) -> bool:
        return self.op.is_aggregation

    @property
    def inputs(self) -> list[RelationalExpression]:
        """
        The inputs to the operation.
        """
        return self._inputs

    @property
    def partition_inputs(self) -> list[RelationalExpression]:
        """
        The inputs used to partition the operation.
        """
        return self._partition_inputs

    @property
    def order_inputs(self) -> list[ExpressionSortInfo]:
        """
        The inputs to order the operation.
        """
        return self._order_inputs

    @property
    def kwargs(self) -> dict[str, object]:
        """
        The keyword arguments to the operation.
        """
        return self._kwargs

    def to_string(self, compact: bool = False) -> str:
        if compact:
            arg_strings: list[str] = [arg.to_string(compact) for arg in self.inputs]
            partition_strings: list[str] = [
                arg.to_string(compact) for arg in self.partition_inputs
            ]
            order_strings: list[str] = [
                arg.to_string(compact) for arg in self.order_inputs
            ]
            arg_string_lists: list[str] = [
                f"args=[{', '.join(arg_strings)}]",
                f"partition=[{', '.join(partition_strings)}]",
                f"order=[{', '.join(order_strings)}]",
            ]
            for kwarg in self.kwargs:
                arg_string_lists.append(f"{kwarg}={self.kwargs[kwarg]}")
            return self.op.to_string(arg_string_lists)
        else:
            return f"WindowCall(op={self.op}, inputs={self.inputs}, partition={self.partition_inputs}, order={self.order_inputs}, return_type={self.data_type}, kwargs={self.kwargs})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, WindowCallExpression)
            and (self.op == other.op)
            and (self.inputs == other.inputs)
            and super().equals(other)
        )

    def accept(self, visitor: "RelationalExpressionVisitor") -> None:  # type: ignore # noqa
        visitor.visit_window_expression(self)

    def accept_shuttle(
        self,
        shuttle: "RelationalExpressionShuttle",  # type: ignore # noqa
    ) -> RelationalExpression:
        return shuttle.visit_window_expression(self)
