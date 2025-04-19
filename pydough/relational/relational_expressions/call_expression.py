"""
The representation of a column function call for use in a relational tree.
"""

__all__ = ["CallExpression"]


from pydough.pydough_operators import PyDoughExpressionOperator
from pydough.types import PyDoughType

from .abstract_expression import RelationalExpression


class CallExpression(RelationalExpression):
    """
    The Expression implementation for calling a function
    on a relational node.
    """

    def __init__(
        self,
        op: PyDoughExpressionOperator,
        return_type: PyDoughType,
        inputs: list[RelationalExpression],
    ) -> None:
        super().__init__(return_type)
        self._op: PyDoughExpressionOperator = op
        self._inputs: list[RelationalExpression] = inputs

    @property
    def op(self) -> PyDoughExpressionOperator:
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

    def to_string(self, compact: bool = False) -> str:
        if compact:
            arg_strings: list[str] = [arg.to_string(compact) for arg in self.inputs]
            return self.op.to_string(arg_strings)
        else:
            return f"Call(op={self.op}, inputs={self.inputs}, return_type={self.data_type})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, CallExpression)
            and (self.op == other.op)
            and (self.inputs == other.inputs)
            and super().equals(other)
        )

    def accept(self, visitor: "RelationalExpressionVisitor") -> None:  # type: ignore # noqa
        visitor.visit_call_expression(self)

    def accept_shuttle(
        self,
        shuttle: "RelationalExpressionShuttle",  # type: ignore # noqa
    ) -> RelationalExpression:
        return shuttle.visit_call_expression(self)
