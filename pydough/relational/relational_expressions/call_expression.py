"""
The representation of a column function call for use in a relational tree.
"""

__all__ = ["CallExpression"]

from collections.abc import MutableSequence

from pydough.pydough_ast.pydough_operators import PyDoughExpressionOperatorAST
from pydough.types import PyDoughType

from .abstract_expression import RelationalExpression


class CallExpression(RelationalExpression):
    """
    The Expression implementation for calling a function
    on a relational node.
    """

    def __init__(
        self,
        op: PyDoughExpressionOperatorAST,
        return_type: PyDoughType,
        inputs: MutableSequence[RelationalExpression],
    ) -> None:
        super().__init__(return_type)
        # TODO: Should we move this op to a different type?
        self._op: PyDoughExpressionOperatorAST = op
        self._inputs: MutableSequence[RelationalExpression] = inputs

    @property
    def op(self) -> PyDoughExpressionOperatorAST:
        """
        The operation this call expression represents.
        """
        return self._op

    @property
    def is_aggregation(self) -> bool:
        return self.op.is_aggregation

    @property
    def inputs(self) -> MutableSequence[RelationalExpression]:
        """
        The inputs to the operation.
        """
        return self._inputs

    def to_string(self) -> str:
        return f"Call(op={self.op}, inputs={self.inputs}, return_type={self.data_type})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, CallExpression)
            and (self.op == other.op)
            and (self.inputs == other.inputs)
            and super().equals(other)
        )
