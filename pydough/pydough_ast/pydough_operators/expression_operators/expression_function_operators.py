"""
TODO: add file-level docstring
"""

__all__ = ["ExpressionFunctionOperator"]


from pydough.pydough_ast.expressions import PyDoughExpressionAST
from pydough.pydough_ast.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
)

from .expression_operator_ast import PyDoughExpressionOperatorAST


class ExpressionFunctionOperator(PyDoughExpressionOperatorAST):
    """
    Implementation class for PyDough operators that return an expression
    and represent a function call, such as `LOWER` or `SUM`.
    """

    def __init__(
        self,
        function_name: str,
        is_aggregation: bool,
        verifier: TypeVerifier,
        deducer: ExpressionTypeDeducer,
    ):
        super().__init__(verifier, deducer)
        self._function_name: str = function_name
        self._is_aggregation: bool = is_aggregation

    @property
    def key(self) -> str:
        return f"FUNCTION-{self.function_name}"

    @property
    def is_aggregation(self) -> bool:
        return self._is_aggregation

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def standalone_string(self) -> str:
        return f"Function[{self.function_name}]"

    def requires_enclosing_parens(self, parent: PyDoughExpressionAST) -> bool:
        return False

    def to_string(self, arg_strings: list[str]) -> str:
        # Stringify as "function_name(arg0, arg1, ...)
        return f"{self.function_name}({', '.join(arg_strings)})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ExpressionFunctionOperator)
            and self.function_name == other.function_name
            and self.is_aggregation == other.is_aggregation
        )
