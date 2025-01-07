"""
Definition of PyDough operator class for window functions that return an
expression.
"""

__all__ = ["ExpressionWindowOperator"]


from pydough.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
)

from .expression_operator import PyDoughExpressionOperator


class ExpressionWindowOperator(PyDoughExpressionOperator):
    """
    Implementation class for PyDough operators that return an expression
    and represent a window function call, such as `RANKING`.
    """

    def __init__(
        self,
        function_name: str,
        verifier: TypeVerifier,
        deducer: ExpressionTypeDeducer,
    ):
        super().__init__(verifier, deducer)
        self._function_name: str = function_name

    @property
    def key(self) -> str:
        return f"WINDOW_FUNCTION-{self.function_name}"

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def is_aggregation(self) -> bool:
        return False

    @property
    def standalone_string(self) -> str:
        return f"WindowFunction[{self.function_name}]"

    def requires_enclosing_parens(self, parent) -> bool:
        return False

    def to_string(self, arg_strings: list[str]) -> str:
        # Stringify as "function_name(arg0, arg1, ...)
        return f"{self.function_name}({', '.join(arg_strings)})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ExpressionWindowOperator)
            and self.function_name == other.function_name
        )
