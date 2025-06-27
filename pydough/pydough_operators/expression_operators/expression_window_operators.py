"""
Definition of PyDough operator class for window functions that return an
expression.
"""

__all__ = ["ExpressionWindowOperator"]


from pydough.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
)

from .expression_function_operators import ExpressionFunctionOperator


class ExpressionWindowOperator(ExpressionFunctionOperator):
    """
    Implementation class for PyDough operators that return an expression
    and represent a window function call, such as `RANKING`.
    """

    def __init__(
        self,
        function_name: str,
        verifier: TypeVerifier,
        deducer: ExpressionTypeDeducer,
        allows_frame: bool = False,
        requires_order: bool = True,
        public: bool = True,
    ):
        super().__init__(function_name, False, verifier, deducer, public)
        self._allows_frame: bool = allows_frame
        self._requires_order: bool = requires_order

    @property
    def key(self) -> str:
        return f"WINDOW_FUNCTION-{self.function_name}"

    @property
    def allows_frame(self) -> bool:
        """
        Whether the window function allows window frames.
        """
        return self._allows_frame

    @property
    def requires_order(self) -> bool:
        """
        Whether the window function requires collation terms.
        """
        return self._requires_order

    @property
    def standalone_string(self) -> str:
        return f"WindowFunction[{self.function_name}]"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ExpressionWindowOperator)
            and self.function_name == other.function_name
        )
