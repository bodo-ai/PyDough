"""
Definition of PyDough operator class for binary operations.
"""

__all__ = ["BinOp", "BinaryOperator"]

from enum import Enum

from pydough.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
)

from .expression_operator import PyDoughExpressionOperator


class BinOp(Enum):
    """
    Enum class used to describe the various binary operations
    """

    ADD = "+"
    SUB = "-"
    MUL = "*"
    DIV = "/"
    POW = "**"
    MOD = "%"
    LET = "<"
    LEQ = "<="
    EQU = "=="
    NEQ = "!="
    GEQ = ">="
    GRT = ">"
    BAN = "&"
    BOR = "|"
    BXR = "^"

    @staticmethod
    def from_string(s: str) -> "BinOp":
        """
        Returns the binary operation enum corresponding to the given string.
        """
        for op in BinOp.__members__.values():
            if s == op.value:
                return op
        raise ValueError(f"Unrecognized operation: {s!r}")


BinOp.__members__.items()


class BinaryOperator(PyDoughExpressionOperator):
    """
    Implementation class for PyDough operators that return an expression
    and represent a binary operation, such as addition.
    """

    def __init__(
        self,
        binop: BinOp,
        verifier: TypeVerifier,
        deducer: ExpressionTypeDeducer,
        public: bool = True,
    ):
        super().__init__(verifier, deducer, public)
        self._binop: BinOp = binop

    @property
    def binop(self) -> BinOp:
        """
        The binary operation enum that this operator corresponds to.
        """
        return self._binop

    @property
    def function_name(self) -> str:
        return self.binop.value

    @property
    def key(self) -> str:
        return f"BINOP-{self.binop}"

    @property
    def is_aggregation(self) -> bool:
        return False

    @property
    def standalone_string(self) -> str:
        return f"BinaryOperator[{self.binop.value}]"

    def requires_enclosing_parens(self, parent) -> bool:
        # For now, until proper precedence is handled, always enclose binary
        # operations in parenthesis if the parent is also a binary operation.

        from pydough.qdag import CollationExpression, ExpressionFunctionCall

        return (
            isinstance(parent, ExpressionFunctionCall)
            and isinstance(parent.operator, BinaryOperator)
            or isinstance(parent, CollationExpression)
        )

    def to_string(self, arg_strings: list[str]) -> str:
        # Stringify as "? + ?" for 0 arguments, "a + ?" for 1 argument, and
        # "a + b + ..." for 2+ arguments
        if len(arg_strings) < 2:
            arg_strings = arg_strings + ["?"] * (2 - len(arg_strings))
        return f" {self.binop.value} ".join(arg_strings)

    def equals(self, other: object) -> bool:
        return isinstance(other, BinaryOperator) and self.binop == other.binop
