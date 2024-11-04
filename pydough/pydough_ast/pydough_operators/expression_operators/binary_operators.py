"""
TODO: add file-level docstring
"""

__all__ = ["BinOp", "BinaryOperator"]

from typing import List

from pydough.pydough_ast.expressions import PyDoughExpressionAST
from .expression_operator_ast import PyDoughExpressionOperatorAST
from pydough.pydough_ast.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
)

from enum import Enum


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


class BinaryOperator(PyDoughExpressionOperatorAST):
    """
    Implementation class for PyDough operators that return an expression
    and represent a binary operation, such as addition.
    """

    def __init__(
        self, binop: BinOp, verifier: TypeVerifier, deducer: ExpressionTypeDeducer
    ):
        super().__init__(verifier, deducer)
        self._binop: BinOp = binop

    @property
    def binop(self) -> BinOp:
        """
        The binary operation enum that this operator corresponds to.
        """
        return self._binop

    @property
    def is_aggregation(self) -> bool:
        return False

    @property
    def standalone_string(self) -> str:
        return f"BinaryOperator[{self.binop.value}]"

    def requires_enclosing_parens(self, parent: PyDoughExpressionAST) -> bool:
        # For now, until proper precedence is handled, always enclose binary
        # operations in parenthesis if the parent is also a binary operation.

        from pydough.pydough_ast import ExpressionFunctionCall

        return isinstance(parent, ExpressionFunctionCall) and isinstance(
            parent.operator, BinaryOperator
        )

    def to_string(self, arg_strings: List[str]) -> str:
        # Stringify as "? + ?" for 0 arguments, "a + ?" for 1 argument, and
        # "a + b + ..." for 2+ arguments
        if len(arg_strings) < 2:
            arg_strings = arg_strings + ["?"] * (2 - len(arg_strings))
        return f" {self.binop.value} ".join(arg_strings)

    def equals(self, other: "BinaryOperator") -> bool:
        return super().equals(other) and self.binop == other.binop
