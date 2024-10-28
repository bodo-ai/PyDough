"""
TODO: add file-level docstring
"""

__all__ = [
    "ADD",
    "BAN",
    "BOR",
    "BXR",
    "DIV",
    "EQU",
    "GEQ",
    "GRT",
    "LEQ",
    "LET",
    "MOD",
    "MUL",
    "NEQ",
    "POW",
    "SUB",
    "LOWER",
    "IFF",
    "SUM",
]

from .binary_operators import BinaryOperator, BinOp
from .expression_function_operators import ExpressionFunctionOperator
from pydough.pydough_ast.pydough_operators.type_inference import (
    RequireNumArgs,
    SelectArgumentType,
    ConstantType,
)
from pydough.types import BooleanType

# TODO: replace with full argument verifiers & deducers
ADD = BinaryOperator(BinOp.ADD, RequireNumArgs(2), SelectArgumentType(0))
SUB = BinaryOperator(BinOp.SUB, RequireNumArgs(2), SelectArgumentType(0))
MUL = BinaryOperator(BinOp.MUL, RequireNumArgs(2), SelectArgumentType(0))
DIV = BinaryOperator(BinOp.DIV, RequireNumArgs(2), SelectArgumentType(0))
POW = BinaryOperator(BinOp.POW, RequireNumArgs(2), SelectArgumentType(0))
MOD = BinaryOperator(BinOp.MOD, RequireNumArgs(2), SelectArgumentType(0))
LET = BinaryOperator(BinOp.LET, RequireNumArgs(2), ConstantType(BooleanType()))
LEQ = BinaryOperator(BinOp.LEQ, RequireNumArgs(2), ConstantType(BooleanType()))
EQU = BinaryOperator(BinOp.EQU, RequireNumArgs(2), ConstantType(BooleanType()))
NEQ = BinaryOperator(BinOp.NEQ, RequireNumArgs(2), ConstantType(BooleanType()))
GEQ = BinaryOperator(BinOp.GEQ, RequireNumArgs(2), ConstantType(BooleanType()))
GRT = BinaryOperator(BinOp.GRT, RequireNumArgs(2), ConstantType(BooleanType()))
BAN = BinaryOperator(BinOp.BAN, RequireNumArgs(2), SelectArgumentType(0))
BOR = BinaryOperator(BinOp.BOR, RequireNumArgs(2), SelectArgumentType(0))
BXR = BinaryOperator(BinOp.BXR, RequireNumArgs(2), SelectArgumentType(0))
LOWER = ExpressionFunctionOperator(
    "LOWER", False, RequireNumArgs(1), SelectArgumentType(0)
)
SUM = ExpressionFunctionOperator("SUM", True, RequireNumArgs(1), SelectArgumentType(0))
IFF = ExpressionFunctionOperator("IFF", False, RequireNumArgs(3), SelectArgumentType(1))
