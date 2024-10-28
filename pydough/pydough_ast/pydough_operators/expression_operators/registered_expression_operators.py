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
    "SUM",
]

from .binary_operators import BinaryOperator, BinOp
from .expression_function_operators import ExpressionFunctionCall
from pydough.pydough_ast.pydough_operators.type_inference import (
    NumArgs,
    SelectArgumentType,
)

# TODO: replace with full argument verifiers & deducers
ADD = BinaryOperator(BinOp.ADD, NumArgs(2), SelectArgumentType(0))
SUB = BinaryOperator(BinOp.SUB, NumArgs(2), SelectArgumentType(0))
MUL = BinaryOperator(BinOp.MUL, NumArgs(2), SelectArgumentType(0))
DIV = BinaryOperator(BinOp.DIV, NumArgs(2), SelectArgumentType(0))
POW = BinaryOperator(BinOp.POW, NumArgs(2), SelectArgumentType(0))
MOD = BinaryOperator(BinOp.MOD, NumArgs(2), SelectArgumentType(0))
LET = BinaryOperator(BinOp.LET, NumArgs(2), SelectArgumentType(0))
LEQ = BinaryOperator(BinOp.LEQ, NumArgs(2), SelectArgumentType(0))
EQU = BinaryOperator(BinOp.EQU, NumArgs(2), SelectArgumentType(0))
NEQ = BinaryOperator(BinOp.NEQ, NumArgs(2), SelectArgumentType(0))
GEQ = BinaryOperator(BinOp.GEQ, NumArgs(2), SelectArgumentType(0))
GRT = BinaryOperator(BinOp.GRT, NumArgs(2), SelectArgumentType(0))
BAN = BinaryOperator(BinOp.BAN, NumArgs(2), SelectArgumentType(0))
BOR = BinaryOperator(BinOp.BOR, NumArgs(2), SelectArgumentType(0))
BXR = BinaryOperator(BinOp.BXR, NumArgs(2), SelectArgumentType(0))
LOWER = ExpressionFunctionCall("LOWER", False, NumArgs(1), SelectArgumentType(0))
SUM = ExpressionFunctionCall("SUM", True, NumArgs(1), SelectArgumentType(0))
