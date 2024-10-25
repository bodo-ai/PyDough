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
]

from .binary_operators import BinaryOperator, BinOp
from pydough.pydough_ast.pydough_operators.type_inference import (
    NumArgs,
    SelectArgumentType,
)

# TODO: replace with full argument verifiers & deducers
ADD = BinaryOperator(BinOp.ADD, NumArgs(2), SelectArgumentType(2))
SUB = BinaryOperator(BinOp.SUB, NumArgs(2), SelectArgumentType(2))
MUL = BinaryOperator(BinOp.MUL, NumArgs(2), SelectArgumentType(2))
DIV = BinaryOperator(BinOp.DIV, NumArgs(2), SelectArgumentType(2))
POW = BinaryOperator(BinOp.POW, NumArgs(2), SelectArgumentType(2))
MOD = BinaryOperator(BinOp.MOD, NumArgs(2), SelectArgumentType(2))
LET = BinaryOperator(BinOp.LET, NumArgs(2), SelectArgumentType(2))
LEQ = BinaryOperator(BinOp.LEQ, NumArgs(2), SelectArgumentType(2))
EQU = BinaryOperator(BinOp.EQU, NumArgs(2), SelectArgumentType(2))
NEQ = BinaryOperator(BinOp.NEQ, NumArgs(2), SelectArgumentType(2))
GEQ = BinaryOperator(BinOp.GEQ, NumArgs(2), SelectArgumentType(2))
GRT = BinaryOperator(BinOp.GRT, NumArgs(2), SelectArgumentType(2))
BAN = BinaryOperator(BinOp.BAN, NumArgs(2), SelectArgumentType(2))
BOR = BinaryOperator(BinOp.BOR, NumArgs(2), SelectArgumentType(2))
BXR = BinaryOperator(BinOp.BXR, NumArgs(2), SelectArgumentType(2))
