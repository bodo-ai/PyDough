"""
TODO: add file-level docstring
"""

__all__ = [
    "ABS",
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
    "LENGTH",
    "LOWER",
    "UPPER",
    "IFF",
    "SUM",
    "YEAR",
    "MONTH",
    "DAY",
    "NOT",
    "MIN",
    "MAX",
    "COUNT",
    "STARTSWITH",
    "ENDSWITH",
    "CONTAINS",
    "LIKE",
    "AVG",
    "NDISTINCT",
    "ISIN",
    "SLICE",
    "DEFAULT_TO",
    "HAS",
    "HASNOT",
]

from pydough.pydough_ast.pydough_operators.type_inference import (
    AllowAny,
    ConstantType,
    RequireMinArgs,
    RequireNumArgs,
    SelectArgumentType,
)
from pydough.types import BooleanType, Float64Type, Int64Type

from .binary_operators import BinaryOperator, BinOp
from .expression_function_operators import ExpressionFunctionOperator

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
BAN = BinaryOperator(BinOp.BAN, RequireMinArgs(2), SelectArgumentType(0))
BOR = BinaryOperator(BinOp.BOR, RequireMinArgs(2), SelectArgumentType(0))
BXR = BinaryOperator(BinOp.BXR, RequireMinArgs(2), SelectArgumentType(0))
DEFAULT_TO = ExpressionFunctionOperator(
    "DEFAULT_TO", False, AllowAny(), SelectArgumentType(0)
)
LENGTH = ExpressionFunctionOperator(
    "LENGTH", False, RequireNumArgs(1), ConstantType(Int64Type())
)
LOWER = ExpressionFunctionOperator(
    "LOWER", False, RequireNumArgs(1), SelectArgumentType(0)
)
UPPER = ExpressionFunctionOperator(
    "UPPER", False, RequireNumArgs(1), SelectArgumentType(0)
)
STARTSWITH = ExpressionFunctionOperator(
    "STARTSWITH", False, RequireNumArgs(2), ConstantType(BooleanType())
)
ENDSWITH = ExpressionFunctionOperator(
    "ENDSWITH", False, RequireNumArgs(2), ConstantType(BooleanType())
)
CONTAINS = ExpressionFunctionOperator(
    "CONTAINS", False, RequireNumArgs(2), ConstantType(BooleanType())
)
LIKE = ExpressionFunctionOperator(
    "LIKE", False, RequireNumArgs(2), ConstantType(BooleanType())
)
SUM = ExpressionFunctionOperator("SUM", True, RequireNumArgs(1), SelectArgumentType(0))
AVG = ExpressionFunctionOperator(
    "AVG", True, RequireNumArgs(1), ConstantType(Float64Type())
)
COUNT = ExpressionFunctionOperator("COUNT", True, AllowAny(), ConstantType(Int64Type()))
HAS = ExpressionFunctionOperator("HAS", True, AllowAny(), ConstantType(BooleanType()))
HASNOT = ExpressionFunctionOperator(
    "HASNOT", True, AllowAny(), ConstantType(BooleanType())
)
NDISTINCT = ExpressionFunctionOperator(
    "NDISTINCT", True, AllowAny(), ConstantType(Int64Type())
)
MIN = ExpressionFunctionOperator("MIN", True, RequireNumArgs(1), SelectArgumentType(0))
MAX = ExpressionFunctionOperator("MAX", True, RequireNumArgs(1), SelectArgumentType(0))
IFF = ExpressionFunctionOperator("IFF", False, RequireNumArgs(3), SelectArgumentType(1))
YEAR = ExpressionFunctionOperator(
    "YEAR", False, RequireNumArgs(1), ConstantType(Int64Type())
)
MONTH = ExpressionFunctionOperator(
    "MONTH", False, RequireNumArgs(1), ConstantType(Int64Type())
)
DAY = ExpressionFunctionOperator(
    "DAY", False, RequireNumArgs(1), ConstantType(Int64Type())
)
SLICE = ExpressionFunctionOperator(
    "SLICE", False, RequireNumArgs(4), SelectArgumentType(0)
)
NOT = ExpressionFunctionOperator(
    "NOT", False, RequireNumArgs(1), ConstantType(BooleanType())
)
ISIN = ExpressionFunctionOperator(
    "ISIN", False, RequireNumArgs(2), ConstantType(BooleanType())
)
ABS = ExpressionFunctionOperator("ABS", False, RequireNumArgs(1), SelectArgumentType(0))
