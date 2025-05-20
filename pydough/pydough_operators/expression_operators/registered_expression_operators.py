"""
Definition bindings of builtin PyDough operators that return an expression.
"""

__all__ = [
    "ABS",
    "ABSENT",
    "ADD",
    "ANYTHING",
    "AVG",
    "BAN",
    "BOR",
    "BXR",
    "CONTAINS",
    "COUNT",
    "DATEDIFF",
    "DATETIME",
    "DAY",
    "DAYNAME",
    "DAYOFWEEK",
    "DEFAULT_TO",
    "DIV",
    "ENDSWITH",
    "EQU",
    "FIND",
    "FLOAT",
    "GEQ",
    "GRT",
    "HAS",
    "HASNOT",
    "HOUR",
    "IFF",
    "INTEGER",
    "ISIN",
    "JOIN_STRINGS",
    "KEEP_IF",
    "LARGEST",
    "LENGTH",
    "LEQ",
    "LET",
    "LIKE",
    "LOWER",
    "LPAD",
    "MAX",
    "MEDIAN",
    "MIN",
    "MINUTE",
    "MOD",
    "MONOTONIC",
    "MONTH",
    "MUL",
    "NDISTINCT",
    "NEQ",
    "NEXT",
    "NOT",
    "PERCENTILE",
    "POPULATION_STD",
    "POPULATION_VARIANCE",
    "POW",
    "POWER",
    "PRESENT",
    "PREV",
    "QUARTER",
    "RANKING",
    "RELAVG",
    "RELCOUNT",
    "RELSIZE",
    "RELSUM",
    "ROUND",
    "RPAD",
    "SAMPLE_STD",
    "SAMPLE_VARIANCE",
    "SECOND",
    "SIGN",
    "SLICE",
    "SMALLEST",
    "SQRT",
    "STARTSWITH",
    "STD",
    "STRING",
    "STRIP",
    "SUB",
    "SUM",
    "UPPER",
    "VAR",
    "YEAR",
]

from pydough.pydough_operators.type_inference import (
    AllowAny,
    ConstantType,
    RequireArgRange,
    RequireCollection,
    RequireMinArgs,
    RequireNumArgs,
    SelectArgumentType,
)
from pydough.types import BooleanType, DatetimeType, NumericType, StringType

from .binary_operators import BinaryOperator, BinOp
from .expression_function_operators import ExpressionFunctionOperator
from .expression_window_operators import ExpressionWindowOperator
from .keyword_branching_operators import KeywordBranchingExpressionFunctionOperator

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
    "LENGTH", False, RequireNumArgs(1), ConstantType(NumericType())
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
STRIP = ExpressionFunctionOperator(
    "STRIP", False, RequireArgRange(1, 2), SelectArgumentType(0)
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
    "AVG", True, RequireNumArgs(1), ConstantType(NumericType())
)
MEDIAN = ExpressionFunctionOperator(
    "MEDIAN", True, RequireNumArgs(1), ConstantType(NumericType())
)
POWER = ExpressionFunctionOperator(
    "POWER", False, RequireNumArgs(2), ConstantType(NumericType())
)
SQRT = ExpressionFunctionOperator(
    "SQRT", False, RequireNumArgs(1), ConstantType(NumericType())
)
SIGN = ExpressionFunctionOperator(
    "SIGN", False, RequireNumArgs(1), ConstantType(NumericType())
)
COUNT = ExpressionFunctionOperator(
    "COUNT", True, AllowAny(), ConstantType(NumericType())
)
HAS = ExpressionFunctionOperator(
    "HAS", True, RequireCollection(), ConstantType(BooleanType())
)
HASNOT = ExpressionFunctionOperator(
    "HASNOT", True, RequireCollection(), ConstantType(BooleanType())
)
NDISTINCT = ExpressionFunctionOperator(
    "NDISTINCT", True, AllowAny(), ConstantType(NumericType())
)
ANYTHING = ExpressionFunctionOperator(
    "ANYTHING", True, RequireNumArgs(1), SelectArgumentType(0)
)
MIN = ExpressionFunctionOperator("MIN", True, RequireNumArgs(1), SelectArgumentType(0))
MAX = ExpressionFunctionOperator("MAX", True, RequireNumArgs(1), SelectArgumentType(0))
SMALLEST = ExpressionFunctionOperator(
    "SMALLEST", False, RequireMinArgs(2), SelectArgumentType(0)
)
LARGEST = ExpressionFunctionOperator(
    "LARGEST", False, RequireMinArgs(2), SelectArgumentType(0)
)
IFF = ExpressionFunctionOperator("IFF", False, RequireNumArgs(3), SelectArgumentType(1))
DATETIME = ExpressionFunctionOperator(
    "DATETIME", False, AllowAny(), ConstantType(DatetimeType())
)
YEAR = ExpressionFunctionOperator(
    "YEAR", False, RequireNumArgs(1), ConstantType(NumericType())
)
QUARTER = ExpressionFunctionOperator(
    "QUARTER", False, RequireNumArgs(1), ConstantType(NumericType())
)
MONTH = ExpressionFunctionOperator(
    "MONTH", False, RequireNumArgs(1), ConstantType(NumericType())
)
DAY = ExpressionFunctionOperator(
    "DAY", False, RequireNumArgs(1), ConstantType(NumericType())
)
DAYOFWEEK = ExpressionFunctionOperator(
    "DAYOFWEEK", False, RequireNumArgs(1), ConstantType(NumericType())
)
DAYNAME = ExpressionFunctionOperator(
    "DAYNAME", False, RequireNumArgs(1), ConstantType(StringType())
)
HOUR = ExpressionFunctionOperator(
    "HOUR", False, RequireNumArgs(1), ConstantType(NumericType())
)
MINUTE = ExpressionFunctionOperator(
    "MINUTE", False, RequireNumArgs(1), ConstantType(NumericType())
)
SECOND = ExpressionFunctionOperator(
    "SECOND", False, RequireNumArgs(1), ConstantType(NumericType())
)
DATEDIFF = ExpressionFunctionOperator(
    "DATEDIFF", False, RequireNumArgs(3), ConstantType(NumericType())
)
SLICE = ExpressionFunctionOperator(
    "SLICE", False, RequireNumArgs(4), SelectArgumentType(0)
)
LPAD = ExpressionFunctionOperator(
    "LPAD", False, RequireNumArgs(3), SelectArgumentType(0)
)
RPAD = ExpressionFunctionOperator(
    "RPAD", False, RequireNumArgs(3), SelectArgumentType(0)
)
FIND = ExpressionFunctionOperator(
    "FIND", False, RequireNumArgs(2), ConstantType(NumericType())
)
NOT = ExpressionFunctionOperator(
    "NOT", False, RequireNumArgs(1), ConstantType(BooleanType())
)
ISIN = ExpressionFunctionOperator(
    "ISIN", False, RequireNumArgs(2), ConstantType(BooleanType())
)
ABSENT = ExpressionFunctionOperator(
    "ABSENT", False, RequireNumArgs(1), ConstantType(BooleanType())
)
PRESENT = ExpressionFunctionOperator(
    "PRESENT", False, RequireNumArgs(1), ConstantType(BooleanType())
)
ROUND = ExpressionFunctionOperator(
    "ROUND", False, RequireArgRange(1, 2), SelectArgumentType(0)
)
MONOTONIC = ExpressionFunctionOperator(
    "MONOTONIC", False, RequireMinArgs(1), ConstantType(BooleanType())
)
KEEP_IF = ExpressionFunctionOperator(
    "KEEP_IF", False, RequireNumArgs(2), SelectArgumentType(0)
)
JOIN_STRINGS = ExpressionFunctionOperator(
    "JOIN_STRINGS", False, RequireMinArgs(1), ConstantType(StringType())
)
ABS = ExpressionFunctionOperator("ABS", False, RequireNumArgs(1), SelectArgumentType(0))

# Define VAR with keyword branching
VAR = KeywordBranchingExpressionFunctionOperator(
    "VAR",
    True,
    RequireNumArgs(1),
    ConstantType(NumericType()),
    kwarg_defaults={"type": "population"},
)
# Define VAR with keyword branching for "type" which is represented internally.
POPULATION_VARIANCE = VAR.with_kwarg("POPULATION_VARIANCE", {"type": "population"})
SAMPLE_VARIANCE = VAR.with_kwarg("SAMPLE_VARIANCE", {"type": "sample"})

# Define STD with keyword branching
STD = KeywordBranchingExpressionFunctionOperator(
    "STD",
    True,
    RequireNumArgs(1),
    ConstantType(NumericType()),
    kwarg_defaults={"type": "population"},
)
# Define STD with keyword branching for "type" which is represented internally.
POPULATION_STD = STD.with_kwarg("POPULATION_STD", {"type": "population"})
SAMPLE_STD = STD.with_kwarg("SAMPLE_STD", {"type": "sample"})

RANKING = ExpressionWindowOperator(
    "RANKING", RequireNumArgs(0), ConstantType(NumericType())
)
PERCENTILE = ExpressionWindowOperator(
    "PERCENTILE", RequireNumArgs(0), ConstantType(NumericType())
)
PREV = ExpressionWindowOperator("PREV", RequireNumArgs(1), SelectArgumentType(0))
NEXT = ExpressionWindowOperator("NEXT", RequireNumArgs(1), SelectArgumentType(0))
RELSUM = ExpressionWindowOperator(
    "RELSUM", RequireNumArgs(1), SelectArgumentType(0), True, False
)
RELAVG = ExpressionWindowOperator(
    "RELAVG", RequireNumArgs(1), SelectArgumentType(0), True, False
)
RELCOUNT = ExpressionWindowOperator(
    "RELCOUNT", RequireNumArgs(1), ConstantType(NumericType()), True, False
)
RELSIZE = ExpressionWindowOperator(
    "RELSIZE", RequireNumArgs(0), ConstantType(NumericType()), True, False
)
INTEGER = ExpressionFunctionOperator(
    "INTEGER", False, RequireNumArgs(1), ConstantType(NumericType())
)
FLOAT = ExpressionFunctionOperator(
    "FLOAT", False, RequireNumArgs(1), ConstantType(NumericType())
)
STRING = ExpressionFunctionOperator(
    "STRING", False, RequireArgRange(1, 2), ConstantType(StringType())
)
