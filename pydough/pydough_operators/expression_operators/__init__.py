"""
Submodule of PyDough operator module dealing with operators that return
expressions.
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
    "BinOp",
    "BinaryOperator",
    "CONTAINS",
    "COUNT",
    "DATEDIFF",
    "DATETIME",
    "DAY",
    "DEFAULT_TO",
    "DIV",
    "ENDSWITH",
    "EQU",
    "ExpressionFunctionOperator",
    "ExpressionWindowOperator",
    "FIND",
    "GEQ",
    "GRT",
    "HAS",
    "HASNOT",
    "HOUR",
    "IFF",
    "ISIN",
    "JOIN_STRINGS",
    "KEEP_IF",
    "LENGTH",
    "LEQ",
    "LET",
    "LIKE",
    "LOWER",
    "LPAD",
    "MAX",
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
    "POW",
    "POWER",
    "PRESENT",
    "PREV",
    "PyDoughExpressionOperator",
    "RANKING",
    "ROUND",
    "RPAD",
    "SECOND",
    "SIGN",
    "SLICE",
    "SQRT",
    "STARTSWITH",
    "STRIP",
    "SUB",
    "SUM",
    "UPPER",
    "YEAR",
]

from .binary_operators import BinaryOperator, BinOp
from .expression_function_operators import ExpressionFunctionOperator
from .expression_operator import PyDoughExpressionOperator
from .expression_window_operators import ExpressionWindowOperator
from .registered_expression_operators import (
    ABS,
    ABSENT,
    ADD,
    ANYTHING,
    AVG,
    BAN,
    BOR,
    BXR,
    CONTAINS,
    COUNT,
    DATEDIFF,
    DATETIME,
    DAY,
    DEFAULT_TO,
    DIV,
    ENDSWITH,
    EQU,
    FIND,
    GEQ,
    GRT,
    HAS,
    HASNOT,
    HOUR,
    IFF,
    ISIN,
    JOIN_STRINGS,
    KEEP_IF,
    LENGTH,
    LEQ,
    LET,
    LIKE,
    LOWER,
    LPAD,
    MAX,
    MIN,
    MINUTE,
    MOD,
    MONOTONIC,
    MONTH,
    MUL,
    NDISTINCT,
    NEQ,
    NEXT,
    NOT,
    PERCENTILE,
    POW,
    POWER,
    PRESENT,
    PREV,
    RANKING,
    ROUND,
    RPAD,
    SECOND,
    SIGN,
    SLICE,
    SQRT,
    STARTSWITH,
    STRIP,
    SUB,
    SUM,
    UPPER,
    YEAR,
)
