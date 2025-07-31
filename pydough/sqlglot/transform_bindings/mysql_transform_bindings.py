"""
Definition of SQLGlot transformation bindings for the MySQL dialect.
"""

__all__ = ["MySQLTransformBindings"]

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import PyDoughType

from .base_transform_bindings import BaseTransformBindings


class MySQLTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the MySQL dialect.
    """

    PYDOP_TO_MYSQL_FUNC: dict[pydop.PyDoughExpressionOperator, str] = {
        pydop.LPAD: "LPAD",
        pydop.RPAD: "RPAD",
        pydop.SIGN: "SIGN",
        pydop.YEAR: "YEAR",
        pydop.QUARTER: "QUARTER",
        pydop.MONTH: "MONTH",
        pydop.DAY: "DAY",
        pydop.HOUR: "HOUR",
        pydop.MINUTE: "MINUTE",
        pydop.SECOND: "SECOND",
        pydop.DAYNAME: "DAYNAME",
        pydop.SMALLEST: "LEAST",
        pydop.LARGEST: "GREATEST",
    }

    """
    Mapping of PyDough operators to equivalent MySQL function names
    These are used to generate anonymous function calls in SQLGlot
    """

    def convert_call_to_sqlglot(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        if operator in self.PYDOP_TO_MYSQL_FUNC:
            return sqlglot_expressions.Anonymous(
                this=self.PYDOP_TO_MYSQL_FUNC[operator], expressions=args
            )

        return super().convert_call_to_sqlglot(operator, args, types)

    def convert_slice(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        Convert a PyDough slice operation to a SQLGlot expression.
        MySQL uses the SUBSTRING function for slicing.

        Outline of the logic:
        - If the start index is None, it defaults to 1 (1-based indexing).
        - If the stop index is None, it defaults to the length of the string.
        - a = start index
        - b = stop index
        match (a, b):
            case (None, None):
                return SUBSTRING(x, 1)
            case (+a, None):
                return SUBSTRING(x, a + 1)
            case (-a, None):
                return SUBSTRING(x, a)
            case (None, +b):
                return SUBSTRING(x, 1, b)
            case (None, -b):
                return SUBSTRING(x, 1, LENGTH(x) + b)
            case (+a, +b):
                return SUBSTRING(x, a + 1, GREATEST(b - a, 0))
            case (-a, -b):
                return SUBSTRING(x, a, GREATEST(b - a, 0))
            case (+a, -b):
                return SUBSTRING(x, a + 1, GREATEST(LENGTH(x) + b - a, 0))
            case (-a, +b):
                return SUBSTRING(x, a, b - GREATEST(LENGTH(x) + a, 0))
        """

        assert len(args) == 4
        string_expr, start, stop, step = args

        start_idx: int | None = None
        if not isinstance(start, sqlglot_expressions.Null):
            if isinstance(start, sqlglot_expressions.Literal):
                try:
                    start_idx = int(start.this)
                except ValueError:
                    raise ValueError(
                        "SLICE function currently only supports the start index being integer literal or absent."
                    )
            else:
                raise ValueError(
                    "SLICE function currently only supports the start index being integer literal or absent."
                )

        stop_idx: int | None = None
        if not isinstance(stop, sqlglot_expressions.Null):
            if isinstance(stop, sqlglot_expressions.Literal):
                try:
                    stop_idx = int(stop.this)
                except ValueError:
                    raise ValueError(
                        "SLICE function currently only supports the stop index being integer literal or absent."
                    )
            else:
                raise ValueError(
                    "SLICE function currently only supports the stop index being integer literal or absent."
                )

        step_idx: int | None = None
        if not isinstance(step, sqlglot_expressions.Null):
            if isinstance(step, sqlglot_expressions.Literal):
                try:
                    step_idx = int(step.this)
                    if step_idx != 1:
                        raise ValueError(
                            "SLICE function currently only supports the step being integer literal 1 or absent."
                        )
                except ValueError:
                    raise ValueError(
                        "SLICE function currently only supports the step being integer literal 1 or absent."
                    )
            else:
                raise ValueError(
                    "SLICE function currently only supports the step being integer literal 1 or absent."
                )

        # SQLGlot expressions for 0 and 1 and empty string
        sql_one: SQLGlotExpression = sqlglot_expressions.Literal.number(1)
        sql_zero: SQLGlotExpression = sqlglot_expressions.Literal.number(0)
        expr_length: SQLGlotExpression = sqlglot_expressions.Length(this=string_expr)
        one_index_start: SQLGlotExpression = sqlglot_expressions.Add(
            this=start, expression=sql_one
        )
        # length adjustment
        length: SQLGlotExpression = None

        match (start_idx, stop_idx):
            case (None, e) if e is not None and e >= 0:
                length = stop

            case (None, e) if e is not None and e < 0:
                length = sqlglot_expressions.Add(this=expr_length, expression=stop)

            case (s, e) if s is not None and e is not None and s >= 0 and e >= 0:
                length = sqlglot_expressions.Greatest(
                    this=sqlglot_expressions.Sub(this=stop, expression=start),
                    expressions=[sql_zero],
                )

            case (s, e) if s is not None and e is not None and s < 0 and e < 0:
                length = sqlglot_expressions.Greatest(
                    this=sqlglot_expressions.Sub(this=stop, expression=start),
                    expressions=[sql_zero],
                )

            case (s, e) if s is not None and e is not None and s >= 0 and e < 0:
                length = sqlglot_expressions.Greatest(
                    this=sqlglot_expressions.Sub(
                        this=sqlglot_expressions.Add(this=expr_length, expression=stop),
                        expression=start,
                    ),
                    expressions=[sql_zero],
                )

            case (s, e) if s is not None and e is not None and s < 0 and e >= 0:
                length = sqlglot_expressions.Sub(
                    this=stop,
                    expression=sqlglot_expressions.Greatest(
                        this=sqlglot_expressions.Add(
                            this=expr_length, expression=start
                        ),
                        expressions=[sql_zero],
                    ),
                )

        # start adjustment
        if start_idx is not None and start_idx >= 0:
            start = one_index_start
        elif start_idx is None:
            start = sql_one

        result: SQLGlotExpression = sqlglot_expressions.Substring(
            this=string_expr, start=start, length=length
        )

        return result
