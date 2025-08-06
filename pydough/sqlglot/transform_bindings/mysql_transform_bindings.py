"""
Definition of SQLGlot transformation bindings for the MySQL dialect.
"""

__all__ = ["MySQLTransformBindings"]

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import PyDoughType

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import (
    apply_parens,
)


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

    def convert_get_part(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        GETPART(str, delim, idx) ->
            CASE
                WHEN LENGTH(str) = 0 THEN NULL
                WHEN LENGTH(delim) = 0 THEN
                    CASE
                        WHEN ABS(idx) = 1 THEN str
                        ELSE NULL
                    END
                WHEN idx > 0 AND idx <= (LENGTH(str) - LENGTH(REPLACE(str, delim, '')))/LENGTH(delim) + 1
                    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, idx), delim, -1)
                WHEN idx < 0 AND -idx <= (LENGTH(str) - LENGTH(REPLACE(str, delim, '')))/LENGTH(delim) + 1
                    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, idx), delim, 1)
                WHEN idx = 0 THEN SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, 1), delim, -1)
                ELSE NULL
            END
        """

        assert len(args) == 3

        string_expr, delimiter_expr, index_expr = args
        literal_1: SQLGlotExpression = sqlglot_expressions.Literal.number(1)
        literal_neg_1: SQLGlotExpression = sqlglot_expressions.Literal.number(-1)
        literal_0: SQLGlotExpression = sqlglot_expressions.Literal.number(0)

        # (LENGTH(str) - LENGTH(REPLACE(str, delim, '')))/LENGTH(delim) + 1
        difference: SQLGlotExpression = sqlglot_expressions.Sub(
            this=sqlglot_expressions.Length(this=string_expr),
            expression=sqlglot_expressions.Length(
                this=self.convert_replace([string_expr, delimiter_expr], types[:2])
            ),
        )

        index_expr = apply_parens(index_expr)

        delimiter_count: SQLGlotExpression = sqlglot_expressions.Div(
            this=apply_parens(difference),
            expression=sqlglot_expressions.Length(this=delimiter_expr),
        )
        total_parts = sqlglot_expressions.Add(
            this=delimiter_count, expression=literal_1
        )

        # SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, idx), delim, -1)
        pos_index_case: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="SUBSTRING_INDEX",
            expressions=[
                sqlglot_expressions.Anonymous(
                    this="SUBSTRING_INDEX",
                    expressions=[string_expr, delimiter_expr, index_expr],
                ),
                delimiter_expr,
                literal_neg_1,
            ],
        )

        # SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, idx), delim, 1)
        neg_index_case: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="SUBSTRING_INDEX",
            expressions=[
                sqlglot_expressions.Anonymous(
                    this="SUBSTRING_INDEX",
                    expressions=[string_expr, delimiter_expr, index_expr],
                ),
                delimiter_expr,
                literal_1,
            ],
        )

        # SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, 1), delim, -1)
        zero_index_case: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="SUBSTRING_INDEX",
            expressions=[
                sqlglot_expressions.Anonymous(
                    this="SUBSTRING_INDEX",
                    expressions=[string_expr, delimiter_expr, literal_1],
                ),
                delimiter_expr,
                literal_neg_1,
            ],
        )

        result: SQLGlotExpression = sqlglot_expressions.Case(
            ifs=[
                sqlglot_expressions.If(
                    this=sqlglot_expressions.EQ(
                        this=sqlglot_expressions.Length(this=string_expr),
                        expression=literal_0,
                    ),
                    true=sqlglot_expressions.Null(),
                ),
                sqlglot_expressions.If(
                    this=sqlglot_expressions.EQ(
                        this=sqlglot_expressions.Length(this=delimiter_expr),
                        expression=literal_0,
                    ),
                    true=sqlglot_expressions.Case(
                        ifs=[
                            sqlglot_expressions.If(
                                this=sqlglot_expressions.EQ(
                                    this=sqlglot_expressions.Abs(this=index_expr),
                                    expression=literal_1,
                                ),
                                true=string_expr,
                            )
                        ],
                        default=sqlglot_expressions.Null(),
                    ),
                ),
                sqlglot_expressions.If(
                    this=sqlglot_expressions.And(
                        this=sqlglot_expressions.GT(
                            this=index_expr, expression=literal_0
                        ),
                        expression=sqlglot_expressions.LTE(
                            this=index_expr, expression=total_parts
                        ),
                    ),
                    true=pos_index_case,
                ),
                sqlglot_expressions.If(
                    this=sqlglot_expressions.And(
                        this=sqlglot_expressions.LT(
                            this=index_expr, expression=literal_0
                        ),
                        expression=sqlglot_expressions.LTE(
                            this=sqlglot_expressions.Abs(this=index_expr),
                            expression=total_parts,
                        ),
                    ),
                    true=neg_index_case,
                ),
                sqlglot_expressions.If(
                    this=sqlglot_expressions.EQ(this=index_expr, expression=literal_0),
                    true=zero_index_case,
                ),
            ],
            default=sqlglot_expressions.Null(),
        )
        return result

    def convert_variance(
        self, args: list[SQLGlotExpression], types: list[PyDoughType], type: str
    ) -> SQLGlotExpression:
        """
        Converts a population variance calculation to an equivalent
        SQLGlot expression.

        Args:
            `args`: The arguments to the population variance function.
            `types`: The types of the arguments.
            `type`: The type of variance to calculate.

        Returns:
            The SQLGlot expression to calculate the population variance
            of the argument.
        """
        arg = args[0]
        # Formula: (SUM(X*X) - (SUM(X)*SUM(X) / COUNT(X))) / COUNT(X) for population variance
        # For sample variance, divide by (COUNT(X) - 1) instead of COUNT(X)

        # SUM(X*X)
        square_expr = apply_parens(
            sqlglot_expressions.Pow(
                this=arg, expression=sqlglot_expressions.Literal.number(2)
            )
        )
        sum_squares_expr = sqlglot_expressions.Sum(this=square_expr)

        # SUM(X)
        sum_expr = sqlglot_expressions.Sum(this=arg)

        # COUNT(X)
        count_expr = sqlglot_expressions.Count(this=arg)

        # (SUM(X)*SUM(X))
        sum_squared_expr = sqlglot_expressions.Pow(
            this=sum_expr, expression=sqlglot_expressions.Literal.number(2)
        )

        # ((SUM(X)*SUM(X)) / COUNT(X))
        mean_sum_squared_expr = apply_parens(
            sqlglot_expressions.Div(
                this=apply_parens(sum_squared_expr), expression=apply_parens(count_expr)
            )
        )

        # (SUM(X*X) - (SUM(X)*SUM(X) / COUNT(X)))
        numerator = sqlglot_expressions.Sub(
            this=sum_squares_expr, expression=apply_parens(mean_sum_squared_expr)
        )

        if type == "population":
            # Divide by COUNT(X)
            return apply_parens(
                sqlglot_expressions.Div(
                    this=apply_parens(numerator), expression=apply_parens(count_expr)
                )
            )
        elif type == "sample":
            # Divide by (COUNT(X) - 1)
            denominator = sqlglot_expressions.Sub(
                this=count_expr, expression=sqlglot_expressions.Literal.number(1)
            )
            return apply_parens(
                sqlglot_expressions.Div(
                    this=apply_parens(numerator), expression=apply_parens(denominator)
                )
            )
        else:
            raise ValueError(f"Unsupported type: {type}")

    def convert_std(
        self, args: list[SQLGlotExpression], types: list[PyDoughType], type: str
    ) -> SQLGlotExpression:
        """
        Converts a standard deviation calculation to an equivalent
        SQLGlot expression.

        Args:
            `args`: The arguments to the standard deviation function.
            `types`: The types of the arguments.
            `type`: The type of standard deviation to calculate.

        Returns:
            The SQLGlot expression to calculate the standard deviation
            of the argument.
        """
        variance = self.convert_variance(args, types, type)
        return sqlglot_expressions.Pow(
            this=variance, expression=sqlglot_expressions.Literal.number(0.5)
        )
