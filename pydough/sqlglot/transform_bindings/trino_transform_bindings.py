"""
Definition of SQLGlot transformation bindings for the Trino dialect.
"""

__all__ = ["TrinoTransformBindings"]


import math
from typing import Any

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.configs import DayOfWeek
from pydough.types import (
    BooleanType,
    NumericType,
    PyDoughType,
    StringType,
)

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import DateTimeUnit, apply_parens


class TrinoTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the Trino dialect.
    """

    @property
    def values_alias_column(self) -> bool:
        return False

    PYDOP_TO_TRINO_FUNC: dict[pydop.PyDoughExpressionOperator, str] = {
        pydop.STARTSWITH: "STARTS_WITH",
        pydop.SIGN: "SIGN",
        pydop.SMALLEST: "LEAST",
        pydop.LARGEST: "GREATEST",
    }
    """
    Mapping of PyDough operators to equivalent Trino SQL function names
    These are used to generate anonymous function calls in SQLGlot
    """

    @property
    def dialect_start_of_week(self) -> DayOfWeek:
        """
        Which day of the week is considered the start of the week within the
        SQL dialect. Individual dialects may override this.
        """
        return DayOfWeek.MONDAY

    @property
    def dialect_dow_mapping(self) -> dict[str, int]:
        return {
            "Monday": 1,
            "Tuesday": 2,
            "Wednesday": 3,
            "Thursday": 4,
            "Friday": 5,
            "Saturday": 6,
            "Sunday": 7,
        }

    def convert_call_to_sqlglot(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        if operator in self.PYDOP_TO_TRINO_FUNC:
            return sqlglot_expressions.Anonymous(
                this=self.PYDOP_TO_TRINO_FUNC[operator], expressions=args
            )

        return super().convert_call_to_sqlglot(operator, args, types)

    def convert_sum(
        self, arg: SQLGlotExpression, types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        Converts a SUM function call to its SQLGlot equivalent.
        This method checks the type of the argument to determine whether to use
        COUNT_IF (for BooleanType) or SUM (for other types).
        Arguments:
            `arg` : The argument to the SUM function.
            `types` : The types of the arguments.
        """
        match types[0]:
            # If the argument is of BooleanType, it uses COUNT_IF to count true values.
            case BooleanType():
                return sqlglot_expressions.CountIf(this=arg[0])
            case _:
                # For other types, use SUM directly
                return sqlglot_expressions.Sum(this=arg[0])

    def convert_extract_datetime(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
        unit: DateTimeUnit,
    ) -> SQLGlotExpression:
        # Update argument type to fit datetime
        dt_expr: SQLGlotExpression = self.handle_datetime_base_arg(args[0])
        func_expr: SQLGlotExpression
        match unit:
            case DateTimeUnit.YEAR:
                func_expr = sqlglot_expressions.Year(this=dt_expr)
            case DateTimeUnit.QUARTER:
                func_expr = sqlglot_expressions.Quarter(this=dt_expr)
            case DateTimeUnit.MONTH:
                func_expr = sqlglot_expressions.Month(this=dt_expr)
            case DateTimeUnit.DAY:
                func_expr = sqlglot_expressions.Day(this=dt_expr)
            case DateTimeUnit.HOUR | DateTimeUnit.MINUTE | DateTimeUnit.SECOND:
                # Ensure the argument is a TIMESTAMP (as opposed to a DATE)
                # since Trino's datetime extraction functions require
                #  TIMESTAMP argument for hour/minute/second.
                ts_expr: SQLGlotExpression = sqlglot_expressions.Cast(
                    this=dt_expr, to=sqlglot_expressions.DataType(this="TIMESTAMP")
                )
                func_expr = sqlglot_expressions.Anonymous(
                    this=unit.value.upper(), expressions=[ts_expr]
                )
        return func_expr

    def convert_datediff(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        # Ensure the arguments are truncated before performing the date
        # difference calculation.
        assert len(args) == 3
        # Check if unit is a string.
        if not (isinstance(args[0], sqlglot_expressions.Literal) and args[0].is_string):
            raise ValueError(
                f"Unsupported argument for DATEDIFF: {args[0]!r}. It should be a string literal."
            )

        # Extract the underlying unit
        unit: DateTimeUnit | None = DateTimeUnit.from_string(args[0].this)
        if unit is None:
            raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")

        # Truncate the arguments
        truncated_date1: SQLGlotExpression = self.apply_datetime_truncation(
            args[1], unit
        )
        truncated_date2: SQLGlotExpression = self.apply_datetime_truncation(
            args[2], unit
        )

        # Perform the diff computation on the arguments
        answer = sqlglot_expressions.DateDiff(
            unit=sqlglot_expressions.Var(this=unit.value),
            this=truncated_date2,
            expression=truncated_date1,
        )
        return answer

    def apply_datetime_truncation(
        self, base: SQLGlotExpression, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        if unit is DateTimeUnit.WEEK:
            # 1. Get shifted_weekday (# of days since the start of week)
            # 2. Subtract shifted_weekday DAYS from the datetime
            # 3. Truncate the result to the nearest day
            base = self.make_datetime_arg(base)
            shifted_weekday: SQLGlotExpression = self.days_from_start_of_week(base)
            date_sub: SQLGlotExpression = sqlglot_expressions.DateSub(
                this=base,
                expression=shifted_weekday,
                unit=sqlglot_expressions.Var(this="DAY"),
            )
            return sqlglot_expressions.DateTrunc(
                this=date_sub,
                unit=sqlglot_expressions.Var(this="DAY"),
            )
        else:
            # For other units, use the standard SQLGlot truncation. If the
            # unit is Hour/Minute/Second, first cast to TIMESTAMP.
            if unit in (DateTimeUnit.HOUR, DateTimeUnit.MINUTE, DateTimeUnit.SECOND):
                base = sqlglot_expressions.Cast(
                    this=base, to=sqlglot_expressions.DataType(this="TIMESTAMP")
                )
            return super().apply_datetime_truncation(base, unit)

    def make_datetime_arg(self, arg: SQLGlotExpression) -> SQLGlotExpression:
        # Always ensure coercion occurs
        return self.handle_datetime_base_arg(arg)

    def apply_datetime_offset(
        self, base: SQLGlotExpression, amt: int, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        # Convert week offsets to day offsets, since Trino week offsets are
        # not well defined.
        if unit == DateTimeUnit.WEEK:
            unit = DateTimeUnit.DAY
            amt *= 7

        return super().apply_datetime_offset(base, amt, unit)

    def days_from_start_of_week(self, base: SQLGlotExpression) -> SQLGlotExpression:
        offset: int = (-self.start_of_week_offset) % 7
        dow_expr: SQLGlotExpression = self.dialect_day_of_week(base)
        if offset == 6:
            return dow_expr
        result = sqlglot_expressions.Mod(
            this=apply_parens(
                sqlglot_expressions.Sub(
                    this=apply_parens(dow_expr),
                    expression=sqlglot_expressions.Literal.number(
                        self.start_of_week_offset + 1
                    ),
                )
            ),
            expression=sqlglot_expressions.Literal.number(7),
        )
        return result

    def convert_join_strings(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # Need to manually ensure that all of the types are strings before
        # proceeding, since CONCAT_WS in Trino does not support implicit type
        # conversion.
        new_args: list[SQLGlotExpression] = []
        new_types: list[PyDoughType] = []
        for arg, typ in zip(args, types):
            new_args.append(self.ensure_string(arg, typ))
            new_types.append(StringType())
        return super().convert_join_strings(new_args, new_types)

    def convert_lpad(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # Need to manually ensure that the first argument is a string, since
        # LPAD in Trino does not support implicit type conversion.
        new_arg = self.ensure_string(args[0], types[0])
        return sqlglot_expressions.Anonymous(
            this="LPAD", expressions=[new_arg, args[1], args[2]]
        )

    def convert_rpad(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # Need to manually ensure that the first argument is a string, since
        # RPAD in Trino does not support implicit type conversion.
        new_arg = self.ensure_string(args[0], types[0])
        return sqlglot_expressions.Anonymous(
            this="RPAD", expressions=[new_arg, args[1], args[2]]
        )

    def convert_get_part(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # SPLIT_PART(string, delimiter, index)
        regular_split: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="SPLIT_PART", expressions=args
        )

        # SPLIT_PART(string, delimiter, 1)
        first_split: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="SPLIT_PART",
            expressions=[
                args[0],
                args[1],
                sqlglot_expressions.Literal.number(1),
            ],
        )

        # Count how many times the delimiter appears in the string ("n")
        n_delim: SQLGlotExpression = self.convert_str_count(
            [args[0], args[1]], [StringType(), StringType()]
        )

        # SPLIT_PART(string, delimiter, n + 2 + index), for when the index
        # is negative.
        reverse_split: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="SPLIT_PART",
            expressions=[
                args[0],
                args[1],
                sqlglot_expressions.Add(
                    this=sqlglot_expressions.Add(
                        this=apply_parens(n_delim),
                        expression=apply_parens(args[2]),
                    ),
                    expression=sqlglot_expressions.Literal.number(2),
                ),
            ],
        )

        # CASE WHEN index = 0 then <first_split>
        #      WHEN index < (-n-1) then NULL
        #      WHEN index > (n+1) then NULL
        #      WHEN index < 0 then <reverse_split>
        #      ELSE <regular_split>
        # END
        result: sqlglot_expressions = (
            sqlglot_expressions.Case()
            .when(
                sqlglot_expressions.EQ(
                    this=apply_parens(args[2]),
                    expression=sqlglot_expressions.Literal.number(0),
                ),
                first_split,
            )
            .when(
                sqlglot_expressions.LT(
                    this=apply_parens(args[2]),
                    expression=apply_parens(
                        sqlglot_expressions.Add(
                            this=sqlglot_expressions.Neg(this=apply_parens(n_delim)),
                            expression=sqlglot_expressions.Literal.number(-1),
                        )
                    ),
                ),
                sqlglot_expressions.Null(),
            )
            .when(
                sqlglot_expressions.GT(
                    this=apply_parens(args[2]),
                    expression=apply_parens(
                        sqlglot_expressions.Add(
                            this=apply_parens(n_delim),
                            expression=sqlglot_expressions.Literal.number(1),
                        )
                    ),
                ),
                sqlglot_expressions.Null(),
            )
            .when(
                sqlglot_expressions.LT(
                    this=apply_parens(apply_parens(args[2])),
                    expression=sqlglot_expressions.Literal.number(0),
                ),
                reverse_split,
            )
            .else_(regular_split)
        )

        # CASE
        #   WHEN delimiter = ""
        #   THEN (CASE WHEN ABS(index) < 2 THEN string ELSE NULL END)
        #   ELSE result
        # END
        return (
            sqlglot_expressions.Case()
            .when(
                sqlglot_expressions.EQ(
                    this=apply_parens(args[1]),
                    expression=sqlglot_expressions.Literal.string(""),
                ),
                sqlglot_expressions.Case()
                .when(
                    sqlglot_expressions.LT(
                        this=sqlglot_expressions.Abs(this=apply_parens(args[2])),
                        expression=sqlglot_expressions.Literal.number(2),
                    ),
                    args[0],
                )
                .else_(sqlglot_expressions.Null()),
            )
            .else_(result)
        )

    def convert_replace(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        assert 2 <= len(args) <= 3
        delim_arg: SQLGlotExpression = args[1]
        if (
            isinstance(delim_arg, sqlglot_expressions.Literal)
            and delim_arg.is_string
            and delim_arg.this == ""
        ):
            # Ensure that REPLACE(x, "", z) -> x
            return args[0]
        # Wrap the result in a case statement to ensure the result is the
        # input if the delimiter is an empty string, unless it is also a
        # literal that is not an empty string.
        result: SQLGlotExpression = super().convert_replace(args, types)
        if (
            not isinstance(delim_arg, sqlglot_expressions.Literal)
            and delim_arg.is_string
        ):
            result = (
                sqlglot_expressions.Case()
                .when(
                    sqlglot_expressions.EQ(
                        this=delim_arg,
                        expression=sqlglot_expressions.Literal.string(""),
                    ),
                    args[0],
                )
                .else_(result)
            )
        return result

    def convert_integer(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # First cast to a DOUBLE, then to an INTEGER, for safety.
        return sqlglot_expressions.Cast(
            this=sqlglot_expressions.Cast(
                this=args[0], to=sqlglot_expressions.DataType(this="DOUBLE")
            ),
            to=sqlglot_expressions.DataType(this="BIGINT"),
        )

    def generate_dataframe_item_dialect_expression(
        self, item: Any, item_type: PyDoughType
    ) -> SQLGlotExpression:
        if isinstance(item_type, NumericType) and math.isinf(item):
            if item >= 0:
                return sqlglot_expressions.Cast(
                    this=sqlglot_expressions.Literal.string("Infinity"),
                    to=sqlglot_expressions.DataType.build("DOUBLE"),
                )
            else:
                return sqlglot_expressions.Cast(
                    this=sqlglot_expressions.Literal.string("-Infinity"),
                    to=sqlglot_expressions.DataType.build("DOUBLE"),
                )
        return super().generate_dataframe_item_dialect_expression(item, item_type)
