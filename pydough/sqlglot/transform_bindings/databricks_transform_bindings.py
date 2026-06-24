"""
Definition of SQLGlot transformation bindings for the Databricks dialect.
"""

__all__ = ["DatabricksTransformBindings"]

import math
from typing import Any

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.configs import DayOfWeek
from pydough.types import NumericType, PyDoughType
from pydough.types.boolean_type import BooleanType

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import DateTimeUnit, apply_parens


class DatabricksTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the Databricks dialect.
    """

    @property
    def values_alias_column(self) -> bool:
        return False

    PYDOP_TO_DATABRICKS_FUNC: dict[pydop.PyDoughExpressionOperator, str] = {
        pydop.STARTSWITH: "STARTSWITH",
        pydop.ENDSWITH: "ENDSWITH",
        pydop.CONTAINS: "CONTAINS",
        pydop.LPAD: "LPAD",
        pydop.RPAD: "RPAD",
        pydop.SIGN: "SIGN",
        pydop.SMALLEST: "LEAST",
        pydop.LARGEST: "GREATEST",
    }
    """
    Mapping of PyDough operators to equivalent Snowflake SQL function names
    These are used to generate anonymous function calls in SQLGlot
    """

    def convert_get_part(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # Unlike PyDough (and Snowflake's SPLIT_PART), Databricks'
        # SPLIT_PART raises INVALID_INDEX_OF_ZERO when the index is 0
        # instead of treating it as 1, so remap a 0 index to 1.
        assert len(args) == 3
        index_arg: SQLGlotExpression = args[2]
        index_expr: SQLGlotExpression = sqlglot_expressions.Case(
            ifs=[
                sqlglot_expressions.If(
                    this=sqlglot_expressions.EQ(
                        this=index_arg,
                        expression=sqlglot_expressions.Literal.number(0),
                    ),
                    true=sqlglot_expressions.Literal.number(1),
                )
            ],
            default=index_arg,
        )
        return sqlglot_expressions.Anonymous(
            this="SPLIT_PART", expressions=[args[0], args[1], index_expr]
        )

    def convert_call_to_sqlglot(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        if operator in self.PYDOP_TO_DATABRICKS_FUNC:
            return sqlglot_expressions.Anonymous(
                this=self.PYDOP_TO_DATABRICKS_FUNC[operator], expressions=args
            )

        return super().convert_call_to_sqlglot(operator, args, types)

    def convert_sum(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        Converts a SUM function call to its SQLGlot equivalent.
        This method checks the type of the argument to determine whether to use
        COUNT_IF (for BooleanType) or SUM (for other types).
        Arguments:
            `args` : The arguments to the SUM function.
            `types` : The types of the arguments.
        """
        match types[0]:
            # If the argument is of BooleanType, it uses COUNT_IF to count true values.
            case BooleanType():
                return sqlglot_expressions.CountIf(this=args[0])
            case _:
                # For other types, use SUM directly
                return sqlglot_expressions.Sum(this=args[0])

    def convert_integer(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # Databricks raises CAST_INVALID_INPUT when casting a string literal
        # containing a decimal value (e.g. '4.3') directly to BIGINT, even
        # though casting the equivalent numeric literal (4.3) works fine.
        # Cast to DOUBLE first, then to BIGINT, to handle both cases.
        return sqlglot_expressions.Cast(
            this=sqlglot_expressions.Cast(
                this=args[0], to=sqlglot_expressions.DataType.build("DOUBLE")
            ),
            to=sqlglot_expressions.DataType.build("BIGINT"),
        )

    def generate_dataframe_item_dialect_expression(
        self, item: Any, item_type: PyDoughType
    ) -> SQLGlotExpression:
        # Same as the base case, except ±Infinity is generated as a CAST to
        # DOUBLE rather than a bare string literal. Databricks' VALUES clause
        # requires every row to have the same type for a given column, so a
        # plain 'Infinity' string literal alongside numeric literals (e.g.
        # 1.5) raises INCOMPATIBLE_TYPES_IN_INLINE_TABLE.
        if isinstance(item_type, NumericType) and math.isinf(item):
            sign = "" if item >= 0 else "-"
            return sqlglot_expressions.Cast(
                this=sqlglot_expressions.Literal.string(f"{sign}Infinity"),
                to=sqlglot_expressions.DataType.build("DOUBLE"),
            )
        return super().generate_dataframe_item_dialect_expression(item, item_type)

    def _to_start_of_week(self, d: SQLGlotExpression) -> SQLGlotExpression:
        """
        Returns the start-of-week date for d, respecting configs.start_of_week.

        Databricks DATE_TRUNC('WEEK', d) always returns Monday regardless of
        config. Instead we compute DATE_ADD(DATE(d), -days_from_start_of_week(d))
        where days_from_start_of_week already accounts for both the Databricks
        dialect offset (1-based DAYOFWEEK normalized to 0-based via our
        dialect_day_of_week override) and the configured start-of-week day.
        """
        offset: SQLGlotExpression = self.days_from_start_of_week(d)
        return sqlglot_expressions.Anonymous(
            this="DATE_ADD",
            expressions=[
                sqlglot_expressions.Anonymous(this="DATE", expressions=[d]),
                sqlglot_expressions.Neg(this=sqlglot_expressions.Paren(this=offset)),
            ],
        )

    def apply_datetime_truncation(
        self, base: SQLGlotExpression, unit: "DateTimeUnit"
    ) -> SQLGlotExpression:
        if unit == DateTimeUnit.WEEK:
            return self._to_start_of_week(base)
        if unit == DateTimeUnit.DAY:
            # sqlglot renders DateTrunc(unit='day') as TRUNC(expr, 'DAY')
            # for Databricks, but Databricks' TRUNC only supports
            # YEAR/MONTH/QUARTER formats (not DAY), giving incorrect
            # results. Use TimestampTrunc, which renders to
            # DATE_TRUNC('DAY', expr) and works correctly.
            return sqlglot_expressions.TimestampTrunc(
                this=self.make_datetime_arg(base),
                unit=sqlglot_expressions.Var(this=unit.value),
            )
        return super().apply_datetime_truncation(base, unit)

    def apply_datetime_offset(
        self, base: SQLGlotExpression, amt: int, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        # Databricks `DATE_ADD`/`DATE_SUB` only operate on day granularity,
        # silently dropping the unit for sub-day units (HOUR/MINUTE/SECOND)
        # and treating the amount as a number of days. Use interval
        # arithmetic instead, which Databricks supports directly.
        if unit in (DateTimeUnit.HOUR, DateTimeUnit.MINUTE, DateTimeUnit.SECOND):
            if amt == 0:
                return base
            interval: SQLGlotExpression = sqlglot_expressions.Interval(
                this=sqlglot_expressions.convert(abs(amt)),
                unit=sqlglot_expressions.Var(this=unit.value.upper()),
            )
            if amt > 0:
                return sqlglot_expressions.Add(this=base, expression=interval)
            else:
                return sqlglot_expressions.Sub(this=base, expression=interval)
        return super().apply_datetime_offset(base, amt, unit)

    def _day_diff(
        self, x: SQLGlotExpression, y: SQLGlotExpression
    ) -> SQLGlotExpression:
        """DATEDIFF(DATE(y), DATE(x)) — strips time before computing day diff."""
        return sqlglot_expressions.Anonymous(
            this="DATEDIFF",
            expressions=[
                sqlglot_expressions.Anonymous(this="DATE", expressions=[y]),
                sqlglot_expressions.Anonymous(this="DATE", expressions=[x]),
            ],
        )

    def _extract(self, unit: str, expr: SQLGlotExpression) -> SQLGlotExpression:
        """EXTRACT(unit FROM expr)."""
        return sqlglot_expressions.Extract(
            this=sqlglot_expressions.Var(this=unit),
            expression=expr,
        )

    def _hour_diff(
        self, x: SQLGlotExpression, y: SQLGlotExpression
    ) -> SQLGlotExpression:
        """day_diff * 24 + HOUR(y) - HOUR(x)."""
        return sqlglot_expressions.Add(
            this=sqlglot_expressions.Mul(
                this=sqlglot_expressions.Paren(this=self._day_diff(x, y)),
                expression=sqlglot_expressions.Literal.number(24),
            ),
            expression=sqlglot_expressions.Sub(
                this=self._extract("HOUR", y),
                expression=self._extract("HOUR", x),
            ),
        )

    def _minute_diff(
        self, x: SQLGlotExpression, y: SQLGlotExpression
    ) -> SQLGlotExpression:
        """hour_diff * 60 + MINUTE(y) - MINUTE(x)."""
        return sqlglot_expressions.Add(
            this=sqlglot_expressions.Mul(
                this=sqlglot_expressions.Paren(this=self._hour_diff(x, y)),
                expression=sqlglot_expressions.Literal.number(60),
            ),
            expression=sqlglot_expressions.Sub(
                this=self._extract("MINUTE", y),
                expression=self._extract("MINUTE", x),
            ),
        )

    def _second_diff(
        self, x: SQLGlotExpression, y: SQLGlotExpression
    ) -> SQLGlotExpression:
        """minute_diff * 60 + SECOND(y) - SECOND(x)."""
        return sqlglot_expressions.Add(
            this=sqlglot_expressions.Mul(
                this=sqlglot_expressions.Paren(this=self._minute_diff(x, y)),
                expression=sqlglot_expressions.Literal.number(60),
            ),
            expression=sqlglot_expressions.Sub(
                this=self._extract("SECOND", y),
                expression=self._extract("SECOND", x),
            ),
        )

    def convert_datediff(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        """
        Databricks' built-in DATEDIFF(unit, x, y) produces incorrect results
        for most units, so each unit is handled explicitly:

        - YEAR:    YEAR(y) - YEAR(x)
        - MONTH:   (YEAR(y)-YEAR(x))*12 + MONTH(y)-MONTH(x)
        - WEEK:    DATEDIFF(start_of_week(y), start_of_week(x)) / 7, cast to
                   BIGINT, to align with the configured start-of-week day.
        - DAY:     DATEDIFF(DATE(y), DATE(x)) — strips the time component
                   first, since Databricks truncates fractional days.
        - HOUR:    day_diff * 24 + HOUR(y) - HOUR(x)
        - MINUTE:  hour_diff * 60 + MINUTE(y) - MINUTE(x)
        - SECOND:  minute_diff * 60 + SECOND(y) - SECOND(x)
        - QUARTER: (YEAR(y)-YEAR(x))*4 + QUARTER(y)-QUARTER(x)
        """
        assert len(args) == 3
        # Check if unit is a string.
        if not (isinstance(args[0], sqlglot_expressions.Literal) and args[0].is_string):
            raise ValueError(
                f"Unsupported argument for DATEDIFF: {args[0]!r}. It should be a string literal."
            )
        date1: SQLGlotExpression = self.make_datetime_arg(args[1])
        date2: SQLGlotExpression = self.make_datetime_arg(args[2])

        unit: DateTimeUnit | None = DateTimeUnit.from_string(args[0].this)
        if unit is None:
            raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")
        if unit == DateTimeUnit.YEAR:
            return sqlglot_expressions.Sub(
                this=sqlglot_expressions.Year(this=date2),
                expression=sqlglot_expressions.Year(this=date1),
            )
        if unit == DateTimeUnit.MONTH:
            year_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                this=sqlglot_expressions.Year(this=date2),
                expression=sqlglot_expressions.Year(this=date1),
            )
            return sqlglot_expressions.Add(
                this=sqlglot_expressions.Mul(
                    this=sqlglot_expressions.Paren(this=year_diff),
                    expression=sqlglot_expressions.Literal.number(12),
                ),
                expression=sqlglot_expressions.Sub(
                    this=sqlglot_expressions.Month(this=date2),
                    expression=sqlglot_expressions.Month(this=date1),
                ),
            )
        if unit == DateTimeUnit.DAY:
            return self._day_diff(date1, date2)
        if unit == DateTimeUnit.HOUR:
            return self._hour_diff(date1, date2)
        if unit == DateTimeUnit.MINUTE:
            return self._minute_diff(date1, date2)
        if unit == DateTimeUnit.SECOND:
            return self._second_diff(date1, date2)
        if unit == DateTimeUnit.WEEK:
            day_diff: SQLGlotExpression = sqlglot_expressions.Anonymous(
                this="DATEDIFF",
                expressions=[
                    self._to_start_of_week(date2),
                    self._to_start_of_week(date1),
                ],
            )
            return sqlglot_expressions.Cast(
                this=sqlglot_expressions.Div(
                    this=sqlglot_expressions.Paren(this=day_diff),
                    expression=sqlglot_expressions.Literal.number(7),
                ),
                to=sqlglot_expressions.DataType.build("BIGINT"),
            )
        if unit == DateTimeUnit.QUARTER:
            year_diff = sqlglot_expressions.Sub(
                this=sqlglot_expressions.Year(this=date2),
                expression=sqlglot_expressions.Year(this=date1),
            )
            quarter_y: SQLGlotExpression = sqlglot_expressions.Anonymous(
                this="QUARTER", expressions=[date2]
            )
            quarter_x: SQLGlotExpression = sqlglot_expressions.Anonymous(
                this="QUARTER", expressions=[date1]
            )
            return sqlglot_expressions.Add(
                this=sqlglot_expressions.Mul(
                    this=sqlglot_expressions.Paren(this=year_diff),
                    expression=sqlglot_expressions.Literal.number(4),
                ),
                expression=sqlglot_expressions.Sub(
                    this=quarter_y, expression=quarter_x
                ),
            )
        return super().convert_datediff(args, types)

    @property
    def dialect_start_of_week(self) -> DayOfWeek:
        # Databricks DAYOFWEEK() is 1-based starting from Sunday.
        return DayOfWeek.SUNDAY

    @property
    def dialect_dow_mapping(self) -> dict[str, int]:
        # Databricks DAYOFWEEK() returns 1=Sunday...7=Saturday.
        return {
            "Sunday": 1,
            "Monday": 2,
            "Tuesday": 3,
            "Wednesday": 4,
            "Thursday": 5,
            "Friday": 6,
            "Saturday": 7,
        }

    def dialect_day_of_week(self, base: SQLGlotExpression) -> SQLGlotExpression:
        """
        Databricks DAYOFWEEK() returns 1=Sunday...7=Saturday.
        https://docs.databricks.com/aws/en/sql/language-manual/functions/dayofweek
        """
        return sqlglot_expressions.DayOfWeek(this=base)

    def days_from_start_of_week(self, base: SQLGlotExpression) -> SQLGlotExpression:
        # Databricks DAYOFWEEK is 1-based, so subtract 1 before applying the
        # configured start-of-week offset (same pattern as Trino).
        offset: int = (-self.start_of_week_offset) % 7
        dow_expr: SQLGlotExpression = self.dialect_day_of_week(base)
        return sqlglot_expressions.Mod(
            this=apply_parens(
                sqlglot_expressions.Add(
                    this=apply_parens(dow_expr),
                    expression=sqlglot_expressions.Literal.number(offset - 1),
                )
            ),
            expression=sqlglot_expressions.Literal.number(7),
        )

    def convert_monthname(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        Creates a SQLGlot expression for `MONTHNAME(X)` as following:

        monthname(date)

        Args:
            `args`: The operands to `MONTHNAME`, after they were
            converted to SQLGlot expressions.
            `types`: The PyDough types of the arguments to `MONTHNAME`.

        Returns:
            The SQLGlot expression matching the functionality of `MONTHNAME`.
        """
        assert len(args) == 1
        date: SQLGlotExpression = self.make_datetime_arg(args[0])
        return sqlglot_expressions.Anonymous(this="monthname", expressions=[date])
