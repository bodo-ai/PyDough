"""
Definition of SQLGlot transformation bindings for the DuckDB dialect.
"""

__all__ = ["DuckDBTransformBindings"]

import math
from typing import Any

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import NumericType, PyDoughType

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import DateTimeUnit, apply_parens


class DuckDBTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the DuckDB dialect.
    """

    @property
    def values_alias_column(self) -> bool:
        return False

    # PYDOP_TO_DUCKDB_FUNC: dict[pydop.PyDoughExpressionOperator, str] = {
    #     pydop.STARTSWITH: "STARTSWITH",
    #     pydop.ENDSWITH: "ENDSWITH",
    #     pydop.CONTAINS: "CONTAINS",
    #     pydop.LPAD: "LPAD",
    #     pydop.RPAD: "RPAD",
    #     pydop.SIGN: "SIGN",
    #     pydop.SMALLEST: "LEAST",
    #     pydop.LARGEST: "GREATEST",
    # }
    # """
    # Mapping of PyDough operators to equivalent DuckDB SQL function names
    # These are used to generate anonymous function calls in SQLGlot
    # """

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
        return super().convert_call_to_sqlglot(operator, args, types)

    def convert_integer(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # Cast to DOUBLE first to handle string literals like '4.3' that can't
        # be cast directly to BIGINT. Then apply TRUNC before the final BIGINT
        # cast because DuckDB rounds on CAST(DOUBLE→BIGINT) rather than
        # truncating (e.g. CAST(-5.7 AS BIGINT) → -6, not -5).
        return sqlglot_expressions.Cast(
            this=sqlglot_expressions.Anonymous(
                this="TRUNC",
                expressions=[
                    sqlglot_expressions.Cast(
                        this=args[0],
                        to=sqlglot_expressions.DataType.build("DOUBLE"),
                    )
                ],
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

    def convert_current_timestamp(self) -> SQLGlotExpression:
        """
        Create a SQLGlot expression to obtain the current timestamp removing the
        timezone and not DST-aware specifically for Snowflake.
        SQL:
            CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
        """
        return sqlglot_expressions.Cast(
            this=sqlglot_expressions.ConvertTimezone(
                target_tz=sqlglot_expressions.Literal.string("UTC"),
                timestamp=sqlglot_expressions.CurrentTimestamp(),
            ),
            to=sqlglot_expressions.DataType.build("TIMESTAMPNTZ"),
        )

    def _to_start_of_week(self, d: SQLGlotExpression) -> SQLGlotExpression:
        """
        Returns the start-of-week date for d, respecting configs.start_of_week.

        DuckDB DATE_TRUNC('week', d) always returns Monday (ISO). Instead
        compute CAST(d AS DATE) - days_from_start_of_week(d).

        DuckDB dayofweek() is 0-based (Sun=0...Sat=6), matching the base class
        default, so days_from_start_of_week needs no additional offset
        adjustment beyond what the base implementation already provides.
        """
        offset: SQLGlotExpression = self.days_from_start_of_week(d)
        return sqlglot_expressions.Sub(
            this=sqlglot_expressions.Cast(
                this=d, to=sqlglot_expressions.DataType.build("DATE")
            ),
            expression=sqlglot_expressions.Cast(
                this=apply_parens(offset),
                to=sqlglot_expressions.DataType.build("INTEGER"),
            ),
        )

    def apply_datetime_truncation(
        self, base: SQLGlotExpression, unit: "DateTimeUnit"
    ) -> SQLGlotExpression:
        if unit == DateTimeUnit.WEEK:
            return self._to_start_of_week(base)
        return super().apply_datetime_truncation(base, unit)

    def apply_datetime_offset(
        self, base: SQLGlotExpression, amt: int, unit: "DateTimeUnit"
    ) -> SQLGlotExpression:
        # DuckDB renders DateAdd with QUARTER as N*90 DAY intervals instead of
        # the correct N*3 MONTH approach. Convert QUARTER to months explicitly.
        if unit == DateTimeUnit.QUARTER:
            return super().apply_datetime_offset(base, amt * 3, DateTimeUnit.MONTH)
        return super().apply_datetime_offset(base, amt, unit)

    def convert_datediff(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        """
        DuckDB's datediff('week', x, y) counts ISO week boundaries (Monday
        crossings), which is wrong when start_of_week != Monday. Only WEEK
        is overridden; all other units work correctly via DuckDB's native
        datediff, handled by the base class.
        """
        assert len(args) == 3
        unit: DateTimeUnit | None = DateTimeUnit.from_string(args[0].this)
        if unit != DateTimeUnit.WEEK:
            return super().convert_datediff(args, types)

        date1: SQLGlotExpression = self.make_datetime_arg(args[1])
        date2: SQLGlotExpression = self.make_datetime_arg(args[2])
        day_diff: SQLGlotExpression = sqlglot_expressions.DateDiff(
            unit=sqlglot_expressions.Var(this="day"),
            this=self._to_start_of_week(date2),
            expression=self._to_start_of_week(date1),
        )
        return sqlglot_expressions.Cast(
            this=sqlglot_expressions.Div(
                this=sqlglot_expressions.Paren(this=day_diff),
                expression=sqlglot_expressions.Literal.number(7),
            ),
            to=sqlglot_expressions.DataType.build("BIGINT"),
        )
