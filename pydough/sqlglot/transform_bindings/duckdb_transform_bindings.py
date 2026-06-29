"""
Definition of SQLGlot transformation bindings for the DuckDB dialect.
"""

__all__ = ["DuckDBTransformBindings"]

import math
from typing import Any

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

from pydough.types import NumericType, PyDoughType, StringType

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import DateTimeUnit, apply_parens


class DuckDBTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the DuckDB dialect.
    """

    @property
    def values_alias_column(self) -> bool:
        return False

    def convert_get_part(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # DuckDB's SPLIT_PART with an empty delimiter splits character by
        # character, but PyDough semantics treat an empty delimiter as "no
        # split": exactly one part (the whole string). k=0/1/-1 all resolve
        # to that part; all other k values return NULL.
        # The base-class recursive CTE cannot be used because DuckDB hoists
        # correlated CTEs to the top level, losing the outer column reference.
        assert len(args) == 3
        if (
            isinstance(args[1], sqlglot_expressions.Literal)
            and args[1].is_string
            and args[1].this == ""
        ):
            # Remap k=0 to 1; then ABS(k)=1 means it's within the single part.
            remapped: SQLGlotExpression = self._remap_zero_to_one(args[2])
            return sqlglot_expressions.Case(
                ifs=[
                    sqlglot_expressions.If(
                        this=sqlglot_expressions.EQ(
                            this=sqlglot_expressions.Anonymous(
                                this="ABS", expressions=[remapped]
                            ),
                            expression=sqlglot_expressions.Literal.number(1),
                        ),
                        true=args[0],
                    )
                ],
                default=sqlglot_expressions.Null(),
            )
        return sqlglot_expressions.Anonymous(
            this="SPLIT_PART",
            expressions=[args[0], args[1], self._remap_zero_to_one(args[2])],
        )

    def convert_integer(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # Cast to DOUBLE first to handle string literals like '4.3' that can't
        # be cast directly to BIGINT. Then apply TRUNC before the final BIGINT
        # cast because DuckDB rounds on CAST(DOUBLE->BIGINT) rather than
        # truncating (e.g. CAST(-5.7 AS BIGINT) -> -6, not -5).
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

    def convert_lpad(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # DuckDB's LENGTH() only accepts VARCHAR; EXTRACT/MONTH returns BIGINT.
        # Cast the first argument to VARCHAR before the base implementation
        # computes LENGTH(args[0]).
        return super().convert_lpad(
            [self.ensure_string(args[0], types[0]), *args[1:]],
            [StringType(), *types[1:]],
        )

    def convert_rpad(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # Same as convert_lpad: DuckDB requires VARCHAR for LENGTH().
        return super().convert_rpad(
            [self.ensure_string(args[0], types[0]), *args[1:]],
            [StringType(), *types[1:]],
        )

    def generate_dataframe_item_dialect_expression(
        self, item: Any, item_type: PyDoughType
    ) -> SQLGlotExpression:
        # Same as the base case, except ±Infinity is generated as a CAST to
        # DOUBLE rather than a bare string literal. DuckDB's VALUES clause
        # requires every row to have the same type for a given column, so a
        # plain 'Infinity' string literal alongside numeric literals (e.g.
        # 1.5) raises a type-mismatch error.
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
        timezone information to match UTC behavior.
        SQL:
            CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)
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
