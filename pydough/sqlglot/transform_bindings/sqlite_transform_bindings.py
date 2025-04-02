"""
Definition of SQLGlot transformation bindings for the SQLite dialect.
"""

__all__ = ["SQLiteTransformBindings"]

import sqlite3

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import DateType, PyDoughType, StringType

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import (
    DateTimeUnit,
    apply_parens,
)


class SQLiteTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the SQLite dialect.
    """

    def convert_call_to_sqlglot(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        match operator:
            case pydop.IFF if sqlite3.sqlite_version < "3.32":
                return self.convert_iff_case(args, types)
            case pydop.JOIN_STRINGS if sqlite3.sqlite_version < "3.44.1":
                return self.convert_concat_ws_to_concat(args, types)
        return super().convert_call_to_sqlglot(operator, args, types)

    def convert_concat_ws_to_concat(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        Transforms the arguments of a `CONCAT_WS` function into an equivalent
        `CONCAT`, e.g. `CONCAT_WS(x, A, B, C)` becomes `A || x || B || x || C`.


        Args:
            `args`: The operands to `CONCAT_WS`, after they were
            converted to SQLGlot expressions.
            `types`: The PyDough types of the arguments to `CONCAT_WS`.

        Returns:
            The SQLGlot expression matching the functionality of `CONCAT_WS` as
            a `CONCAT`.
        """
        new_args: list[SQLGlotExpression] = []
        for i in range(1, len(args)):
            if i > 1:
                new_args.append(args[0])
            new_args.append(args[i])
        return sqlglot_expressions.Concat(expressions=new_args)

    def convert_extract_datetime(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
        unit: DateTimeUnit,
    ) -> SQLGlotExpression:
        assert len(args) == 1
        return sqlglot_expressions.Cast(
            this=sqlglot_expressions.TimeToStr(
                this=self.make_datetime_arg(args[0]), format=unit.extraction_string
            ),
            to=sqlglot_expressions.DataType(this=sqlglot_expressions.DataType.Type.INT),
        )

    def convert_datediff(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        assert len(args) == 3
        if not isinstance(args[0], sqlglot_expressions.Literal):
            raise ValueError(
                f"Unsupported argument {args[0]} for DATEDIFF.It should be a string."
            )
        elif not args[0].is_string:
            raise ValueError(
                f"Unsupported argument {args[0]} for DATEDIFF.It should be a string."
            )
        unit: DateTimeUnit | None = DateTimeUnit.from_string(args[0].this)
        args = [
            args[0],
            self.make_datetime_arg(args[1]),
            self.make_datetime_arg(args[2]),
        ]
        match unit:
            case DateTimeUnit.YEAR:
                # Extracts the year from the date and subtracts the years.
                year_x: SQLGlotExpression = self.convert_extract_datetime(
                    [args[1]], [types[1]], DateTimeUnit.YEAR
                )
                year_y: SQLGlotExpression = self.convert_extract_datetime(
                    [args[2]], [types[2]], DateTimeUnit.YEAR
                )
                # equivalent to: expression - this
                years_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=year_y, expression=year_x
                )
                return years_diff
            case DateTimeUnit.MONTH:
                # Extracts the difference in years multiplied by 12.
                # Extracts the month from the date and subtracts the months.
                # Adds the difference in months to the difference in years*12.
                # Implementation wise, this is equivalent to:
                # (years_diff * 12 + month_y) - month_x
                # On expansion: (year_y - year_x) * 12 + month_y - month_x
                years_diff = self.convert_datediff(
                    [sqlglot_expressions.convert("year")] + args[1:],
                    types,
                )
                years_diff_in_months = sqlglot_expressions.Mul(
                    this=apply_parens(years_diff),
                    expression=sqlglot_expressions.Literal.number(12),
                )
                month_x: SQLGlotExpression = self.convert_extract_datetime(
                    [args[1]], [types[1]], DateTimeUnit.MONTH
                )
                month_y: SQLGlotExpression = self.convert_extract_datetime(
                    [args[2]], [types[2]], DateTimeUnit.MONTH
                )
                months_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.Add(
                        this=years_diff_in_months, expression=month_y
                    ),
                    expression=month_x,
                )
                return months_diff
            case DateTimeUnit.WEEK:
                # DATEDIFF('week', A, B)
                #   = DATEDIFF('day', DATETIME(A, 'start of week'),
                #               DATETIME(B, 'start of week')) / 7
                dt1 = self.convert_datetime(
                    [
                        args[1],
                        sqlglot_expressions.convert("start of week"),
                    ],
                    [types[1], types[0]],
                )
                dt2 = self.convert_datetime(
                    [
                        args[2],
                        sqlglot_expressions.convert("start of week"),
                    ],
                    [types[1], types[0]],
                )
                weeks_days_diff: SQLGlotExpression = self.convert_datediff(
                    [sqlglot_expressions.convert("days"), dt1, dt2], types
                )
                return sqlglot_expressions.Cast(
                    this=sqlglot_expressions.Div(
                        this=apply_parens(weeks_days_diff),
                        expression=sqlglot_expressions.Literal.number(7),
                    ),
                    to=sqlglot_expressions.DataType(
                        this=sqlglot_expressions.DataType.Type.INT
                    ),
                )
            case DateTimeUnit.DAY:
                # Extracts the start of date from the datetime and subtracts the dates.
                date_x = sqlglot_expressions.Date(
                    this=[args[1], sqlglot_expressions.convert("start of day")],
                )
                date_y = sqlglot_expressions.Date(
                    this=[args[2], sqlglot_expressions.convert("start of day")],
                )
                # This calculates 'this-expression'.
                answer = sqlglot_expressions.DateDiff(
                    unit=sqlglot_expressions.Var(this="days"),
                    this=date_y,
                    expression=date_x,
                )
                return answer
            case DateTimeUnit.HOUR:
                # Extracts the difference in days multiplied by 24 to get difference in hours.
                # Extracts the hours of x and hours of y.
                # Adds the difference in hours to the (difference in days*24).
                # Implementation wise, this is equivalent to:
                # (days_diff*24 + hours_y) - hours_x
                # On expansion: (( day_y - day_x ) * 24 + hours_y) - hours_x
                days_diff: SQLGlotExpression = self.convert_datediff(
                    [sqlglot_expressions.convert("days")] + args[1:],
                    types,
                )
                days_diff_in_hours = sqlglot_expressions.Mul(
                    this=apply_parens(days_diff),
                    expression=sqlglot_expressions.Literal.number(24),
                )
                hours_x: SQLGlotExpression = self.convert_extract_datetime(
                    [args[1]], [types[1]], DateTimeUnit.HOUR
                )
                hours_y: SQLGlotExpression = self.convert_extract_datetime(
                    [args[2]], [types[2]], DateTimeUnit.HOUR
                )
                hours_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.Add(
                        this=days_diff_in_hours, expression=hours_y
                    ),
                    expression=hours_x,
                )
                return hours_diff
            case DateTimeUnit.MINUTE:
                # Extracts the difference in hours multiplied by 60 to get difference in minutes.
                # Extracts the minutes of x and minutes of y.
                # Adds the difference in minutes to the (difference in hours*60).
                # Implementation wise, this is equivalent to:
                # (hours_diff*60 + minutes_y) - minutes_x
                # On expansion: (( hours_y - hours_x )*60 + minutes_y) - minutes_x
                hours_diff = self.convert_datediff(
                    [sqlglot_expressions.convert("hours")] + args[1:],
                    types,
                )
                hours_diff_in_mins = sqlglot_expressions.Mul(
                    this=apply_parens(hours_diff),
                    expression=sqlglot_expressions.Literal.number(60),
                )
                min_x: SQLGlotExpression = self.convert_extract_datetime(
                    [args[1]], [types[1]], DateTimeUnit.MINUTE
                )
                min_y: SQLGlotExpression = self.convert_extract_datetime(
                    [args[2]], [types[2]], DateTimeUnit.MINUTE
                )
                mins_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.Add(
                        this=hours_diff_in_mins, expression=min_y
                    ),
                    expression=min_x,
                )
                return mins_diff
            case DateTimeUnit.SECOND:
                # Extracts the difference in minutes multiplied by 60 to get difference in seconds.
                # Extracts the seconds of x and seconds of y.
                # Adds the difference in seconds to the (difference in minutes*60).
                # Implementation wise, this is equivalent to:
                # (mins_diff*60 + seconds_y) - seconds_x
                # On expansion: (( mins_y - mins_x )*60 + seconds_y) - seconds_x
                mins_diff = self.convert_datediff(
                    [sqlglot_expressions.convert("minutes")] + args[1:],
                    types,
                )
                minutes_diff_in_secs = sqlglot_expressions.Mul(
                    this=apply_parens(mins_diff),
                    expression=sqlglot_expressions.Literal.number(60),
                )
                sec_x: SQLGlotExpression = self.convert_extract_datetime(
                    [args[1]], [types[1]], DateTimeUnit.SECOND
                )
                sec_y: SQLGlotExpression = self.convert_extract_datetime(
                    [args[2]], [types[2]], DateTimeUnit.SECOND
                )
                secs_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.Add(
                        this=minutes_diff_in_secs, expression=sec_y
                    ),
                    expression=sec_x,
                )
                return secs_diff
            case _:
                raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")

    def dialect_day_of_week(self, base: SQLGlotExpression) -> SQLGlotExpression:
        """
        Gets the day of the week, as an integer, for the `base` argument in
        terms of its dialect.

        Args:
            `base`: The base date/time expression to calculate the day of week
            from.

        Returns:
            The SQLGlot expression to calculating the day of week of `base` in
            terms of the dialect's start of week.
        """
        return self.convert_extract_datetime([base], [DateType()], DateTimeUnit.WEEK)

    def apply_datetime_truncation(
        self, base: SQLGlotExpression, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        """
        Applies a truncation operation to a date/time expression by a certain unit.

        Args:
            `base`: The base date/time expression to truncate.
            `unit`: The unit to truncate the date/time expression to.

        Returns:
            The SQLGlot expression to truncate `base`.
        """
        base = self.make_datetime_arg(base)
        match unit:
            # For y/m/d, use the `start of` modifier in SQLite.
            case DateTimeUnit.YEAR | DateTimeUnit.MONTH | DateTimeUnit.DAY:
                trunc_expr: SQLGlotExpression = sqlglot_expressions.convert(
                    f"start of {unit.value}"
                )
                if isinstance(base, sqlglot_expressions.Date):
                    base.this.append(trunc_expr)
                    return base
                if (
                    isinstance(base, sqlglot_expressions.Datetime)
                    and len(base.this) == 1
                ):
                    return sqlglot_expressions.Date(
                        this=base.this + [trunc_expr],
                    )
                return sqlglot_expressions.Date(
                    this=[base, trunc_expr],
                )
            case DateTimeUnit.WEEK:
                # Implementation for week.
                # Assumption: By default start of week is Sunday
                # Week starts at 0
                # Sunday = 0, Monday = 1, ..., Saturday = 6
                shifted_weekday: SQLGlotExpression = self.days_from_start_of_week(base)
                # This expression is equivalent to  "- ((STRFTIME('%w', base) + offset) % 7) days"
                offset_expr: SQLGlotExpression = self.convert_concat(
                    [
                        sqlglot_expressions.convert("-"),
                        sqlglot_expressions.Cast(
                            this=shifted_weekday,
                            to=sqlglot_expressions.DataType.build("TEXT"),
                        ),
                        sqlglot_expressions.convert(" days"),
                    ],
                    [StringType()] * 3,
                )
                start_of_day_expr: SQLGlotExpression = (
                    sqlglot_expressions.Literal.string("start of day")
                )
                # Apply the "- <offset> days", then truncate to the start of the day
                if isinstance(base, sqlglot_expressions.Date):
                    base.this.extend(
                        [offset_expr, sqlglot_expressions.convert("start of day")]
                    )
                    return base
                if (
                    isinstance(base, sqlglot_expressions.Datetime)
                    and len(base.this) == 1
                ):
                    return sqlglot_expressions.Date(
                        this=base.this + [offset_expr, start_of_day_expr],
                    )
                return sqlglot_expressions.Date(
                    this=[base, offset_expr, start_of_day_expr],
                )
            # SQLite does not have `start of` modifiers for hours, minutes, or
            # seconds, so we use `strftime` to truncate to the unit.
            case DateTimeUnit.HOUR | DateTimeUnit.MINUTE | DateTimeUnit.SECOND:
                return sqlglot_expressions.TimeToStr(
                    this=base,
                    format=unit.truncation_string,
                )

    def apply_datetime_offset(
        self, base: SQLGlotExpression, amt: int, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        # Convert "+n weeks" to "+7n days"
        if unit == DateTimeUnit.WEEK:
            amt, unit = amt * 7, DateTimeUnit.DAY
        # For sqlite, use the DATETIME operator to add the interval
        offset_expr: SQLGlotExpression = sqlglot_expressions.convert(
            f"{amt} {unit.value}"
        )
        if isinstance(base, sqlglot_expressions.Datetime) or (
            isinstance(base, sqlglot_expressions.Date)
            and unit in (DateTimeUnit.YEAR, DateTimeUnit.MONTH, DateTimeUnit.DAY)
        ):
            base.this.append(offset_expr)
            return base
        return sqlglot_expressions.Datetime(
            this=[base, sqlglot_expressions.convert(f"{amt} {unit.value}")],
        )

    def convert_current_timestamp(self) -> SQLGlotExpression:
        return sqlglot_expressions.Datetime(this=[sqlglot_expressions.convert("now")])

    def coerce_to_timestamp(self, base: SQLGlotExpression) -> SQLGlotExpression:
        return sqlglot_expressions.Datetime(this=[base])
