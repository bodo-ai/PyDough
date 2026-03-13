"""
Definition of SQLGlot transformation bindings for the Trino dialect.
"""

__all__ = ["TrinoTransformBindings"]


import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.configs import DayOfWeek
from pydough.types import PyDoughType

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
        pydop.LPAD: "LPAD",
        pydop.RPAD: "RPAD",
        pydop.SIGN: "SIGN",
        pydop.SMALLEST: "LEAST",
        pydop.LARGEST: "GREATEST",
        pydop.GETPART: "SPLIT_PART",
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
                func_expr = sqlglot_expressions.Anonymous(
                    this=unit.value.upper(), expressions=[dt_expr]
                )
        return func_expr

    def apply_datetime_truncation(
        self, base: SQLGlotExpression, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        if unit is DateTimeUnit.WEEK:
            # 1. Get shifted_weekday (# of days since the start of week)
            # 2. Subtract shifted_weekday DAYS from the datetime
            # 3. Truncate the result to the nearest day
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
            # For other units, use the standard SQLGlot truncation
            return super().apply_datetime_truncation(base, unit)

    def days_from_start_of_week(self, base: SQLGlotExpression) -> SQLGlotExpression:
        offset: int = (-self.start_of_week_offset) % 7
        dow_expr: SQLGlotExpression = self.dialect_day_of_week(base)
        if offset == 1:
            return dow_expr
        breakpoint()
        return sqlglot_expressions.Mod(
            this=apply_parens(
                sqlglot_expressions.Add(
                    this=dow_expr,
                    expression=sqlglot_expressions.Literal.number(offset - 1),
                )
            ),
            expression=sqlglot_expressions.Literal.number(7),
        )
