"""
Definition of SQLGlot transformation bindings for the Databricks dialect.
"""

__all__ = ["DatabricksTransformBindings"]

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import PyDoughType
from pydough.types.boolean_type import BooleanType

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import (
    DateTimeUnit,
)


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
        pydop.GETPART: "SPLIT_PART",
    }
    """
    Mapping of PyDough operators to equivalent Snowflake SQL function names
    These are used to generate anonymous function calls in SQLGlot
    """

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

    def _to_start_of_week(self, d: SQLGlotExpression) -> SQLGlotExpression:
        """
        Returns the start-of-week date for d, respecting configs.start_of_week.

        Databricks DATE_TRUNC('WEEK', d) always returns Monday regardless of
        config. Instead we compute DATE_ADD(DATE(d), -days_from_start_of_week(d))
        where days_from_start_of_week already accounts for both the Databricks
        dialect offset (1-based DAYOFWEEK normalized to 0-based via our
        dialect_day_of_week override) and the configured start-of-week day.
        """
        offset = self.days_from_start_of_week(d)
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
        from .sqlglot_transform_utils import DateTimeUnit as DTU

        if unit == DTU.WEEK:
            return self._to_start_of_week(base)
        return super().apply_datetime_truncation(base, unit)

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

        assert len(args) == 3
        # Check if unit is a string.
        if not (isinstance(args[0], sqlglot_expressions.Literal) and args[0].is_string):
            raise ValueError(
                f"Unsupported argument for DATEDIFF: {args[0]!r}. It should be a string literal."
            )
        date1 = self.make_datetime_arg(args[1])
        date2 = self.make_datetime_arg(args[2])

        unit: DateTimeUnit | None = DateTimeUnit.from_string(args[0].this)
        if unit is None:
            raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")
        if unit == DateTimeUnit.YEAR:
            # Databricks DATEDIFF(YEAR, x, y) counts complete year intervals,
            # use: YEAR(y) - YEAR(x).
            return sqlglot_expressions.Sub(
                this=sqlglot_expressions.Year(this=date2),
                expression=sqlglot_expressions.Year(this=date1),
            )
        if unit == DateTimeUnit.MONTH:
            # Databricks DATEDIFF(MONTH, x, y) counts complete month intervals,
            # use:(YEAR(y)-YEAR(x))*12 + MONTH(y)-MONTH(x).
            year_diff = sqlglot_expressions.Sub(
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
            # Databricks DATEDIFF(DAY, x_ts, y_ts) includes the fractional day
            # from any time component and truncates, giving the wrong answer when
            # the start is not at midnight.
            # use date-only difference: DATEDIFF(DATE(y), DATE(x)).
            return self._day_diff(date1, date2)
        if unit == DateTimeUnit.HOUR:
            # Databricks DATEDIFF(HOUR, x_ts, y_ts) includes fractional hours.
            # use: day_diff * 24 + HOUR(y) - HOUR(x).
            return self._hour_diff(date1, date2)
        if unit == DateTimeUnit.MINUTE:
            # use: hour_diff * 60 + MINUTE(y) - MINUTE(x).
            return self._minute_diff(date1, date2)
        if unit == DateTimeUnit.SECOND:
            # use: minute_diff * 60 + SECOND(y) - SECOND(x).
            return self._second_diff(date1, date2)
        if unit == DateTimeUnit.WEEK:
            # Databricks DATEDIFF(WEEK, x, y) counts complete 7-day intervals
            # (truncated toward zero), make sure to align to the start of the week
            # configuration.
            # boundaries: DATEDIFF(start_of_week(y), start_of_week(x)) / 7.
            # Databricks DAYOFWEEK returns 1=Sunday...7=Saturday, so subtracting
            # 1 gives the 0-based day offset back to the previous Sunday.
            day_diff = sqlglot_expressions.Anonymous(
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
            # Databricks DATEDIFF(QUARTER, x, y) counts complete quarter intervals,
            # use: (YEAR(y)-YEAR(x))*4 + QUARTER(y)-QUARTER(x).
            year_diff = sqlglot_expressions.Sub(
                this=sqlglot_expressions.Year(this=date2),
                expression=sqlglot_expressions.Year(this=date1),
            )
            quarter_y = sqlglot_expressions.Anonymous(
                this="QUARTER", expressions=[date2]
            )
            quarter_x = sqlglot_expressions.Anonymous(
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

    def dialect_day_of_week(self, base: SQLGlotExpression) -> SQLGlotExpression:
        """
        Databricks DAYOFWEEK() returns 1=Sunday...7=Saturday.
        https://docs.databricks.com/aws/en/sql/language-manual/functions/dayofweek
        Subtract 1 to normalize to 0=Sunday...6=Saturday.
        """
        return sqlglot_expressions.Sub(
            this=sqlglot_expressions.DayOfWeek(this=base),
            expression=sqlglot_expressions.Literal.number(1),
        )
