"""
Definition of SQLGlot transformation bindings for the Postgres dialect.
"""

__all__ = ["PostgresTransformBindings"]

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import PyDoughType
from pydough.types.boolean_type import BooleanType

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import (
    DateTimeUnit,
)


class PostgresTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the Postgres dialect.
    """

    PYDOP_TO_POSTGRES_FUNC: dict[pydop.PyDoughExpressionOperator, str] = {
        pydop.ABS: "ABS",
        pydop.AVG: "AVG",
        pydop.CEIL: "CEIL",
        #        pydop.COUNT: "COUNT",
        pydop.FLOOR: "FLOOR",
        # pydop.GETPART: "SPLIT_PART",
        pydop.LENGTH: "LENGTH",
        pydop.LOWER: "LOWER",
        pydop.MAX: "MAX",
        pydop.MIN: "MIN",
        pydop.MOD: "MOD",
        pydop.POWER: "POWER",
        #        pydop.ROUND: "ROUND",
        #       pydop.SQRT: "SQRT",
        pydop.UPPER: "UPPER",
        pydop.LPAD: "LPAD",
        pydop.RPAD: "RPAD",
        #        pydop.SIGN: "SIGN",
        pydop.SMALLEST: "LEAST",
        pydop.LARGEST: "GREATEST",
    }

    """
    Mapping of PyDough operators to equivalent Postgres function names
    These are used to generate anonymous function calls in SQLGlot
    """

    def convert_call_to_sqlglot(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        match operator:
            case pydop.SUM:
                return self.convert_sum(args, types)
        #     case pydop.GETPART:
        #         return self.convert_getpart(args, types)
        if operator in self.PYDOP_TO_POSTGRES_FUNC:
            return sqlglot_expressions.Anonymous(
                this=self.PYDOP_TO_POSTGRES_FUNC[operator], expressions=args
            )

        return super().convert_call_to_sqlglot(operator, args, types)

    def convert_sum(
        self, arg: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        Converts a SUM function call to its SQLGlot equivalent.
        This method checks the type of the argument to determine whether to use
        COUNT_IF (for BooleanType) or SUM (for other types).
        Arguments:
            arg (SQLGlotExpression): The argument to the SUM function.
            types (list[PyDoughType]): The types of the arguments.
        """
        match types[0]:
            # If the argument is of BooleanType, it uses COUNT_IF to count true values.
            case BooleanType():
                return sqlglot_expressions.CountIf(this=arg[0])
            case _:
                # For other types, use SUM directly
                return sqlglot_expressions.Sum(this=arg[0])

    def convert_get_part(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # Check if position is a CAST to BIGINT. If it is change it to INT.

        assert len(args) == 3
        string_expr, delimiter_expr, index_expr = args

        result: SQLGlotExpression = sqlglot_expressions.SplitPart(
            this=string_expr,  # string expression
            delimiter=delimiter_expr,  # delimiter
            part_index=index_expr,  # position (1-based)
        )
        # breakpoint()
        return result

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
        # Build the EXTRACT expression
        extract_expr: SQLGlotExpression = sqlglot_expressions.Extract(
            this=sqlglot_expressions.var("DOW"),  # The field to extract
            expression=base,  # The date column
        )
        return extract_expr

    def apply_datetime_offset(
        self, base: SQLGlotExpression, amt: int, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        """
        Adds/subtracts a datetime interval to to a date/time expression.

        Args:
            `base`: The base date/time expression to add/subtract from.
            `amt`: The amount of the unit to add (if positive) or subtract
            (if negative).
            `unit`: The unit of the interval to add/subtract.

        Returns:
            The SQLGlot expression to add/subtract the specified interval to/from
            `base`.
        """
        if unit == DateTimeUnit.QUARTER:
            unit = DateTimeUnit.MONTH
            amt *= 3

        return super().apply_datetime_offset(base, amt, unit)

    def convert_extract_datetime(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
        unit: DateTimeUnit,
    ) -> SQLGlotExpression:
        assert len(args) == 1

        result = sqlglot_expressions.Extract(
            this=sqlglot_expressions.Var(this=unit.value.upper()),
            expression=self.make_datetime_arg(args[0]),
        )

        if unit == DateTimeUnit.SECOND:
            result = sqlglot_expressions.Cast(
                this=result, to=sqlglot_expressions.DataType.build("BIGINT")
            )

        return result
