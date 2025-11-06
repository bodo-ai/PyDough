"""
Definition of SQLGlot transformation bindings for the Snowflake dialect.
"""

__all__ = ["SnowflakeTransformBindings"]


import math

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import PyDoughType
from pydough.types.boolean_type import BooleanType
from pydough.user_collections.range_collection import RangeGeneratedCollection

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import DateTimeUnit


class SnowflakeTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the Snowflake dialect.
    """

    PYDOP_TO_SNOWFLAKE_FUNC: dict[pydop.PyDoughExpressionOperator, str] = {
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
        if operator in self.PYDOP_TO_SNOWFLAKE_FUNC:
            return sqlglot_expressions.Anonymous(
                this=self.PYDOP_TO_SNOWFLAKE_FUNC[operator], expressions=args
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
        if unit is DateTimeUnit.WEEK:
            args = [
                args[0],
                self.make_datetime_arg(args[1]),
                self.make_datetime_arg(args[2]),
            ]
            # 1. For both dates, get # of shifted of days since the start of week
            shifted_start: SQLGlotExpression = self.days_from_start_of_week(args[1])
            shifted_end: SQLGlotExpression = self.days_from_start_of_week(args[2])

            # 2. Subtract shifted_weekday DAYS from the datetime

            date_sub_start: SQLGlotExpression = sqlglot_expressions.DateSub(
                this=args[1],
                expression=shifted_start,
                unit=sqlglot_expressions.Var(this="DAY"),
            )

            date_sub_end: SQLGlotExpression = sqlglot_expressions.DateSub(
                this=args[2],
                expression=shifted_end,
                unit=sqlglot_expressions.Var(this="DAY"),
            )

            # 3. Call DATEDIFF in weeks with the shifted dates
            return sqlglot_expressions.DateDiff(
                unit=sqlglot_expressions.Var(this=unit.value),
                this=date_sub_end,
                expression=date_sub_start,
            )
        else:
            # For other units, use base implementation
            return super().convert_datediff(args, types)

    def convert_user_generated_range(
        self, collection: RangeGeneratedCollection
    ) -> SQLGlotExpression:
        """
        Converts a user-generated range collection to its Snowflake SQLGlot
        representation.
        Arguments:
            `collection` : The user-generated range collection to convert.
        Returns:
            A SQLGlotExpression representing the user-generated range as table.
        """

        # Calculate the number of rows needed for the range (end-start)/step
        row_count: int = math.ceil(
            (collection.end - collection.start) / collection.step
        )

        # Handle empty range by injecting a single NULL row
        # SELECT CAST(NULL AS INT) AS x WHERE FALSE
        if row_count <= 0:
            query: SQLGlotExpression = sqlglot_expressions.Select(
                expressions=[
                    sqlglot_expressions.Alias(
                        this=sqlglot_expressions.Cast(
                            this=sqlglot_expressions.Null(),
                            to=sqlglot_expressions.DataType.build("INTEGER"),
                        ),
                        alias=sqlglot_expressions.Identifier(
                            this=collection.column_name
                        ),
                    )
                ],
            ).where(sqlglot_expressions.false())

        else:
            # Build the SQLGlot query using Snowflake's GENERATOR function
            #   WITH table_name AS (
            #       SELECT
            #           start + SEQ4() * step AS column_name
            #       FROM TABLE(GENERATOR(ROWCOUNT => row_count))
            #   )
            #   SELECT column_name FROM table_name

            # Step 1. Build the base expression: SEQ4() * step
            # (or just SEQ4() if step == 1)
            if collection.step == 1:
                seq4_expr = sqlglot_expressions.Anonymous(this="SEQ4")
            else:
                seq4_expr = sqlglot_expressions.Mul(
                    this=sqlglot_expressions.Anonymous(this="SEQ4"),
                    expression=sqlglot_expressions.Literal.number(collection.step),
                )

            # Step 2. Add start if start != 0
            # Final expression: start + SEQ4() * step
            if collection.start != 0:
                final_expr = sqlglot_expressions.Add(
                    this=sqlglot_expressions.Literal.number(collection.start),
                    expression=seq4_expr,
                )
            else:
                final_expr = seq4_expr

            # 3. Build the inner SELECT
            # SELECT start + SEQ4() * step AS column_name
            # FROM TABLE(GENERATOR(ROWCOUNT => row_count))
            inner_select: SQLGlotExpression = sqlglot_expressions.Select(
                expressions=[
                    sqlglot_expressions.Alias(
                        this=final_expr,
                        alias=sqlglot_expressions.Identifier(
                            this=collection.column_name
                        ),
                    )
                ]
            ).from_(
                sqlglot_expressions.Table(
                    this=sqlglot_expressions.Anonymous(
                        this="TABLE",
                        expressions=[
                            sqlglot_expressions.Anonymous(
                                this="GENERATOR",
                                expressions=[
                                    sqlglot_expressions.Kwarg(
                                        this=sqlglot_expressions.Var(this="ROWCOUNT"),
                                        expression=sqlglot_expressions.Literal.number(
                                            row_count
                                        ),
                                    )
                                ],
                            )
                        ],
                    )
                )
            )

            # 4. Wrap it as a subquery with alias
            # WITH table_name AS ( ...inner_select... )
            subquery: SQLGlotExpression = sqlglot_expressions.Subquery(
                this=inner_select,
                alias=sqlglot_expressions.Identifier(this=collection.name),
            )

            # 5. Outer SELECT that references the subquery
            # SELECT column_name FROM table_name
            query = sqlglot_expressions.Select(
                expressions=[
                    sqlglot_expressions.Column(
                        this=collection.column_name, table=collection.name
                    )
                ]
            ).from_(subquery)

        return query
