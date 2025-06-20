"""
Definition of SQLGlot transformation bindings for the Snowflake dialect.
"""

__all__ = ["SnowflakeTransformBindings"]


import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import PyDoughType
from pydough.types.boolean_type import BooleanType

from .base_transform_bindings import BaseTransformBindings


class SnowflakeTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the Snowflake dialect.
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
            case pydop.STARTSWITH:
                return sqlglot_expressions.Anonymous(
                    this="STARTSWITH", expressions=args
                )
            case pydop.ENDSWITH:
                return sqlglot_expressions.Anonymous(this="ENDSWITH", expressions=args)
            case pydop.CONTAINS:
                return sqlglot_expressions.Anonymous(this="CONTAINS", expressions=args)
            case pydop.SLICE:
                return convert_slice(args, types)
            case pydop.LPAD:
                return sqlglot_expressions.Anonymous(this="LPAD", expressions=args)
            case pydop.RPAD:
                return sqlglot_expressions.Anonymous(this="RPAD", expressions=args)
            case pydop.SIGN:
                return sqlglot_expressions.Anonymous(this="SIGN", expressions=args)
            case pydop.YEAR:
                return sqlglot_expressions.Anonymous(this="YEAR", expressions=args)
            case pydop.QUARTER:
                return sqlglot_expressions.Anonymous(this="QUARTER", expressions=args)
            case pydop.MONTH:
                return sqlglot_expressions.Anonymous(this="MONTH", expressions=args)
            case pydop.DAY:
                return sqlglot_expressions.Anonymous(this="DAY", expressions=args)
            case pydop.HOUR:
                return sqlglot_expressions.Anonymous(this="HOUR", expressions=args)
            case pydop.MINUTE:
                return sqlglot_expressions.Anonymous(this="MINUTE", expressions=args)
            case pydop.SECOND:
                return sqlglot_expressions.Anonymous(this="SECOND", expressions=args)
            case pydop.DAYNAME:
                return sqlglot_expressions.Anonymous(this="DAYNAME", expressions=args)
            case pydop.SMALLEST:
                return sqlglot_expressions.Anonymous(this="LEAST", expressions=args)
            case pydop.LARGEST:
                return sqlglot_expressions.Anonymous(this="GREATEST", expressions=args)

        return super().convert_call_to_sqlglot(operator, args, types)

    def convert_sum(
        self, arg: SQLGlotExpression, types: list[PyDoughType]
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


def convert_slice(
    args: list[SQLGlotExpression], types: list[PyDoughType]
) -> SQLGlotExpression:
    """
    This method handles the conversion of slice operation, which has an
    equivalent in Snowflake SQL as `SUBSTR`.
    The slice arguments in PyDough are in the form of
    `slice(string, start, end, step=1)`, which translates to
    `SUBSTRING(string, start, end - start+1)` in Snowflake SQL.

    Arguments:
        args (list[SQLGlotExpression]): The arguments to the slice operation.
        types (list[PyDoughType]): The types of the arguments.
    """
    string: SQLGlotExpression = args[0]
    # Snowflake's SUBSTR function uses 1-based indexing, so no need to adjust the start index.
    start: SQLGlotExpression = args[1]
    if isinstance(start, sqlglot_expressions.Null):
        start = sqlglot_expressions.Literal.number(1)
    end: SQLGlotExpression = args[2]
    if isinstance(end, sqlglot_expressions.Null):
        # If end is not provided, it defaults to the length of the string.
        # In SQLGlot, we can use LENGTH to get the length of the string.
        end = sqlglot_expressions.Length(this=string)
    # Snowflake's SUBSTR function requires the length of the substring,
    # which is calculated as end - start + 1.
    length: SQLGlotExpression = sqlglot_expressions.convert(end - start + 1)

    # Step is not used in Snowflake's SUBSTR, so it is ignored.
    # return sqlglot_expressions.Anonymous(this="SUBSTR", expressions=[string, start, length])
    return sqlglot_expressions.Substring(this=string, start=start, length=length)
