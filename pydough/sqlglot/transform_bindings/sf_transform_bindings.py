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
        PYDOP_TO_SNOWFLAKE_FUNC: dict[pydop.PyDoughExpressionOperator, str] = {
            pydop.STARTSWITH: "STARTSWITH",
            pydop.ENDSWITH: "ENDSWITH",
            pydop.CONTAINS: "CONTAINS",
            pydop.LPAD: "LPAD",
            pydop.RPAD: "RPAD",
            pydop.SIGN: "SIGN",
            pydop.YEAR: "YEAR",
            pydop.QUARTER: "QUARTER",
            pydop.MONTH: "MONTH",
            pydop.DAY: "DAY",
            pydop.HOUR: "HOUR",
            pydop.MINUTE: "MINUTE",
            pydop.SECOND: "SECOND",
            pydop.DAYNAME: "DAYNAME",
            pydop.SMALLEST: "LEAST",
            pydop.LARGEST: "GREATEST",
        }
        if operator in PYDOP_TO_SNOWFLAKE_FUNC:
            return sqlglot_expressions.Anonymous(
                this=PYDOP_TO_SNOWFLAKE_FUNC[operator], expressions=args
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
