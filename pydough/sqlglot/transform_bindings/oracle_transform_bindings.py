"""
Definition of SQLGlot transformation bindings for the Oracle dialect.
"""

__all__ = ["OracleTransformBindings"]


import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import PyDoughType

from .base_transform_bindings import BaseTransformBindings


class OracleTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the Oracle dialect.
    """

    PYDOP_TO_ORACLE_FUNC: dict[pydop.PyDoughExpressionOperator, str] = {
        pydop.ABS: "ABS",
        pydop.DEFAULT_TO: "NVL",
        pydop.LARGEST: "GREATEST",
        pydop.SMALLEST: "LEAST",
        pydop.STRIP: "TRIM",
        pydop.FIND: "INSTR",
        pydop.SLICE: "SUBSTR",
        pydop.JOIN_STRINGS: "LISTAGG",
        pydop.PERCENTILE: "PERCENTILE_CONT",
    }

    """
    Mapping of PyDough operators to equivalent Oracle function names
    These are used to generate anonymous function calls in SQLGlot
    """

    def convert_call_to_sqlglot(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        if operator in self.PYDOP_TO_ORACLE_FUNC:
            return sqlglot_expressions.Anonymous(
                this=self.PYDOP_TO_ORACLE_FUNC[operator], expressions=args
            )

        return super().convert_call_to_sqlglot(operator, args, types)
