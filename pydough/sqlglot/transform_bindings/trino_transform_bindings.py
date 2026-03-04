"""
Definition of SQLGlot transformation bindings for the Trino dialect.
"""

__all__ = ["TrinoTransformBindings"]


import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import PyDoughType

from .base_transform_bindings import BaseTransformBindings


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
