"""
Definition of SQLGlot transformation bindings for the BodoSQL dialect.
"""

__all__ = ["BodoSQLTransformBindings"]


import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

from pydough.relational.relational_expressions.literal_expression import (
    LiteralExpression,
)
from pydough.user_collections.range_collection import RangeGeneratedCollection

from .sf_transform_bindings import SnowflakeTransformBindings
from .sqlglot_transform_utils import (
    create_constant_table,
    generate_range_rows,
)


class BodoSQLTransformBindings(SnowflakeTransformBindings):
    """
    Subclass of SnowflakeTransformBindings for the BodoSQL dialect.
    """

    def convert_literal_expression(
        self,
        arg: LiteralExpression,
    ) -> SQLGlotExpression:
        return sqlglot_expressions.convert(arg.value)

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

        # Generate rows for the range, using Tuple.
        range_rows: list[SQLGlotExpression] = generate_range_rows(collection, self)

        result: SQLGlotExpression = create_constant_table(
            collection.name, [collection.column_name], range_rows, self
        )

        return result
