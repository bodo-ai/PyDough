"""
Class that converts the relational tree to the "executed" forms
of PyDough, which is either returns the SQL text or executes
the query on the database.
"""

from sqlglot.dialects import Dialect as SQLGlotDialect
from sqlglot.expressions import Expression as SQLGlotExpression

from pydough.relational.relational_nodes import RelationalRoot

from .sqlglot_relational_visitor import SQLGlotRelationalVisitor

__all__ = ["convert_relation_to_sql"]


def convert_relation_to_sql(relational: RelationalRoot, dialect: SQLGlotDialect) -> str:
    """
    Convert the given relational tree to a SQL string using the given dialect.

    Args:
        relational (RelationalRoot): The relational tree to convert.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        str: The SQL string representing the relational tree.
    """
    glot_expr: SQLGlotExpression = SQLGlotRelationalVisitor().relational_to_sqlglot(
        relational
    )
    return glot_expr.sql(dialect)
