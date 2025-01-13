"""
Class that converts the relational tree to the "executed" forms
of PyDough, which is either returns the SQL text or executes
the query on the database.
"""

import pandas as pd
from sqlglot import parse_one
from sqlglot.dialects import Dialect as SQLGlotDialect
from sqlglot.dialects import SQLite as SQLiteDialect
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.optimizer import optimize

from pydough.database_connectors import (
    DatabaseContext,
    DatabaseDialect,
)
from pydough.relational import RelationalRoot

from .sqlglot_relational_visitor import SQLGlotRelationalVisitor
from .transform_bindings import SqlGlotTransformBindings

__all__ = ["convert_relation_to_sql", "execute_df"]


def convert_relation_to_sql(
    relational: RelationalRoot,
    dialect: SQLGlotDialect,
    bindings: SqlGlotTransformBindings,
    run_optimizer: bool,
) -> str:
    """
    Convert the given relational tree to a SQL string using the given dialect.

    Args:
        `relational`: The relational tree to convert.
        `dialect`: The dialect to use for the conversion.
        `bindings`: The function bindings used for conversion.
        `run_optimizer`: If True, runs the SQLGlot optimizer before converting
        to SQL text.

    Returns:
        str: The SQL string representing the relational tree.
    """
    glot_expr: SQLGlotExpression = SQLGlotRelationalVisitor(
        dialect, bindings
    ).relational_to_sqlglot(relational)
    sql_text: str = glot_expr.sql(dialect)
    if run_optimizer:
        print()
        print(sql_text)
        print()
        glot_expr = parse_one(sql_text, dialect=dialect)

        # glot_expr = optimize(glot_expr, dialect=dialect, rules=RULES[:1])
        glot_expr = optimize(glot_expr, dialect=dialect)
        sql_text = glot_expr.sql(dialect)
    return sql_text


def convert_dialect_to_sqlglot(dialect: DatabaseDialect) -> SQLGlotDialect:
    """
    Convert the given DatabaseDialect to the corresponding SQLGlotDialect.

    Args:
        dialect (DatabaseDialect): The dialect to convert.

    Returns:
        SQLGlotDialect: The corresponding SQLGlot dialect.
    """
    if dialect == DatabaseDialect.ANSI:
        # Note: ANSI is the base dialect for SQLGlot.
        return SQLGlotDialect()
    elif dialect == DatabaseDialect.SQLITE:
        return SQLiteDialect()
    else:
        raise ValueError(f"Unsupported dialect: {dialect}")


def execute_df(
    relational: RelationalRoot,
    ctx: DatabaseContext,
    bindings: SqlGlotTransformBindings,
    display_sql: bool = False,
    run_optimizer: bool = True,
) -> pd.DataFrame:
    """
    Execute the given relational tree on the given database access
    context and return the result.

    Args:
        `relational`: The relational tree to execute.
        `ctx`: The database context to execute the query in.
        `bindings`: The function transformation bindings used to convert
        PyDough operators into SQLGlot expressions.
        `display_sql`: if True, prints out the SQL that will be run before
        it is executed.
        `run_optimizer`: If True, runs the SQLGlot optimizer before converting
        to SQL text.

    Returns:
        The result of the query as a Pandas DataFrame
    """
    sqlglot_dialect: SQLGlotDialect = convert_dialect_to_sqlglot(ctx.dialect)
    sql: str = convert_relation_to_sql(
        relational, sqlglot_dialect, bindings, run_optimizer
    )
    # TODO: (gh #163) handle with a proper Python logger instead of
    # just printing
    if display_sql:
        print("SQL query:\n", sql)
    return ctx.connection.execute_query_df(sql)
