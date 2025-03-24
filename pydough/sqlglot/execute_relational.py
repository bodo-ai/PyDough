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

from pydough.database_connectors import (
    DatabaseContext,
    DatabaseDialect,
)
from pydough.logger import get_logger
from pydough.relational import RelationalRoot

from .sqlglot_relational_visitor import SQLGlotRelationalVisitor
from .transform_bindings import SqlGlotTransformBindings

__all__ = ["convert_relation_to_sql", "execute_df"]


def convert_relation_to_sql(
    relational: RelationalRoot,
    dialect: SQLGlotDialect,
    bindings: SqlGlotTransformBindings,
) -> str:
    """
    Convert the given relational tree to a SQL string using the given dialect.

    Args:
        `relational`: The relational tree to convert.
        `dialect`: The dialect to use for the conversion.
        `bindings`: The function bindings used for conversion.

    Returns:
        str: The SQL string representing the relational tree.
    """
    glot_expr: SQLGlotExpression = SQLGlotRelationalVisitor(
        dialect, bindings
    ).relational_to_sqlglot(relational)
    glot_expr = parse_one(glot_expr.sql(dialect))
    print(glot_expr.sql(dialect, pretty=True))
    breakpoint()
    import inspect

    from sqlglot.optimizer import RULES as rules

    rules = rules[:1] + rules[2:10] + rules[11:]
    for rule in rules:
        kwargs = {}
        rule_params = inspect.getfullargspec(rule).args
        if "dialect" in rule_params:
            kwargs["dialect"] = dialect
        if "quote_identifiers" in rule_params:
            kwargs["quote_identifiers"] = False
        if "leave_tables_isolated" in rule_params:
            kwargs["leave_tables_isolated"] = True
        glot_expr = rule(glot_expr, **kwargs)
        print("*" * 50)
        print(rule.__name__)
        print(glot_expr.sql(dialect, pretty=True))
    # glot_expr = optimize(glot_expr, rules=rules, dialect=dialect)
    # glot_expr = optimize(parse_one(glot_expr.sql(dialect)), dialect=dialect)
    breakpoint()
    return glot_expr.sql(dialect, pretty=True)


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

    Returns:
        The result of the query as a Pandas DataFrame
    """
    sqlglot_dialect: SQLGlotDialect = convert_dialect_to_sqlglot(ctx.dialect)
    sql: str = convert_relation_to_sql(relational, sqlglot_dialect, bindings)
    if display_sql:
        pyd_logger = get_logger(__name__)
        pyd_logger.info(f"SQL query:\n {sql}")
    return ctx.connection.execute_query_df(sql)
