"""
Class that converts the relational tree to the "executed" forms
of PyDough, which is either returns the SQL text or executes
the query on the database.
"""

import pandas as pd
from sqlglot import parse_one
from sqlglot.dialects import Dialect as SQLGlotDialect
from sqlglot.dialects import SQLite as SQLiteDialect
from sqlglot.errors import SqlglotError
from sqlglot.expressions import Alias, Column, Select, Table
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.optimizer import find_all_in_scope
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.eliminate_ctes import eliminate_ctes
from sqlglot.optimizer.eliminate_joins import eliminate_joins
from sqlglot.optimizer.eliminate_subqueries import eliminate_subqueries
from sqlglot.optimizer.merge_subqueries import merge_subqueries
from sqlglot.optimizer.normalize import normalize
from sqlglot.optimizer.optimize_joins import optimize_joins
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.pushdown_projections import pushdown_projections
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.simplify import simplify

from pydough.configs import PyDoughConfigs
from pydough.database_connectors import (
    DatabaseContext,
    DatabaseDialect,
)
from pydough.logger import get_logger
from pydough.relational import JoinType, JoinTypeRelationalVisitor, RelationalRoot
from pydough.relational.relational_expressions import (
    RelationalExpression,
)

from .sqlglot_relational_visitor import SQLGlotRelationalVisitor

__all__ = ["convert_relation_to_sql", "execute_df"]


def convert_relation_to_sql(
    relational: RelationalRoot,
    dialect: DatabaseDialect,
    config: PyDoughConfigs,
) -> str:
    """
    Convert the given relational tree to a SQL string using the given dialect.

    Args:
        `relational`: The relational tree to convert.
        `dialect`: The dialect to use for the conversion.

    Returns:
        str: The SQL string representing the relational tree.
    """
    glot_expr: SQLGlotExpression = SQLGlotRelationalVisitor(
        dialect, config
    ).relational_to_sqlglot(relational)
    sqlglot_dialect: SQLGlotDialect = convert_dialect_to_sqlglot(dialect)

    # Apply the SQLGlot optimizer to the AST.
    try:
        glot_expr = apply_sqlglot_optimizer(glot_expr, relational, sqlglot_dialect)
    except SqlglotError as e:
        print(
            f"ERROR WHILE OPTIMIZING QUERY:\n{glot_expr.sql(sqlglot_dialect, pretty=True)}"
        )
        raise e

    # Convert the optimized AST back to a SQL string.
    return glot_expr.sql(sqlglot_dialect, pretty=True)


def apply_sqlglot_optimizer(
    glot_expr: SQLGlotExpression, relational: RelationalRoot, dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Apply the SQLGlot optimizer to the given SQLGlot expression.

    Args:
        glot_expr: The SQLGlot expression to optimize.
        relational: The relational tree to optimize the expression for.
        dialect: The dialect to use for the optimization.

    Returns:
        The optimized SQLGlot expression.
    """
    # Convert the SQLGlot AST to a SQL string and back to an AST hoping that
    # SQLGlot will "clean" up the AST to make it more compatible with the
    # optimizer.
    glot_expr = parse_one(glot_expr.sql(dialect), dialect=dialect)

    # Apply each rule explicitly with appropriate kwargs
    # TODO: (gh #313) - Two rules that sqlglot optimizer provides are skipped
    # (unnest_subqueries and canonicalize). Additionally, the rule
    # `merge_subqueries` is skipped if the AST has a `semi` or `anti` join.

    # Rewrite sqlglot AST to have normalized and qualified tables and columns.
    glot_expr = qualify(
        glot_expr, dialect=dialect, quote_identifiers=False, isolate_tables=True
    )

    # Rewrite sqlglot AST to remove unused columns projections.
    glot_expr = pushdown_projections(glot_expr)

    # Rewrite sqlglot AST into conjunctive normal form
    glot_expr = normalize(glot_expr)

    # TODO: (gh #313) RULE SKIPPED
    # Rewrite sqlglot AST to convert some predicates with subqueries into joins.
    # Convert scalar subqueries into cross joins.
    # Convert correlated or vectorized subqueries into a group by so it is not
    # a many to many left join.
    # glot_expr = unnest_subqueries(glot_expr)

    # Rewrite sqlglot AST to pushdown predicates in FROMS and JOINS.
    glot_expr = pushdown_predicates(glot_expr, dialect=dialect)

    # Removes cross joins if possible and reorder joins based on predicate
    # dependencies.
    glot_expr = optimize_joins(glot_expr)

    # Rewrite derived tables as CTES, deduplicating if possible.
    glot_expr = eliminate_subqueries(glot_expr)

    # TODO: (gh #313) RULE SKIPPED for `semi` and `anti` joins.
    join_types = JoinTypeRelationalVisitor().get_join_types(relational)
    if JoinType.ANTI not in join_types and JoinType.SEMI not in join_types:
        glot_expr = merge_subqueries(glot_expr)

    # Remove unused joins from an expression.
    # This only removes joins when we know that the join condition doesn't
    # produce duplicate rows.
    glot_expr = eliminate_joins(glot_expr)

    # Remove unused CTEs from an expression.
    glot_expr = eliminate_ctes(glot_expr)

    # Infers the types of an expression, annotating its AST accordingly.
    # depends on the schema.
    glot_expr = annotate_types(glot_expr, dialect=dialect)

    # TODO: (gh #313) RULE SKIPPED
    # Converts a sql expression into a standard form.
    # This method relies on annotate_types because many of the
    # conversions rely on type inference.
    # glot_expr = canonicalize(glot_expr, dialect=dialect)

    # Rewrite sqlglot AST to simplify expressions.
    glot_expr = simplify(glot_expr, dialect=dialect)

    # Fix column names in the top-level SELECT expressions.
    # The optimizer changes the cases of column names, so we need to
    # match the alias in the relational tree.
    fix_column_case(glot_expr, relational.ordered_columns)

    # Remove table aliases if there is only one Table source in the FROM clause.
    # Skip for Semi and Anti joins. We do this because this function will remove
    # the table alias of the outer query even if it is used in the exists clause.
    # Because, the exists clause is in another "scope" and we are not currently
    # dealing with pan-scope updates.
    if JoinType.ANTI not in join_types and JoinType.SEMI not in join_types:
        remove_table_aliases_conditional(glot_expr)

    return glot_expr


def fix_column_case(
    glot_expr: SQLGlotExpression,
    ordered_columns: list[tuple[str, RelationalExpression]],
) -> None:
    """
    Fixes the column names in the SQLGlot expression to match the case
    of the column names in the original RelationalRoot.

    Args:
        glot_expr: The SQLGlot expression to fix
        ordered_columns: The ordered columns from the RelationalRoot
    """
    # Fix column names in the top-level SELECT expressions
    if hasattr(glot_expr, "expressions"):
        for idx, (col_name, _) in enumerate(ordered_columns):
            expr = glot_expr.expressions[idx]
            # Handle expressions with aliases
            if isinstance(expr, Alias):
                identifier = expr.args.get("alias")
                identifier.set("this", col_name)
            elif isinstance(expr, Column):
                expr.this.this.set("this", col_name)


def remove_table_aliases_conditional(expr: SQLGlotExpression) -> None:
    """
    Visits the AST and removes table aliases if there is only one Table
    source in the FROM clause. Specifically, it removes the alias if the
    table name and alias are the same. It also updates the column names to
    be unqualified if the above condition is met.

    Args:
        expr: The SQLGlot expression to visit.

    Returns:
        None (The AST is modified in place.)
    """
    # Only remove aliases if there are no joins.
    if isinstance(expr, Select) and (
        expr.args.get("joins") is None or len(expr.args.get("joins")) == 0
    ):
        from_clause = expr.args.get("from")
        # Only remove aliases if there is a table in the FROM clause as opposed
        # to a subquery.
        if from_clause is not None and isinstance(from_clause.this, Table):
            # Table(this=Identifier(this=..),..)
            table = from_clause.this
            # actual_table_name = table.name
            # Table(this=..,alias=TableAlias(this=Identifier(this=..)))
            alias = table.alias
            if len(alias) != 0:  # alias exists for the table
                # Remove cases like `..FROM t1 as t1..` or `..FROM t1 as t2..`
                # to get `..FROM t1..`.
                table.args.pop("alias")

                # "Scope" represents the current context of a Select statement.
                # For example, if we have a SELECT statement with a FROM clause
                # that contains a subquery, there are two scopes:
                # 1. The scope of the subquery.
                # 2. The scope of the outer query.
                # This loop is used to find all the columns in the scope of
                # the outer query and replace the qualified column names with
                # the unqualified column names.
                for column in find_all_in_scope(expr, Column):
                    skip: bool = False
                    # Skip if the table alias is not present in the qualified
                    # column name(check correl_11).
                    for part in column.parts[:-1]:
                        if alias != part.name:
                            skip = True
                    if skip:
                        continue
                    for part in column.parts[:-1]:
                        part.pop()

    # Remove aliases from the SELECT expressions if the alias is the same
    # as the column name.
    if isinstance(expr, Select) and expr.args.get("expressions") is not None:
        for i in range(len(expr.expressions)):
            cur_expr = expr.expressions[i]
            if isinstance(cur_expr, Alias) and isinstance(
                cur_expr.args.get("this"), Column
            ):
                if cur_expr.alias == cur_expr.this.name:
                    expr.expressions[i] = cur_expr.this

    # Recursively visit the AST.
    for arg in expr.args.values():
        if isinstance(arg, SQLGlotExpression):
            remove_table_aliases_conditional(arg)
        if isinstance(arg, list):
            for item in arg:
                if isinstance(item, SQLGlotExpression):
                    remove_table_aliases_conditional(item)


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
    config: PyDoughConfigs,
    display_sql: bool = False,
) -> pd.DataFrame:
    """
    Execute the given relational tree on the given database access
    context and return the result.

    Args:
        `relational`: The relational tree to execute.
        `ctx`: The database context to execute the query in.
        `display_sql`: if True, prints out the SQL that will be run before
        it is executed.

    Returns:
        The result of the query as a Pandas DataFrame
    """
    sql: str = convert_relation_to_sql(relational, ctx.dialect, config)
    if display_sql:
        pyd_logger = get_logger(__name__)
        pyd_logger.info(f"SQL query:\n {sql}")
    return ctx.connection.execute_query_df(sql)
