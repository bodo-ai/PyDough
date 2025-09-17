"""
Class that converts the relational tree to the "executed" forms
of PyDough, which is either returns the SQL text or executes
the query on the database.
"""

import pandas as pd
import sqlglot.expressions as sqlglot_expressions
from sqlglot import parse_one
from sqlglot.dialects import Dialect as SQLGlotDialect
from sqlglot.dialects import MySQL as MySQLDialect
from sqlglot.dialects import Postgres as PostgresDialect
from sqlglot.dialects import Snowflake as SnowflakeDialect
from sqlglot.dialects import SQLite as SQLiteDialect
from sqlglot.errors import SqlglotError
from sqlglot.expressions import Alias, Column, Select, Table, With
from sqlglot.expressions import Collate as SQLGlotCollate
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.optimizer import find_all_in_scope
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.eliminate_ctes import eliminate_ctes
from sqlglot.optimizer.eliminate_joins import eliminate_joins
from sqlglot.optimizer.eliminate_subqueries import eliminate_subqueries
from sqlglot.optimizer.normalize import normalize
from sqlglot.optimizer.optimize_joins import optimize_joins
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import traverse_scope, walk_in_scope

import pydough
from pydough.configs import PyDoughConfigs
from pydough.database_connectors import (
    DatabaseContext,
    DatabaseDialect,
)
from pydough.logger import get_logger
from pydough.relational import RelationalRoot
from pydough.relational.relational_expressions import (
    RelationalExpression,
)

from .override_canonicalize import canonicalize
from .override_merge_subqueries import merge_subqueries
from .override_pushdown_predicates import pushdown_predicates
from .override_pushdown_projections import pushdown_projections
from .override_simplify import simplify
from .override_unnest_subqueries import unnest_subqueries
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
        The SQL string representing the relational tree.
    """
    glot_expr: SQLGlotExpression = SQLGlotRelationalVisitor(
        dialect, config
    ).relational_to_sqlglot(relational)
    sqlglot_dialect: SQLGlotDialect = convert_dialect_to_sqlglot(dialect)

    # Apply the SQLGlot optimizer to the AST.
    try:
        glot_expr = apply_sqlglot_optimizer(glot_expr, relational, sqlglot_dialect)
    except SqlglotError as e:
        sql_text: str = glot_expr.sql(sqlglot_dialect, pretty=True)
        print(f"ERROR WHILE OPTIMIZING QUERY:\n{sql_text}")
        raise pydough.active_session.error_builder.sql_runtime_failure(
            sql_text, e, False
        ) from e

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

    kwargs = {
        "quote_identifiers": False,
        "isolate_tables": True,
        "validate_qualify_columns": False,
        "expand_alias_refs": False,
    }
    # Exclude Snowflake dialect to avoid some issues
    # related to name qualification
    if not isinstance(dialect, SnowflakeDialect):
        kwargs["dialect"] = dialect

    # Rewrite sqlglot AST to have normalized and qualified tables and columns.
    glot_expr = qualify(glot_expr, **kwargs)

    # Rewrite sqlglot AST to remove unused columns projections.
    glot_expr = pushdown_projections(glot_expr)

    # Rewrite sqlglot AST into conjunctive normal form
    glot_expr = normalize(glot_expr)

    # Rewrite sqlglot AST to convert some predicates with subqueries into joins.
    # Convert scalar subqueries into cross joins.
    # Convert correlated or vectorized subqueries into a group by so it is not
    # a many to many left join.
    # PyDough skips this step if there are any recursive CTEs in the query, due
    # to flaws in how SQLGlot handles such subqueries.
    if not any(e.args.get("recursive") for e in glot_expr.find_all(With)):
        glot_expr = unnest_subqueries(glot_expr)

    # limit clauses, which is not correct.
    # Rewrite sqlglot AST to pushdown predicates in FROMS and JOINS.
    glot_expr = pushdown_predicates(glot_expr, dialect=dialect)

    # Removes cross joins if possible and reorder joins based on predicate
    glot_expr = optimize_joins(glot_expr)

    # Rewrite derived tables as CTES, deduplicating if possible.
    glot_expr = eliminate_subqueries(glot_expr)

    # Merge subqueries into one another if possible.
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

    # Converts a sql expression into a standard form.
    glot_expr = canonicalize(glot_expr, dialect=dialect)

    # Rewrite sqlglot AST to simplify expressions.
    glot_expr = simplify(glot_expr, dialect=dialect)

    # Fix column names in the top-level SELECT expressions.
    # The optimizer changes the cases of column names, so we need to
    # match the alias in the relational tree.
    fix_column_case(glot_expr, relational.ordered_columns)

    # Replaces any grouping or ordering keys that point to a clause in the
    # SELECT with an index (e.g. ORDER BY 1, GROUP BY 1, 2)
    replace_keys_with_indices(glot_expr)

    # Remove table aliases if there is only one Table source in the FROM clause.
    remove_table_aliases_conditional(glot_expr)

    return glot_expr


def replace_keys_with_indices(glot_expr: SQLGlotExpression) -> None:
    """
    Runs a transformation postprocessing pass on the SQLGlot AST to make the
    following changes:
    - Replace ORDER BY keys that are in the select clause with indices, and if
      they have a COLLATE, move the COLLATE from the ORDER BY key to the
      operation in the select clause.
    - Replace GROUP BY keys that are in the select clause with indices, unless
      the key appears multiple times in the select clause (e.g. as a top level
      expression and as a subexpression in other scalar expressions).
    - If any window function ordering key expressions have become literals,
      delete and/or replace them with '1'
    """
    assert isinstance(glot_expr, Select)

    for scope in traverse_scope(glot_expr):
        expression = scope.expression

        # Obtain all the raw expressions in the SELECT clause, without aliases.
        expressions: list[SQLGlotExpression] = [
            expr.this if isinstance(expr, Alias) else expr
            for expr in expression.expressions
        ]
        expr_idx: int

        # Replace ORDER BY keys that are in the select clause with indices. This
        # includes cases where the entire ORDER BY key is in the select clause,
        # or a subexpression inside COLLATE is. If it is a collate, change the
        # original expression to include the collate instead.
        if expression.args.get("order") is not None:
            order_list: list[SQLGlotExpression] = expression.args["order"].expressions
            aliases: list[str | None] = []
            for expr in expression.expressions:
                if isinstance(expr, Alias):
                    aliases.append(expr.alias.lower())
                elif isinstance(expr, Column):
                    aliases.append(expr.name.lower())
                else:
                    aliases.append(None)
            for idx, order_expr in enumerate(order_list):
                if order_expr.this in expressions or (
                    isinstance(order_expr.this, Column)
                    and order_expr.this.name.lower() in aliases
                ):
                    if order_expr.this in expressions:
                        expr_idx = expressions.index(order_expr.this)
                    else:
                        expr_idx = aliases.index(order_expr.this.name.lower())
                    order_list[idx].set(
                        "this",
                        sqlglot_expressions.convert(expr_idx + 1),
                    )
                elif isinstance(order_expr.this, SQLGlotCollate) and (
                    order_expr.this.this in expressions
                    or (
                        isinstance(order_expr.this.this, Column)
                        and order_expr.this.this.name.lower() in aliases
                    )
                ):
                    collate: SQLGlotExpression = order_expr.this
                    if order_expr.this.this in expressions:
                        expr_idx = expressions.index(collate.this)
                    else:
                        expr_idx = aliases.index(collate.this.this.name.lower())
                    # Remove the COLLATE from the order expression, but change
                    # the original expression to include the collate.
                    order_list[idx].set(
                        "this",
                        sqlglot_expressions.convert(expr_idx + 1),
                    )
                    if isinstance(expression.expressions[expr_idx], Alias):
                        expression.expressions[expr_idx].set("this", collate)
                    else:
                        expression.expressions[expr_idx] = collate

        # Replace GROUP BY keys that are in the select clause with indices.
        if expression.args.get("group") is not None:
            keys_list: list[SQLGlotExpression] = expression.args["group"].expressions
            for idx, key_expr in enumerate(keys_list):
                # Only replace with the index if the key expression appears in
                # the select list exactly once. Otherwise, replace with the
                # alias.
                if key_expr in expressions:
                    expr_idx = expressions.index(key_expr)
                    n_match: int = 0
                    for select_elem in expressions:
                        if not select_elem.find(sqlglot_expressions.AggFunc):
                            for exp in walk_in_scope(select_elem):
                                if exp == key_expr:
                                    n_match += 1
                    if n_match <= 1:
                        keys_list[idx] = sqlglot_expressions.convert(expr_idx + 1)
                    elif isinstance(expression.expressions[expr_idx], Alias):
                        keys_list[idx] = sqlglot_expressions.Identifier(
                            this=expression.expressions[expr_idx].alias, quoted=False
                        )
            keys_list.sort(key=repr)

    # Now iterate through all window functions and replace any ordering keys
    # that are literals with '1'.
    for window_expr in glot_expr.find_all(sqlglot_expressions.Window):
        if window_expr.args.get("order") is not None:
            exprs: list[SQLGlotExpression] = window_expr.args.get("order").expressions
            original_length: int = len(exprs)
            for idx in range(len(exprs) - 1, -1, -1):
                order_expr = exprs[idx]
                if isinstance(order_expr.this, sqlglot_expressions.Literal):
                    exprs.pop(idx)
            if len(exprs) == 0 and original_length > 0:
                exprs.append(sqlglot_expressions.convert("1"))


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
        `dialect` The dialect to convert.

    Returns:
        The corresponding SQLGlot dialect.
    """
    match dialect:
        case DatabaseDialect.ANSI:
            # Note: ANSI is the base dialect for SQLGlot.
            return SQLGlotDialect()
        case DatabaseDialect.SQLITE:
            return SQLiteDialect()
        case DatabaseDialect.SNOWFLAKE:
            return SnowflakeDialect()
        case DatabaseDialect.MYSQL:
            return MySQLDialect()
        case DatabaseDialect.POSTGRES:
            return PostgresDialect()
        case _:
            raise NotImplementedError(f"Unsupported dialect: {dialect}")


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
