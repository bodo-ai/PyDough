"""
Overridden version of the merge_subqueries.py file from sqlglot.
"""

from __future__ import annotations

import typing as t
from collections import defaultdict

from sqlglot import expressions as exp
from sqlglot.optimizer.merge_subqueries import (
    FromOrJoin,
    _merge_expressions,
    _merge_from,
    _merge_hints,
    _merge_joins,
    _merge_order,
    _merge_where,
    _pop_cte,
    _rename_inner_sources,
    merge_derived_tables,
)
from sqlglot.optimizer.merge_subqueries import _mergeable as _old_mergeable
from sqlglot.optimizer.scope import Scope, traverse_scope

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def merge_subqueries(expression: E, leave_tables_isolated: bool = False) -> E:
    """
    Rewrite sqlglot AST to merge derived tables into the outer query.

    This also merges CTEs if they are selected from only once.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT a FROM (SELECT x.a FROM x) CROSS JOIN y")
        >>> merge_subqueries(expression).sql()
        'SELECT x.a FROM x CROSS JOIN y'

    If `leave_tables_isolated` is True, this will not merge inner queries into outer
    queries if it would result in multiple table selects in a single query:
        >>> expression = sqlglot.parse_one("SELECT a FROM (SELECT x.a FROM x) CROSS JOIN y")
        >>> merge_subqueries(expression, leave_tables_isolated=True).sql()
        'SELECT a FROM (SELECT x.a FROM x) CROSS JOIN y'

    Inspired by https://dev.mysql.com/doc/refman/8.0/en/derived-table-optimization.html

    Args:
        expression (sqlglot.Expression): expression to optimize
        leave_tables_isolated (bool):
    Returns:
        sqlglot.Expression: optimized expression
    """
    expression = merge_ctes(expression, leave_tables_isolated)
    expression = merge_derived_tables(expression, leave_tables_isolated)
    return expression


# If a derived table has these Select args, it can't be merged
UNMERGABLE_ARGS = set(exp.Select.arg_types) - {
    "expressions",
    "from",
    "joins",
    "where",
    "order",
    "hint",
}


def merge_ctes(expression: E, leave_tables_isolated: bool = False) -> E:
    scopes = traverse_scope(expression)

    # All places where we select from CTEs.
    # We key on the CTE scope so we can detect CTES that are selected from multiple times.
    cte_selections = defaultdict(list)
    for outer_scope in scopes:
        for table, inner_scope in outer_scope.selected_sources.values():
            if isinstance(inner_scope, Scope) and inner_scope.is_cte:
                cte_selections[id(inner_scope)].append(
                    (
                        outer_scope,
                        inner_scope,
                        table,
                    )
                )

    singular_cte_selections = [v[0] for k, v in cte_selections.items() if len(v) == 1]
    for outer_scope, inner_scope, table in singular_cte_selections:
        from_or_join = table.find_ancestor(exp.From, exp.Join)
        if _mergeable(outer_scope, inner_scope, leave_tables_isolated, from_or_join):
            alias = table.alias_or_name
            _rename_inner_sources(outer_scope, inner_scope, alias)
            _merge_from(outer_scope, inner_scope, table, alias)
            _merge_expressions(outer_scope, inner_scope, alias)
            _merge_joins(outer_scope, inner_scope, from_or_join)
            _merge_where(outer_scope, inner_scope, from_or_join)
            _merge_order(outer_scope, inner_scope)
            _merge_hints(outer_scope, inner_scope)
            _pop_cte(inner_scope)
            outer_scope.clear_cache()
    return expression


def _mergeable(
    outer_scope: Scope,
    inner_scope: Scope,
    leave_tables_isolated: bool,
    from_or_join: FromOrJoin,
) -> bool:
    """
    Overridden version of the original `_mergeable`.
    """
    # PYDOUGH CHANGE: avoid merging CTEs when it would break a left join.
    if isinstance(from_or_join, exp.Join) and from_or_join.side not in ("INNER", ""):
        return False
    # Otherwise, fall back on the original implementation.
    return _old_mergeable(outer_scope, inner_scope, leave_tables_isolated, from_or_join)
