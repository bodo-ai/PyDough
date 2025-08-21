"""
Overridden version of the merge_subqueries.py file from sqlglot.
"""

from __future__ import annotations

import typing as t
from collections import defaultdict

from sqlglot import expressions as exp
from sqlglot.optimizer.merge_subqueries import (
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
from sqlglot.optimizer.scope import Scope, traverse_scope

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from sqlglot.optimizer.merge_subqueries import FromOrJoin


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
    "group",  # PyDough Change: allow group to be mergeable
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

    # PyDough TODO: account for certain situations where a CTE used more than
    # once SHOULD be merged (e.g. just a simple scan + select with nothing to
    # it).
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
            # PyDough Change: merge groups
            _merge_groups(outer_scope, inner_scope)
            _merge_order(outer_scope, inner_scope)
            _merge_hints(outer_scope, inner_scope)
            _pop_cte(inner_scope)
            outer_scope.clear_cache()
    return expression


def _merge_groups(outer_scope: Scope, inner_scope: Scope) -> None:
    """
    Merge GROUP clause of inner query into outer query.

    Args:
        outer_scope (sqlglot.optimizer.scope.Scope)
        inner_scope (sqlglot.optimizer.scope.Scope)
    """
    if outer_scope.expression.args.get("group") or not inner_scope.expression.args.get(
        "group"
    ):
        return

    outer_scope.expression.set("group", inner_scope.expression.args.get("group"))


def invalid_aggregate_convolution(inner_scope: Scope, outer_scope: Scope) -> bool:
    """
    Returns whether the inner scope should not be merge into the outer scope
    due to a scenario where the inner scope contains aggregations while the
    outer scope also contains aggregations, joins, or group-by clauses, or
    the inner scope has a window function, or the outer scope has a join or
    where clause.
    """
    # Temporarily remove the "with" context from the outer scope to avoid
    # using CTEs when considering what the outer scope contains, then restore
    # it before returning.
    with_ctx = outer_scope.expression.args.pop("with", None)
    result: bool = False
    if inner_scope.expression.find(exp.AggFunc) and (
        outer_scope.expression.find(exp.AggFunc)
        or len(outer_scope.expression.args.get("joins", [])) > 0
        or outer_scope.expression.args.get("where") is not None
        or (
            inner_scope.expression.find(exp.Group)
            and (
                inner_scope.expression.find(exp.Window)
                or outer_scope.expression.find(exp.Group)
            )
        )
    ):
        result = True
    # Do not allow merging the inner scope into the outer if the inner contains a grouping
    # key used by the outer scope besides being passed-through.
    if inner_scope.expression.find(exp.Group):
        # Identify all of the expressions in the inner scope, and the aliases they correspond to.
        aliases: list[str] = []
        exprs: list[exp.Expression] = []
        for expr in inner_scope.expression.expressions:
            assert isinstance(expr, exp.Alias)
            aliases.append(expr.alias)
            exprs.append(expr.this)
        # Identify which columns in the inner select list are amongst the grouping keys
        key_column_names: list[str] = []
        for key_expr in inner_scope.expression.find(exp.Group).expressions:
            if isinstance(key_expr, exp.Identifier) and key_expr.this in aliases:
                key_column_names.append(key_expr.this)
            elif key_expr in exprs:
                key_column_names.append(aliases[exprs.index(key_expr)])
        # Search the columns of the outer select list. If any of them are nested
        # expressions that contain one of the key columns, do not allow a merge.
        for outer_expr in outer_scope.expression.expressions:
            assert isinstance(outer_expr, exp.Alias)
            if isinstance(outer_expr.this, exp.Column):
                continue
            contains_match: bool = False
            for sub_expr in outer_expr.this.find_all(exp.Column):
                if (
                    isinstance(sub_expr, exp.Column)
                    and sub_expr.alias_or_name in key_column_names
                ):
                    contains_match = True
                    break
            if contains_match:
                result = True
                break
    outer_scope.expression.args["with"] = with_ctx
    return result


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

    # PYDOUGH CHANGE: avoid merging CTEs when the inner scope has a window
    # expression and the outer scope has a join.
    if (
        inner_scope.expression.find(exp.Window)
        and outer_scope.expression.args.get("joins") is not None
    ):
        return False

    inner_select = inner_scope.expression.unnest()

    def _is_a_window_expression_in_unmergable_operation():
        window_expressions = inner_select.find_all(exp.Window)
        window_alias_names = {
            window.parent.alias_or_name for window in window_expressions
        }
        inner_select_name = from_or_join.alias_or_name
        unmergable_window_columns = [
            column
            for column in outer_scope.columns
            if column.find_ancestor(
                exp.Where, exp.Group, exp.Order, exp.Join, exp.Having, exp.AggFunc
            )
        ]
        window_expressions_in_unmergable = [
            column
            for column in unmergable_window_columns
            if column.table == inner_select_name and column.name in window_alias_names
        ]
        return any(window_expressions_in_unmergable)

    def _outer_select_joins_on_inner_select_join():
        """
        All columns from the inner select in the ON clause must be from the first FROM table.

        That is, this can be merged:
            SELECT * FROM x JOIN (SELECT y.a AS a FROM y JOIN z) AS q ON x.a = q.a
                                         ^^^           ^
        But this can't:
            SELECT * FROM x JOIN (SELECT z.a AS a FROM y JOIN z) AS q ON x.a = q.a
                                         ^^^                  ^
        """
        if not isinstance(from_or_join, exp.Join):
            return False

        alias = from_or_join.alias_or_name

        on = from_or_join.args.get("on")
        if not on:
            return False
        selections = [c.name for c in on.find_all(exp.Column) if c.table == alias]
        inner_from = inner_scope.expression.args.get("from")
        if not inner_from:
            return False
        inner_from_table = inner_from.alias_or_name
        inner_projections = {s.alias_or_name: s for s in inner_scope.expression.selects}
        return any(
            col.table != inner_from_table
            for selection in selections
            for col in inner_projections[selection].find_all(exp.Column)
        )

    def _is_recursive():
        # Recursive CTEs look like this:
        #     WITH RECURSIVE cte AS (
        #       SELECT * FROM x  <-- inner scope
        #       UNION ALL
        #       SELECT * FROM cte  <-- outer scope
        #     )
        cte = inner_scope.expression.parent
        node = outer_scope.expression.parent

        while node:
            if node is cte:
                return True
            node = node.parent
        return False

    return (
        isinstance(outer_scope.expression, exp.Select)
        and not outer_scope.expression.is_star
        and isinstance(inner_select, exp.Select)
        and not any(inner_select.args.get(arg) for arg in UNMERGABLE_ARGS)
        and inner_select.args.get("from") is not None
        and not outer_scope.pivots
        and not any(e.find(exp.Select, exp.Explode) for e in inner_select.expressions)
        # PYDOUGH CHANGE: allow merging when the inner select has an
        # aggregation, as long as the outer select does not, and so long as the
        # grouping keys are not used in the outer select besides pass-through.
        and not invalid_aggregate_convolution(inner_scope, outer_scope)
        and not (leave_tables_isolated and len(outer_scope.selected_sources) > 1)
        and not (
            isinstance(from_or_join, exp.Join)
            and inner_select.args.get("where")
            and from_or_join.side in ("FULL", "LEFT", "RIGHT")
        )
        and not (
            isinstance(from_or_join, exp.From)
            and inner_select.args.get("where")
            and any(
                j.side in ("FULL", "RIGHT")
                for j in outer_scope.expression.args.get("joins", [])
            )
        )
        and not _outer_select_joins_on_inner_select_join()
        and not _is_a_window_expression_in_unmergable_operation()
        and not _is_recursive()
        and not (inner_select.args.get("order") and outer_scope.is_union)
    )
