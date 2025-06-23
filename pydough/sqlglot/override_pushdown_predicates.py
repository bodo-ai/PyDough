"""
Overridden version of the pushdown_predicates.py file from sqlglot.
"""

from sqlglot import exp
from sqlglot.optimizer.pushdown_predicates import pushdown
from sqlglot.optimizer.scope import build_scope


def pushdown_predicates(expression, dialect=None):
    """
    Rewrite sqlglot AST to pushdown predicates in FROMS and JOINS

    Example:
        >>> import sqlglot
        >>> sql = "SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y WHERE y.a = 1"
        >>> expression = sqlglot.parse_one(sql)
        >>> pushdown_predicates(expression).sql()
        'SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x WHERE x.a = 1) AS y WHERE TRUE'

    Args:
        expression (sqlglot.Expression): expression to optimize
    Returns:
        sqlglot.Expression: optimized expression
    """
    root = build_scope(expression)

    if root:
        scope_ref_count = root.ref_count()

        for scope in reversed(list(root.traverse())):
            select = scope.expression
            where = select.args.get("where")
            if where:
                selected_sources = scope.selected_sources
                join_index = {
                    join.alias_or_name: i
                    for i, join in enumerate(select.args.get("joins") or [])
                }

                # PyDough Change: remove any sources that have a "limit"
                # clause from consideration for pushdown, as a filter cannot
                # be moved before a limit if it previously occurred after.
                selected_sources = {
                    k: (node, source)
                    for k, (node, source) in selected_sources.items()
                    if node.args.get("limit") is None
                }

                # a right join can only push down to itself and not the source FROM table
                for k, (node, source) in selected_sources.items():
                    parent = node.find_ancestor(exp.Join, exp.From)
                    if isinstance(parent, exp.Join) and parent.side == "RIGHT":
                        selected_sources = {k: (node, source)}
                        break

                pushdown(
                    where.this, selected_sources, scope_ref_count, dialect, join_index
                )

            # joins should only pushdown into itself, not to other joins
            # so we limit the selected sources to only itself
            for join in select.args.get("joins") or []:
                name = join.alias_or_name
                if name in scope.selected_sources:
                    pushdown(
                        join.args.get("on"),
                        {name: scope.selected_sources[name]},
                        scope_ref_count,
                        dialect,
                    )

    return expression
