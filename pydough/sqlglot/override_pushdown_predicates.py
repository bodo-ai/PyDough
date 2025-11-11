"""
Overridden version of the pushdown_predicates.py file from sqlglot.
"""

from sqlglot import exp
from sqlglot.optimizer.normalize import normalized
from sqlglot.optimizer.pushdown_predicates import nodes_for_predicate, replace_aliases
from sqlglot.optimizer.simplify import simplify
from sqlglot.optimizer.scope import find_all_in_scope
from sqlglot.optimizer.scope import build_scope

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def contains_real_aggregate(expression) -> bool:
    """
    Check if the expression contains a real aggregate function (e.g. SUM, AVG),
    as opposed to MAX(a, b) which is a form of the LEAST/GREATEST function. This
    is created by PyDough to account for such an edge case when pushing down
    predicates.
    """
    for agg_expr in find_all_in_scope(expression, exp.AggFunc, bfs=True):
        if (
            isinstance(agg_expr, (exp.Max, exp.Min))
            and len(agg_expr.args["expressions"]) > 0
        ):
            continue
        return True
    return False


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


def pushdown(condition, sources, scope_ref_count, dialect, join_index=None):
    if not condition:
        return

    condition = condition.replace(simplify(condition, dialect=dialect))
    cnf_like = normalized(condition) or not normalized(condition, dnf=True)

    predicates = list(
        condition.flatten()
        if isinstance(condition, exp.And if cnf_like else exp.Or)
        else [condition]
    )

    if cnf_like:
        pushdown_cnf(predicates, sources, scope_ref_count, join_index=join_index)
    else:
        pushdown_dnf(predicates, sources, scope_ref_count)


def pushdown_cnf(predicates, sources, scope_ref_count, join_index=None):
    """
    If the predicates are in CNF like form, we can simply replace each block in the parent.
    """
    join_index = join_index or {}
    for predicate in predicates:
        for node in nodes_for_predicate(predicate, sources, scope_ref_count).values():
            if isinstance(node, exp.Join):
                name = node.alias_or_name
                predicate_tables = exp.column_table_names(predicate, name)

                # Don't push the predicate if it references tables that appear in later joins
                this_index = join_index[name]
                if all(
                    join_index.get(table, -1) < this_index for table in predicate_tables
                ):
                    predicate.replace(exp.true())
                    node.on(predicate, copy=False)
                    break
            if isinstance(node, exp.Select):
                predicate.replace(exp.true())
                inner_predicate = replace_aliases(node, predicate)
                # PyDough Change: stop using `find_in_scope(inner_predicate, exp.AggFunc)`
                # since this will fail if the predicate is MIN/MAX with 2+ args.
                if contains_real_aggregate(inner_predicate):
                    node.having(inner_predicate, copy=False)
                else:
                    node.where(inner_predicate, copy=False)


def pushdown_dnf(predicates, sources, scope_ref_count):
    """
    If the predicates are in DNF form, we can only push down conditions that are in all blocks.
    Additionally, we can't remove predicates from their original form.
    """
    # find all the tables that can be pushdown too
    # these are tables that are referenced in all blocks of a DNF
    # (a.x AND b.x) OR (a.y AND c.y)
    # only table a can be push down
    pushdown_tables = set()

    for a in predicates:
        a_tables = exp.column_table_names(a)

        for b in predicates:
            a_tables &= exp.column_table_names(b)

        pushdown_tables.update(a_tables)

    conditions = {}

    # pushdown all predicates to their respective nodes
    for table in sorted(pushdown_tables):
        for predicate in predicates:
            nodes = nodes_for_predicate(predicate, sources, scope_ref_count)

            if table not in nodes:
                continue

            conditions[table] = (
                exp.or_(conditions[table], predicate)
                if table in conditions
                else predicate
            )

        for name, node in nodes.items():
            if name not in conditions:
                continue

            predicate = conditions[name]

            if isinstance(node, exp.Join):
                node.on(predicate, copy=False)
            elif isinstance(node, exp.Select):
                inner_predicate = replace_aliases(node, predicate)
                # PyDough Change: stop using `find_in_scope(inner_predicate, exp.AggFunc)`
                # since this will fail if the predicate is MIN/MAX with 2+ args.
                if contains_real_aggregate(inner_predicate):
                    node.having(inner_predicate, copy=False)
                else:
                    node.where(inner_predicate, copy=False)
