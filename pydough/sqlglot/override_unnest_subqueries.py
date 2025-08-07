"""
Overridden version of the unnest_subqueries.py file from sqlglot.
"""

from sqlglot import exp
from sqlglot.helper import name_sequence
from sqlglot.optimizer.scope import ScopeType, traverse_scope
from sqlglot.optimizer.unnest_subqueries import unnest, _replace, _other_operand

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def unnest_subqueries(expression):
    """
    Rewrite sqlglot AST to convert some predicates with subqueries into joins.

    Convert scalar subqueries into cross joins.
    Convert correlated or vectorized subqueries into a group by so it is not a many to many left join.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT * FROM x AS x WHERE (SELECT y.a AS a FROM y AS y WHERE x.a = y.a) = 1 ")
        >>> unnest_subqueries(expression).sql()
        'SELECT * FROM x AS x LEFT JOIN (SELECT y.a AS a FROM y AS y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE _u_0.a = 1'

    Args:
        expression (sqlglot.Expression): expression to unnest
    Returns:
        sqlglot.Expression: unnested expression
    """
    next_alias_name = name_sequence("_u_")

    for scope in traverse_scope(expression):
        select = scope.expression
        parent = select.parent_select
        if not parent:
            continue
        if scope.external_columns:
            decorrelate(select, parent, scope.external_columns, next_alias_name)
        elif scope.scope_type == ScopeType.SUBQUERY:
            unnest(select, parent, next_alias_name)

    return expression


def decorrelate(select, parent_select, external_columns, next_alias_name):
    where = select.args.get("where")

    if not where or where.find(exp.Or) or select.find(exp.Limit, exp.Offset):
        return

    table_alias = next_alias_name()
    keys = []

    # for all external columns in the where statement, find the relevant predicate
    # keys to convert it into a join
    for column in external_columns:
        if column.find_ancestor(exp.Where) is not where:
            return

        predicate = column.find_ancestor(exp.Predicate)

        if not predicate or predicate.find_ancestor(exp.Where) is not where:
            return

        if isinstance(predicate, exp.Binary):
            key = (
                predicate.right
                if any(node is column for node in predicate.left.walk())
                else predicate.left
            )
        else:
            return

        keys.append((key, column, predicate))

    if not any(isinstance(predicate, exp.EQ) for *_, predicate in keys):
        return

    is_subquery_projection = any(
        node is select.parent
        for node in map(lambda s: s.unalias(), parent_select.selects)
        if isinstance(node, exp.Subquery)
    )

    value = select.selects[0]
    key_aliases = {}
    group_by = []

    for key, _, predicate in keys:
        # if we filter on the value of the subquery, it needs to be unique
        if key == value.this:
            key_aliases[key] = value.alias
            group_by.append(key)
        else:
            if key not in key_aliases:
                key_aliases[key] = next_alias_name()
            # all predicates that are equalities must also be in the unique
            # so that we don't do a many to many join
            if isinstance(predicate, exp.EQ) and key not in group_by:
                group_by.append(key)

    parent_predicate = select.find_ancestor(exp.Predicate)

    # PyDough Change: abandon de-correlation in this scenario since this
    # solution is not usable in most dialects.
    if (not is_subquery_projection) and any(
        expr not in group_by for expr in key_aliases
    ):
        return

    # if the value of the subquery is not an agg or a key, we need to collect it into an array
    # so that it can be grouped. For subquery projections, we use a MAX aggregation instead.
    agg_func = exp.Max if is_subquery_projection else exp.ArrayAgg
    if not value.find(exp.AggFunc) and value.this not in group_by:
        select.select(
            exp.alias_(agg_func(this=value.this), value.alias, quoted=False),
            append=False,
            copy=False,
        )

    # exists queries should not have any selects as it only checks if there are any rows
    # all selects will be added by the optimizer and only used for join keys
    if isinstance(parent_predicate, exp.Exists):
        select.args["expressions"] = []

    for key, alias in key_aliases.items():
        if key in group_by:
            # add all keys to the projections of the subquery
            # so that we can use it as a join key
            if isinstance(parent_predicate, exp.Exists) or key != value.this:
                select.select(f"{key} AS {alias}", copy=False)
        else:
            select.select(
                exp.alias_(agg_func(this=key.copy()), alias, quoted=False), copy=False
            )

    alias = exp.column(value.alias, table_alias)
    other = _other_operand(parent_predicate)
    op_type = type(parent_predicate.parent) if parent_predicate else None

    if isinstance(parent_predicate, exp.Exists):
        alias = exp.column(list(key_aliases.values())[0], table_alias)
        parent_predicate = _replace(parent_predicate, f"NOT {alias} IS NULL")
    elif isinstance(parent_predicate, exp.All):
        assert issubclass(op_type, exp.Binary)
        predicate = op_type(this=other, expression=exp.column("_x"))
        parent_predicate = _replace(
            parent_predicate.parent, f"ARRAY_ALL({alias}, _x -> {predicate})"
        )
    elif isinstance(parent_predicate, exp.Any):
        assert issubclass(op_type, exp.Binary)
        if value.this in group_by:
            predicate = op_type(this=other, expression=alias)
            parent_predicate = _replace(parent_predicate.parent, predicate)
        else:
            predicate = op_type(this=other, expression=exp.column("_x"))
            parent_predicate = _replace(
                parent_predicate, f"ARRAY_ANY({alias}, _x -> {predicate})"
            )
    elif isinstance(parent_predicate, exp.In):
        if value.this in group_by:
            parent_predicate = _replace(parent_predicate, f"{other} = {alias}")
        else:
            parent_predicate = _replace(
                parent_predicate,
                f"ARRAY_ANY({alias}, _x -> _x = {parent_predicate.this})",
            )
    else:
        if is_subquery_projection and select.parent.alias:
            alias = exp.alias_(alias, select.parent.alias)

        # COUNT always returns 0 on empty datasets, so we need take that into consideration here
        # by transforming all counts into 0 and using that as the coalesced value
        if value.find(exp.Count):

            def remove_aggs(node):
                if isinstance(node, exp.Count):
                    return exp.Literal.number(0)
                elif isinstance(node, exp.AggFunc):
                    return exp.null()
                return node

            alias = exp.Coalesce(
                this=alias, expressions=[value.this.transform(remove_aggs)]
            )

        select.parent.replace(alias)

    for key, column, predicate in keys:
        predicate.replace(exp.true())
        nested = exp.column(key_aliases[key], table_alias)

        if is_subquery_projection:
            key.replace(nested)
            if not isinstance(predicate, exp.EQ):
                parent_select.where(predicate, copy=False)
            continue

        if key in group_by:
            key.replace(nested)
        elif isinstance(predicate, exp.EQ):
            parent_predicate = _replace(
                parent_predicate,
                f"({parent_predicate} AND ARRAY_CONTAINS({nested}, {column}))",
            )
        else:
            key.replace(exp.to_identifier("_x"))
            parent_predicate = _replace(
                parent_predicate,
                f"({parent_predicate} AND ARRAY_ANY({nested}, _x -> {predicate}))",
            )

    parent_select.join(
        select.group_by(*group_by, copy=False),
        on=[predicate for *_, predicate in keys if isinstance(predicate, exp.EQ)],
        join_type="LEFT",
        join_alias=table_alias,
        copy=False,
    )
