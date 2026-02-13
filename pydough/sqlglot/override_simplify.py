"""
Overridden version of the simplify.py file from sqlglot.
"""

from __future__ import annotations

import datetime
import typing as t

from sqlglot import Dialect, exp
from sqlglot.helper import merge_ranges, while_changing
from sqlglot.optimizer.simplify import (
    DATETRUNC_BINARY_COMPARISONS,
    DATETRUNC_COMPARISONS,
    DATETRUNCS,
    FINAL,
    UnsupportedUnit,
    _datetrunc_eq_expression,
    _datetrunc_range,
    _is_datetrunc_predicate,
    absorb_and_eliminate,
    catch,
    connector_depth,
    date_floor,
    date_literal,
    extract_date,
    extract_type,
    flatten,
    is_null,
    logger,
    propagate_constants,
    remove_complements,
    remove_where_true,
    rewrite_between,
    simplify_coalesce,
    simplify_concat,
    simplify_conditionals,
    simplify_connectors,
    simplify_equality,
    simplify_literals,
    simplify_not,
    simplify_parens,
    simplify_startswith,
    sort_comparison,
    uniq_sort,
)

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    DateTruncBinaryTransform = t.Callable[
        [exp.Expression, datetime.date, str, Dialect, exp.DataType],
        exp.Expression | None,
    ]


def simplify(
    expression: exp.Expression,
    constant_propagation: bool = False,
    dialect: DialectType = None,
    max_depth: int | None = None,
):
    """
    Rewrite sqlglot AST to simplify expressions.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("TRUE AND TRUE")
        >>> simplify(expression).sql()
        'TRUE'

    Args:
        expression: expression to simplify
        constant_propagation: whether the constant propagation rule should be used
        max_depth: Chains of Connectors (AND, OR, etc) exceeding `max_depth` will be skipped
    Returns:
        sqlglot.Expression: simplified expression
    """

    dialect = Dialect.get_or_raise(dialect)

    def _simplify(expression, root=True):
        if (
            max_depth
            and isinstance(expression, exp.Connector)
            and not isinstance(expression.parent, exp.Connector)
        ):
            depth = connector_depth(expression)
            if depth > max_depth:
                logger.info(
                    f"Skipping simplification because connector depth {depth} exceeds max {max_depth}"
                )
                return expression

        if expression.meta.get(FINAL):
            return expression

        # group by expressions cannot be simplified, for example
        # select x + 1 + 1 FROM y GROUP BY x + 1 + 1
        # the projection must exactly match the group by key
        group = expression.args.get("group")

        if group and hasattr(expression, "selects"):
            groups = set(group.expressions)
            group.meta[FINAL] = True

            for e in expression.selects:
                for node in e.walk():
                    if node in groups:
                        e.meta[FINAL] = True
                        break

            having = expression.args.get("having")
            if having:
                for node in having.walk():
                    if node in groups:
                        having.meta[FINAL] = True
                        break

        # Pre-order transformations
        node = expression
        node = rewrite_between(node)
        node = uniq_sort(node, root)
        node = absorb_and_eliminate(node, root)
        node = simplify_concat(node)
        node = simplify_conditionals(node)

        # PyDough Change: new pre-order transformations
        node = rewrite_case_to_nullif(node)
        node = rewrite_coalesce_nullif(node)
        node = rewrite_sum_nullif(node)
        node = rewrite_coalesce_count(node)

        if constant_propagation:
            node = propagate_constants(node, root)

        exp.replace_children(node, lambda e: _simplify(e, False))

        # Post-order transformations
        node = simplify_not(node)
        node = flatten(node)
        node = simplify_connectors(node, root)
        node = remove_complements(node, root)
        node = simplify_coalesce(node)
        node.parent = expression.parent
        node = simplify_literals(node, root)
        node = simplify_equality(node)
        node = simplify_parens(node)
        node = simplify_datetrunc(node, dialect)
        node = sort_comparison(node)
        node = simplify_startswith(node)

        # PyDough Change: new post-order transformations
        node = rewrite_nullif_coalesce(node)

        if root:
            expression.replace(node)
        return node

    expression = while_changing(expression, _simplify)
    remove_where_true(expression)
    return expression


@catch(ModuleNotFoundError, UnsupportedUnit)
def simplify_datetrunc(expression: exp.Expression, dialect: Dialect) -> exp.Expression:
    """Simplify expressions like `DATE_TRUNC('year', x) >= CAST('2021-01-01' AS DATE)`"""
    comparison = expression.__class__

    if isinstance(expression, DATETRUNCS):
        this = expression.this
        trunc_type = extract_type(this)
        date = extract_date(this)
        #### Start of PyDough Change ####
        # If date is datetime.datetime, it should NOT enter the if statement
        # because `date_floor` only works correctly on datetime.date
        if date and not isinstance(date, datetime.datetime) and expression.unit:
            return date_literal(
                date_floor(date, expression.unit.name.lower(), dialect), trunc_type
            )
        #### End of PyDough Change ####
    elif comparison not in DATETRUNC_COMPARISONS:
        return expression

    if isinstance(expression, exp.Binary):
        l, r = expression.left, expression.right  # noqa: E741

        if not _is_datetrunc_predicate(l, r):
            return expression

        l = t.cast(exp.DateTrunc, l)  # noqa: E741
        trunc_arg = l.this
        unit = l.unit.name.lower()
        date = extract_date(r)

        if not date:
            return expression

        return (
            DATETRUNC_BINARY_COMPARISONS[comparison](
                trunc_arg, date, unit, dialect, extract_type(r)
            )
            or expression
        )

    if isinstance(expression, exp.In):
        l = expression.this  # noqa: E741
        rs = expression.expressions

        if rs and all(_is_datetrunc_predicate(l, r) for r in rs):
            l = t.cast(exp.DateTrunc, l)  # noqa: E741
            unit = l.unit.name.lower()

            ranges = []
            for r in rs:
                date = extract_date(r)
                if not date:
                    return expression
                drange = _datetrunc_range(date, unit, dialect)
                if drange:
                    ranges.append(drange)

            if not ranges:
                return expression

            ranges = merge_ranges(ranges)
            target_type = extract_type(*rs)

            return exp.or_(
                *[
                    _datetrunc_eq_expression(l, drange, target_type)
                    for drange in ranges
                ],
                copy=False,
            )

    return expression


def rewrite_case_to_nullif(expr: exp.Expression) -> exp.Expression:
    """
    Rewrite expressions like `CASE WHEN x != y THEN x ELSE NULL END` to
    `NULLIF(x, y)`

    Args:
        `expr`: The expression to rewrite.

    Returns:
        The rewritten expression.
    """
    if not isinstance(expr, exp.Case):
        return expr

    if (
        not (expr.args.get("this") is None and is_null(expr.args.get("default", None)))
        and len(expr.args.get("ifs", [])) == 1
    ):
        return expr

    if_expr = expr.args["ifs"][0]
    condition = if_expr.args.get("this")
    result = if_expr.args.get("true")

    if not isinstance(condition, exp.NEQ):
        return expr

    lhs = condition.args.get("this")
    rhs = condition.args.get("expression")

    if lhs == result:
        return exp.Nullif(this=lhs, expression=rhs, copy=False)

    if rhs == result:
        return exp.Nullif(this=rhs, expression=lhs, copy=False)

    return expr


def rewrite_coalesce_nullif(expr: exp.Expression) -> exp.Expression:
    """
    Rewrite expressions like `COALESCE(NULLIF(x, y), z)` to
    `CASE WHEN x = y THEN z ELSE x END`, or if `y` and `z` are the same then
    just to `COALESCE(x, z)`.

    Args:
        `expr`: The expression to rewrite.

    Returns:
        The rewritten expression.
    """
    if not isinstance(expr, exp.Coalesce):
        return expr

    if len(expr.expressions) != 1 or expr.args.get("is_nvl"):
        return expr

    first = expr.this
    second = expr.expressions[0]

    if not isinstance(first, exp.Nullif):
        return expr

    lhs: exp.Expression = first.args.get("this")
    rhs: exp.Expression = first.args.get("expression")

    if rhs == second:
        return exp.Coalesce(this=lhs, expressions=[second], copy=False)

    return exp.Case(
        whens=[
            exp.When(
                this=exp.EQ(this=lhs, expression=rhs, copy=False),
                true=second,
                copy=False,
            )
        ],
        default=lhs,
        copy=False,
    )


def rewrite_sum_nullif(expr: exp.Expression) -> exp.Expression:
    """
    Rewrite `SUM(NULLIF(x, 0))` to `SUM(x)`.

    Args:
        `expr`: The expression to rewrite.

    Returns:
        The rewritten expression.
    """
    if not isinstance(expr, exp.Sum):
        return expr

    arg = expr.this
    if not isinstance(arg, exp.Nullif):
        return expr

    lhs: exp.Expression = arg.args.get("this")
    rhs: exp.Expression = arg.args.get("expression")

    if isinstance(rhs, exp.Literal) and rhs.is_number and float(rhs.this) == 0:
        return exp.Sum(this=lhs, copy=False)

    return expr


def rewrite_coalesce_count(expr: exp.Expression) -> exp.Expression:
    """
    Rewrite `COALESCE(COUNT(x), 0)` to `COUNT(x)`, and does the same for
    `COALESCE(COUNT_IF(x), 0)`.

    Args:
        `expr`: The expression to rewrite.

    Returns:
        The rewritten expression.
    """
    if not isinstance(expr, exp.Coalesce):
        return expr

    return expr.this if isinstance(expr.this, (exp.Count, exp.CountIf)) else expr


def rewrite_nullif_coalesce(expr: exp.Expression) -> exp.Expression:
    """
    Rewrite `NULLIF(COALESCE(x, y), y)` to `NULLIF(x, y)`.

    Args:
        `expr`: The expression to rewrite.

    Returns:
        The rewritten expression.
    """
    if not isinstance(expr, exp.Nullif):
        return expr

    lhs: exp.Expression = expr.args.get("this")
    rhs: exp.Expression = expr.args.get("expression")

    if not isinstance(lhs, exp.Coalesce) or len(lhs.expressions) != 1:
        return expr

    if lhs.expressions[0] == rhs:
        return exp.Nullif(this=lhs.args.get("this"), expression=rhs, copy=False)
    else:
        return expr
