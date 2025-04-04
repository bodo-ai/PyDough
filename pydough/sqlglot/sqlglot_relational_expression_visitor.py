"""
Handle the conversion from the Relation Expressions inside
the relation Tree to a single SQLGlot query component.
"""

import datetime
import warnings

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier
from sqlglot.expressions import Star as SQLGlotStar

from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseDialect
from pydough.relational import (
    CallExpression,
    ColumnReference,
    CorrelatedReference,
    LiteralExpression,
    RelationalExpression,
    RelationalExpressionVisitor,
    WindowCallExpression,
)
from pydough.types import PyDoughType

from .sqlglot_helpers import set_glot_alias
from .transform_bindings import BaseTransformBindings, bindings_from_dialect

__all__ = ["SQLGlotRelationalExpressionVisitor"]


class SQLGlotRelationalExpressionVisitor(RelationalExpressionVisitor):
    """
    The visitor pattern for creating SQLGlot expressions from
    the relational tree 1 node at a time.
    """

    def __init__(
        self,
        dialect: DatabaseDialect,
        correlated_names: dict[str, str],
        config: PyDoughConfigs,
    ) -> None:
        # Keep a stack of SQLGlot expressions so we can build up
        # intermediate results.
        self._stack: list[SQLGlotExpression] = []
        self._dialect: DatabaseDialect = dialect
        self._correlated_names: dict[str, str] = correlated_names
        self._config: PyDoughConfigs = config
        self._bindings: BaseTransformBindings = bindings_from_dialect(dialect, config)

    def reset(self) -> None:
        """
        Reset just clears our stack.
        """
        self._stack = []

    def visit_call_expression(self, call_expression: CallExpression) -> None:
        # Visit the inputs in reverse order so we can pop them off in order.
        for arg in reversed(call_expression.inputs):
            arg.accept(self)
        input_exprs: list[SQLGlotExpression] = [
            self._stack.pop() for _ in range(len(call_expression.inputs))
        ]
        input_types: list[PyDoughType] = [
            arg.data_type for arg in call_expression.inputs
        ]
        output_expr: SQLGlotExpression = self._bindings.convert_call_to_sqlglot(
            call_expression.op, input_exprs, input_types
        )
        self._stack.append(output_expr)

    def visit_window_expression(self, window_expression: WindowCallExpression) -> None:
        # Visit the inputs in reverse order so we can pop them off in order.
        for arg in reversed(window_expression.inputs):
            arg.accept(self)
        arg_exprs: list[SQLGlotExpression] = [
            self._stack.pop() for _ in range(len(window_expression.inputs))
        ]
        # Do the same with the partition expressions.
        for arg in reversed(window_expression.partition_inputs):
            arg.accept(self)
        partition_exprs: list[SQLGlotExpression] = [
            self._stack.pop() for _ in range(len(window_expression.partition_inputs))
        ]
        # Do the same with the order
        order_exprs: list[SQLGlotExpression] = []
        for order_arg in window_expression.order_inputs:
            order_arg.expr.accept(self)
            glot_expr: SQLGlotExpression = self._stack.pop()
            # Ignore non-default na first/last positions for SQLite dialect
            na_first: bool
            if self._dialect == DatabaseDialect.SQLITE:
                if order_arg.ascending:
                    if not order_arg.nulls_first:
                        warnings.warn(
                            "PyDough when using SQLITE dialect does not support ascending ordering with nulls last (changed to nulls first)"
                        )
                    na_first = True
                else:
                    if order_arg.nulls_first:
                        warnings.warn(
                            "PyDough when using SQLITE dialect does not support ascending ordering with nulls first (changed to nulls last)"
                        )
                    na_first = False
            else:
                na_first = order_arg.nulls_first
            if order_arg.ascending:
                glot_expr = glot_expr.asc(nulls_first=na_first)
            else:
                glot_expr = glot_expr.desc(nulls_first=na_first)
            order_exprs.append(glot_expr)
        this: SQLGlotExpression
        match window_expression.op.function_name:
            case "PERCENTILE":
                # Extract the number of buckets to use for the percentile
                # operation (default is 100).
                n_buckets = window_expression.kwargs.get("n_buckets", 100)
                assert isinstance(n_buckets, int)
                this = sqlglot_expressions.Anonymous(
                    this="NTILE", expressions=[sqlglot_expressions.convert(n_buckets)]
                )
            case "RANKING":
                if window_expression.kwargs.get("allow_ties", False):
                    if window_expression.kwargs.get("dense", False):
                        this = sqlglot_expressions.Anonymous(this="DENSE_RANK")
                    else:
                        this = sqlglot_expressions.Anonymous(this="RANK")
                else:
                    this = sqlglot_expressions.RowNumber()
            case "PREV" | "NEXT":
                offset = window_expression.kwargs.get("n", 1)
                if not isinstance(offset, int):
                    raise ValueError(
                        f"Invalid 'n' argument to {window_expression.op.function_name}: {offset!r} (expected an integer)"
                    )
                # By default, we use the LAG function. If doing NEXT, switch
                # to LEAD. If the offset is negative, switch again.
                func, other_func = sqlglot_expressions.Lag, sqlglot_expressions.Lead
                if window_expression.op.function_name == "NEXT":
                    func, other_func = other_func, func
                if offset < 0:
                    offset *= -1
                    func, other_func = other_func, func
                lag_args: dict[str, SQLGlotExpression] = {}
                lag_args["this"] = arg_exprs[0]
                lag_args["offset"] = sqlglot_expressions.convert(offset)
                if "default" in window_expression.kwargs:
                    lag_args["default"] = sqlglot_expressions.convert(
                        window_expression.kwargs.get("default")
                    )
                this = func(**lag_args)
            case "RELSUM":
                this = sqlglot_expressions.Sum.from_arg_list(arg_exprs)
            case "RELAVG":
                this = sqlglot_expressions.Avg.from_arg_list(arg_exprs)
            case "RELCOUNT":
                this = sqlglot_expressions.Count.from_arg_list(arg_exprs)
            case "RELSIZE":
                this = sqlglot_expressions.Count.from_arg_list([SQLGlotStar()])
            case _:
                raise NotImplementedError(
                    f"Window operator {window_expression.op.function_name} not supported"
                )
        window_args: dict[str, object] = {"this": this}
        if partition_exprs:
            window_args["partition_by"] = partition_exprs
        if order_exprs:
            window_args["order"] = sqlglot_expressions.Order(
                this=None, expressions=order_exprs
            )
        self._stack.append(sqlglot_expressions.Window(**window_args))

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        # Note: This assumes each literal has an associated type that can be parsed
        # and types do not represent implicit casts.
        literal: SQLGlotExpression = sqlglot_expressions.convert(
            literal_expression.value
        )

        # Special handling: insert cast calls for ansi casting of date/time
        # instead of relying on SQLGlot conversion functions. This is because
        # the default handling in SQLGlot without a dialect is to produce a
        # nonsensical TIME_STR_TO_TIME or DATE_STR_TO_DATE function which each
        # specific dialect is responsible for translating into its own logic.
        # Rather than have that logic show up in the ANSI sql text, we will
        # instead create the CAST calls ourselves.
        if self._dialect == DatabaseDialect.ANSI:
            if isinstance(literal_expression.value, datetime.date):
                date: datetime.date = literal_expression.value
                literal = sqlglot_expressions.Cast(
                    this=sqlglot_expressions.convert(date.strftime("%Y-%m-%d")),
                    to=sqlglot_expressions.DataType.build("DATE"),
                )
            if isinstance(literal_expression.value, datetime.datetime):
                dt: datetime.datetime = literal_expression.value
                if dt.tzinfo is not None:
                    raise ValueError(
                        "PyDough does not yet support datetime values with a timezone"
                    )
                literal = sqlglot_expressions.Cast(
                    this=sqlglot_expressions.convert(dt.isoformat(sep=" ")),
                    to=sqlglot_expressions.DataType.build("TIMESTAMP"),
                )
        self._stack.append(literal)

    def visit_correlated_reference(
        self, correlated_reference: CorrelatedReference
    ) -> None:
        full_name: str = f"{self._correlated_names[correlated_reference.correl_name]}.{correlated_reference.name}"
        self._stack.append(Identifier(this=full_name, quoted=False))

    @staticmethod
    def make_sqlglot_column(
        column_reference: ColumnReference,
    ) -> Identifier:
        """
        Generate an identifier for a column reference. This is split into a
        separate static method to ensure consistency across multiple visitors.
        Args:
            column_reference (ColumnReference): The column reference to generate
                an identifier for.
        Returns:
            Identifier: The output identifier.
        """
        if column_reference.input_name is not None:
            full_name = f"{column_reference.input_name}.{column_reference.name}"
        else:
            full_name = column_reference.name
        return Identifier(this=full_name, quoted=False)

    def visit_column_reference(self, column_reference: ColumnReference) -> None:
        self._stack.append(self.make_sqlglot_column(column_reference))

    def relational_to_sqlglot(
        self, expr: RelationalExpression, output_name: str | None = None
    ) -> SQLGlotExpression:
        """
        Interface to convert an entire relational expression to a SQLGlot expression
        and assign it the given alias.

        Args:
            expr (RelationalExpression): The relational expression to convert.
            output_name (str | None): The name to assign to the final SQLGlot expression
                or None if we should omit any alias.

        Returns:
            SQLGlotExpression: The final SQLGlot expression representing the entire
                relational tree.
        """
        self.reset()
        expr.accept(self)
        result = self.get_sqlglot_result()
        return set_glot_alias(result, output_name)

    def get_sqlglot_result(self) -> SQLGlotExpression:
        """
        Interface to get the current SQLGlot expression result based on the current state.

        Returns:
            SQLGlotExpression: The SQLGlot expression representing the tree we have already
                visited.
        """
        assert len(self._stack) == 1, "Expected exactly one expression on the stack"
        return self._stack[0]
