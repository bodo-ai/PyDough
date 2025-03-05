"""
Handle the conversion from the Relation Tree to a single
SQLGlot query.
"""

import warnings
from collections import defaultdict
from collections.abc import MutableMapping, MutableSequence

from sqlglot.dialects import Dialect as SQLGlotDialect
from sqlglot.expressions import Alias as SQLGlotAlias
from sqlglot.expressions import Column as SQLGlotColumn
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier as SQLGlotIdentifier
from sqlglot.expressions import Literal as SQLGlotLiteral
from sqlglot.expressions import Select, Subquery, values
from sqlglot.expressions import Star as SQLGlotStar
from sqlglot.expressions import TableAlias as SQLGlotTableAlias

from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    ColumnReferenceInputNameModifier,
    ColumnReferenceInputNameRemover,
    CorrelatedReference,
    EmptySingleton,
    ExpressionSortInfo,
    Filter,
    Join,
    Limit,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalRoot,
    RelationalVisitor,
    Scan,
    WindowCallExpression,
)

from .sqlglot_relational_expression_visitor import SQLGlotRelationalExpressionVisitor
from .transform_bindings import SqlGlotTransformBindings

__all__ = ["SQLGlotRelationalVisitor"]


class SQLGlotRelationalVisitor(RelationalVisitor):
    """
    The visitor pattern for creating SQLGlot expressions from
    the relational tree 1 node at a time.
    """

    def __init__(
        self, dialect: SQLGlotDialect, bindings: SqlGlotTransformBindings
    ) -> None:
        # Keep a stack of SQLGlot expressions so we can build up
        # intermediate results.
        self._stack: list[Select] = []
        self._correlated_names: dict[str, str] = {}
        self._expr_visitor: SQLGlotRelationalExpressionVisitor = (
            SQLGlotRelationalExpressionVisitor(
                dialect, bindings, self._correlated_names
            )
        )
        self._alias_modifier: ColumnReferenceInputNameModifier = (
            ColumnReferenceInputNameModifier()
        )
        self._alias_remover: ColumnReferenceInputNameRemover = (
            ColumnReferenceInputNameRemover()
        )
        # Counter for generating unique table alias.
        self._alias_counter: int = 0

    def _generate_table_alias(self) -> str:
        """
        Generate a unique table alias for use in the SQLGlot query. This
        is used to allow operators to have standard names but not reuse the
        same table alias in the same query.

        Returns:
            str: A unique table alias.
        """
        alias = f"_table_alias_{self._alias_counter}"
        self._alias_counter += 1
        return alias

    @staticmethod
    def _is_mergeable_column(expr: SQLGlotExpression) -> bool:
        """
        Determine if a given SQLGlot expression is a candidate
        for merging with other columns.

        Args:
            expr (SQLGlotExpression): The expression to check.

        Returns:
            bool: Can we potentially merge this column.
        """
        if isinstance(expr, SQLGlotAlias):
            return SQLGlotRelationalVisitor._is_mergeable_column(expr.this)
        else:
            return isinstance(expr, (SQLGlotLiteral, SQLGlotIdentifier, SQLGlotColumn))

    def _convert_ordering(
        self, ordering: MutableSequence[ExpressionSortInfo], input_alias: str
    ) -> list[SQLGlotExpression]:
        """
        Convert the orderings from the a relational operator into a variant
        that can be used in SQLGlot.

        Args:
            ordering (MutableSequence[ExpressionSortInfo]): The orderings to convert.

        Returns:
            list[SQLGlotExpression]: The converted orderings.
        """
        glot_exprs: list[SQLGlotExpression] = []
        for col in ordering:
            glot_expr: SQLGlotExpression = self._expr_visitor.relational_to_sqlglot(
                col.expr, input_alias
            )
            # Ignore non-default na first/last positions for SQLite dialect
            na_first: bool
            if self._expr_visitor._dialect.__class__.__name__ == "SQLite":
                if col.ascending:
                    if not col.nulls_first:
                        warnings.warn(
                            "PyDough when using SQLITE dialect does not support ascending ordering with nulls last (changed to nulls first)"
                        )
                    na_first = True
                else:
                    if col.nulls_first:
                        warnings.warn(
                            "PyDough when using SQLITE dialect does not support ascending ordering with nulls first (changed to nulls last)"
                        )
                    na_first = False
            else:
                na_first = col.nulls_first
            if col.ascending:
                glot_expr = glot_expr.asc(nulls_first=na_first)
            else:
                glot_expr = glot_expr.desc(nulls_first=na_first)
            glot_exprs.append(glot_expr)
        return glot_exprs

    @staticmethod
    def _is_mergeable_ordering(
        ordering: list[SQLGlotExpression], input_expr: Select
    ) -> bool:
        """
        Determine if the given ordering can be merged with the input
        expression. This occurs when the orderings are identical or
        either side doesn't contain any ordering.

        Args:
            ordering (list[SQLGlotExpression]): The new ordering, possibly
                an empty list.
            input_expr (Select): The old ordering.

        Returns:
            bool: Can the orderings be merged together.
        """
        if "order" not in input_expr.args or not ordering:
            return True
        else:
            return ordering == input_expr.args["order"].expressions

    def _build_subquery(
        self,
        input_expr: Select,
        column_exprs: list[SQLGlotExpression],
        alias: str | None = None,
        sort: bool = True,
    ) -> Select:
        """
        Generate a subquery select statement with the given
        input from and the given columns.

        Args:
            `input_expr`: The from input, which should be
                another select statement.
            column_exprs (list[SQLGlotExpression]): The columns to select.
            alias (str | None): The alias to give the subquery.
            sort (bool): If True, the final select statement ordering is based on the
                sorted string representation of input column expressions.

        Returns:
            Select: A select statement representing the subquery.
        """
        if alias is None:
            alias = self._generate_table_alias()
        if sort:
            column_exprs = sorted(column_exprs, key=repr)
        return (
            Select()
            .select(*column_exprs)
            .from_(Subquery(this=input_expr, alias=SQLGlotTableAlias(this=alias)))
        )

    def contains_window(self, exp: RelationalExpression) -> bool:
        """
        Returns whether a relational expression contains a window call.
        """
        match exp:
            case CallExpression():
                return any(self.contains_window(arg) for arg in exp.inputs)
            case ColumnReference() | LiteralExpression() | CorrelatedReference():
                return False
            case WindowCallExpression():
                return True
            case _:
                raise NotImplementedError(f"{exp.__class__.__name__}")

    def reset(self) -> None:
        """
        Reset returns or resets all of the state associated with this
        visitor, which is currently the stack, expression visitor, and
        alias generator.
        """
        self._stack = []
        self._expr_visitor.reset()
        self._alias_counter = 0

    def visit_scan(self, scan: Scan) -> None:
        exprs: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, scan.table_name, alias)
            for alias, col in sorted(scan.columns.items())
        ]
        query: Select = Select().select(*exprs).from_(scan.table_name)
        query = Subquery(
            this=query, alias=SQLGlotTableAlias(this=self._generate_table_alias())
        )
        self._stack.append(query)

    def visit_join(self, join: Join) -> None:
        alias_map: dict[str | None, str] = {}
        if join.correl_name is not None:
            input_name = join.default_input_aliases[0]
            alias = self._generate_table_alias()
            alias_map[input_name] = alias
            self._correlated_names[join.correl_name] = alias
        self.visit_inputs(join)
        inputs: list[Select] = [self._stack.pop() for _ in range(len(join.inputs))]
        inputs.reverse()
        # Compute a dictionary to find all duplicate names.
        seen_names: MutableMapping[str, int] = defaultdict(int)
        for input in join.inputs:
            for column in input.columns.keys():
                seen_names[column] += 1
        # Only keep duplicate names.
        kept_names = {key for key, value in seen_names.items() if value > 1}
        alias_map = {
            join.default_input_aliases[i]: SQLGlotTableAlias(
                this=self._generate_table_alias()
            )
            for i in range(len(join.inputs))
            if kept_names.intersection(join.inputs[i].columns.keys())
        }
        self._alias_remover.set_kept_names(kept_names)
        self._alias_modifier.set_map(alias_map)
        columns = {
            alias: col.accept_shuttle(self._alias_remover).accept_shuttle(
                self._alias_modifier
            )
            for alias, col in join.columns.items()
        }
        column_exprs = [
            self._expr_visitor.relational_to_sqlglot(col, None, alias)
            for alias, col in columns.items()
        ]
        query: Select = self._build_subquery(
            inputs[0], column_exprs, alias_map.get(join.default_input_aliases[0], None)
        )
        joins: list[tuple[Subquery, SQLGlotExpression, str]] = []
        for i in range(1, len(inputs)):
            subquery: Subquery = Subquery(
                this=inputs[i], alias=alias_map.get(join.default_input_aliases[i], None)
            )
            cond: RelationalExpression = (
                join.conditions[i - 1]
                .accept_shuttle(self._alias_remover)
                .accept_shuttle(self._alias_modifier)
            )
            cond_expr: SQLGlotExpression = self._expr_visitor.relational_to_sqlglot(
                cond, None
            )
            join_type: str = join.join_types[i - 1].value
            joins.append((subquery, cond_expr, join_type))
        for subquery, cond_expr, join_type in joins:
            query = query.join(subquery, on=cond_expr, join_type=join_type)
        self._stack.append(query)

    def visit_project(self, project: Project) -> None:
        self.visit_inputs(project)
        input_expr: Select = self._stack.pop()
        in_alias = self._generate_table_alias()

        exprs: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, in_alias, alias)
            for alias, col in project.columns.items()
        ]
        query: Select = self._build_subquery(input_expr, exprs, in_alias)
        self._stack.append(query)

    def visit_filter(self, filter: Filter) -> None:
        self.visit_inputs(filter)
        input_expr: Select = self._stack.pop()
        in_alias = self._generate_table_alias()
        exprs: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, in_alias, alias)
            for alias, col in filter.columns.items()
        ]
        query: Select
        if self.contains_window(filter.condition):
            # If there is a window function in the condition, use QUALIFY
            # instead of WHERE.
            cond = self._expr_visitor.relational_to_sqlglot(
                filter.condition, input_expr.alias
            )
            query = self._build_subquery(input_expr, [SQLGlotStar()], in_alias)
            query = query.qualify(cond)
            # Apply `_build_subquery` with `exprs` after qualification in case
            # the SELECT clause would remove any of the columns used by the
            # QUALIFY.
            query = self._build_subquery(query, exprs, in_alias)
        else:
            cond = self._expr_visitor.relational_to_sqlglot(filter.condition, in_alias)
            # TODO: (gh #151) Consider allowing combining where if
            # limit isn't present?
            query = self._build_subquery(input_expr, exprs, in_alias)
            query = query.where(cond)
        self._stack.append(query)

    def visit_aggregate(self, aggregate: Aggregate) -> None:
        self.visit_inputs(aggregate)
        input_expr: Select = self._stack.pop()
        in_alias = self._generate_table_alias()
        keys: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, in_alias, alias)
            for alias, col in aggregate.keys.items()
        ]
        aggregations: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, in_alias, alias)
            for alias, col in aggregate.aggregations.items()
        ]
        select_cols = keys + aggregations
        query: Select = self._build_subquery(input_expr, select_cols, in_alias)
        if keys:
            if aggregations:
                query = query.group_by(*keys)
            else:
                query = query.distinct()
        self._stack.append(query)

    def visit_limit(self, limit: Limit) -> None:
        self.visit_inputs(limit)
        input_expr: Select = self._stack.pop()
        in_alias = self._generate_table_alias()
        assert isinstance(
            limit.limit, LiteralExpression
        ), "Limit currently only supports literals"
        limit_expr: SQLGlotExpression = self._expr_visitor.relational_to_sqlglot(
            limit.limit, in_alias
        )
        exprs: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, in_alias, alias)
            for alias, col in limit.columns.items()
        ]
        ordering_exprs: list[SQLGlotExpression] = self._convert_ordering(
            limit.orderings, in_alias
        )
        query: Select = self._build_subquery(input_expr, exprs, in_alias)
        if ordering_exprs:
            query = query.order_by(*ordering_exprs)
        query = query.limit(limit_expr)
        self._stack.append(query)

    def visit_empty_singleton(self, singleton: EmptySingleton) -> None:
        query: Select = Select().from_(values([()]))
        query = Subquery(
            this=query, alias=SQLGlotTableAlias(self._generate_table_alias())
        )
        self._stack.append(query)

    def visit_root(self, root: RelationalRoot) -> None:
        self.visit_inputs(root)
        input_expr: Select = self._stack.pop()
        in_alias = self._generate_table_alias()
        # Pop the expressions in order.
        exprs: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, in_alias, alias)
            for alias, col in root.ordered_columns
        ]
        ordering_exprs: list[SQLGlotExpression] = self._convert_ordering(
            root.orderings, in_alias
        )
        query: Select = self._build_subquery(input_expr, exprs, in_alias)
        if ordering_exprs:
            query = query.order_by(*ordering_exprs)
        self._stack.append(query)

    def relational_to_sqlglot(self, root: RelationalRoot) -> SQLGlotExpression:
        """
        Interface to convert an entire relational tree to a SQLGlot expression.

        Args:
            root (RelationalRoot): The root of the relational tree.

        Returns:
            SQLGlotExpression: The final SQLGlot expression representing the entire
                relational tree.
        """
        self.reset()
        root.accept(self)
        return self.get_sqlglot_result()

    def get_sqlglot_result(self) -> SQLGlotExpression:
        """
        Interface to get the current SQLGlot expression result based on the current state.
        This is used so we can convert individual nodes to SQLGlot expressions without
        having to convert the entire tree at once and is mostly used for testing.

        Returns:
            SQLGlotExpression: The SQLGlot expression representing the tree we have already
                visited.
        """
        assert (
            len(self._stack) == 1
        ), "Expected exactly one SQLGlot expression on the stack"
        return self._stack[0]
