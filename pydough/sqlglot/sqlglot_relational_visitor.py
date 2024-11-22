"""
Handle the conversion from the Relation Tree to a single
SQLGlot query.
"""

from collections.abc import MutableSequence

from sqlglot.expressions import Alias as SQLGlotAlias
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier, Select, Subquery
from sqlglot.expressions import Literal as SQLGlotLiteral

from pydough.relational import (
    Aggregate,
    ColumnReferenceInputNameModifier,
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
)

from .sqlglot_helpers import get_glot_name, set_glot_alias, unwrap_alias
from .sqlglot_identifier_finder import find_identifiers, find_identifiers_in_list
from .sqlglot_relational_expression_visitor import SQLGlotRelationalExpressionVisitor

__all__ = ["SQLGlotRelationalVisitor"]


class SQLGlotRelationalVisitor(RelationalVisitor):
    """
    The visitor pattern for creating SQLGlot expressions from
    the relational tree 1 node at a time.
    """

    def __init__(self) -> None:
        # Keep a stack of SQLGlot expressions so we can build up
        # intermediate results.
        self._stack: list[Select] = []
        self._expr_visitor: SQLGlotRelationalExpressionVisitor = (
            SQLGlotRelationalExpressionVisitor()
        )
        self._alias_modifier: ColumnReferenceInputNameModifier = (
            ColumnReferenceInputNameModifier()
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
            return isinstance(expr, (SQLGlotLiteral, Identifier))

    @staticmethod
    def _try_merge_columns(
        new_columns: list[SQLGlotExpression],
        old_columns: list[SQLGlotExpression],
        old_column_deps: set[Identifier],
    ) -> tuple[list[SQLGlotExpression] | None, list[SQLGlotExpression]]:
        """
        Attempt to merge the new_columns with the old_columns whenever
        possible  to reduce the amount of SQL that needs to be generated
        for the given output columns. In addition to the columns themselves,
        the old_columns could also produce dependencies for the new expression
        since it logically occurs before the new columns, so we need to
        determine those for tracking.

        The final result is presented as a tuple of two lists, the first
        list is the new columns that need to be produced in a separate
        query and the second list is the list of columns that can be
        placed in the original query. The new columns will be None if we
        can generate the SQL entirely in the original query.

        Args:
            new_columns (list[SQLGlotExpression]): The new columns that
                need to be the output of the current Relational node.
            old_columns (list[SQLGlotExpression]): The old columns that
                were the output of the previous Relational node.
            deps (set[str]): A set of column names that are dependencies
                of the old columns in some operator other than the
                "SELECT" component. For example a filter will need to
                include the column names of any WHERE conditions.
        Returns:
            tuple[list[SQLGlotExpression] | None, list[SQLGlotExpression]]:
                The columns that should be generated in a separate new
                select statement and the columns that can be placed in
                the original query.
        """
        # Only support fusing columns that are simple renames or literals for
        # now. If we see just column references or literals though we can
        # always merge.
        # TODO: Enable merging more complex expressions for example we can
        # merge a + b if a and b are both just simple columns in the input.
        can_merge: bool = all(
            SQLGlotRelationalVisitor._is_mergeable_column(c) for c in new_columns
        )
        if can_merge:
            modified_new_columns = None
            modified_old_columns = []
            # Create a mapping for the old columns so we can replace column
            # references.
            old_column_map = {get_glot_name(c): c for c in old_columns}
            seen_cols: set[Identifier] = set()
            for new_column in new_columns:
                new_name = get_glot_name(new_column)
                new_column = unwrap_alias(new_column)
                if isinstance(new_column, SQLGlotLiteral):
                    # If the new column is a literal, we can just add it to the old
                    # columns.
                    modified_old_columns.append(set_glot_alias(new_column, new_name))
                else:
                    expr = set_glot_alias(old_column_map[new_column.this], new_name)
                    modified_old_columns.append(expr)
                    if isinstance(expr, Identifier):
                        seen_cols.add(expr)
            # Check that there are no missing dependencies in the old columns.
            if old_column_deps - seen_cols:
                return new_columns, old_columns
            return modified_new_columns, modified_old_columns
        else:
            return new_columns, old_columns

    def _merge_selects(
        self,
        new_columns: list[SQLGlotExpression],
        orig_select: Select,
        deps: set[Identifier],
    ) -> Select:
        """
        Attempt to merge a new select statement with an existing one.
        This is used to reduce the unnecessary generation of nested
        queries. Currently this only supports merging the SELECT columns.

        Args:
            new_columns (list[SQLGlotExpression]): The new columns to attempt to merge.
            orig_select (Select): The original select statement to merge with.
            deps (set[str]): A set of column names that are dependencies
                of the old columns in some operator other than the
                "SELECT" component. For example a filter will need to
                include the column names of any WHERE conditions.

        Returns:
            Select: A final select statement that may contain the merged columns.
        """
        new_exprs, old_exprs = self._try_merge_columns(
            new_columns, orig_select.expressions, deps
        )
        orig_select.set("expressions", old_exprs)
        if new_exprs is None:
            return orig_select
        else:
            return self._build_subquery(orig_select, new_exprs)

    def _convert_ordering(
        self, ordering: MutableSequence[ExpressionSortInfo]
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
                col.expr
            )
            if col.ascending:
                glot_expr = glot_expr.asc(nulls_first=col.nulls_first)
            else:
                glot_expr = glot_expr.desc(nulls_first=col.nulls_first)
            glot_exprs.append(glot_expr)
        return glot_exprs

    @staticmethod
    def _build_subquery(
        input_expr: Select,
        column_exprs: list[SQLGlotExpression],
        alias: str | None = None,
    ) -> Select:
        """
        Generate a subquery select statement with the given
        input from and the given columns.

        Args:
            input_expr (Select): The from input, which should be
                another select statement.
            column_exprs (list[SQLGlotExpression]): The columns to select.

        Returns:
            Select: A select statement representing the subquery.
        """
        return (
            Select().select(*column_exprs).from_(Subquery(this=input_expr, alias=alias))
        )

    def reset(self) -> None:
        """
        Reset returns or resets all of the state associated with this
        visitor, which is currently the stack, expression visitor, and
        alias generator.
        """
        self._stack = []
        self._expr_visitor.reset()
        self._alias_modifier.reset()
        self._alias_counter = 0

    def visit_scan(self, scan: Scan) -> None:
        exprs: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, alias)
            for alias, col in scan.columns.items()
        ]
        query: Select = Select().select(*exprs).from_(scan.table_name)
        self._stack.append(query)

    def visit_join(self, join: Join) -> None:
        alias_map = {
            key: self._generate_table_alias() for key in join.default_input_aliases
        }
        self.visit_inputs(join)
        inputs: list[Select] = [self._stack.pop() for _ in range(len(join.inputs))][
            ::-1
        ]
        self._alias_modifier.set_map(alias_map)
        columns = {
            alias: self._alias_modifier.modify_expression_names(col)
            for alias, col in join.columns.items()
        }
        column_exprs = [
            self._expr_visitor.relational_to_sqlglot(col, alias)
            for alias, col in columns.items()
        ]
        query: Select = self._build_subquery(
            inputs[0], column_exprs, alias_map[join.default_input_aliases[0]]
        )
        joins: list[tuple[Subquery, SQLGlotExpression, str]] = []
        for i in range(1, len(inputs)):
            subquery: Subquery = Subquery(
                this=inputs[i], alias=alias_map[join.default_input_aliases[i]]
            )
            cond: RelationalExpression = self._alias_modifier.modify_expression_names(
                join.conditions[i - 1]
            )
            cond_expr: SQLGlotExpression = self._expr_visitor.relational_to_sqlglot(
                cond
            )
            join_type: str = join.join_types[i - 1].value
            joins.append((subquery, cond_expr, join_type))
        for subquery, cond_expr, join_type in joins:
            query = query.join(subquery, on=cond_expr, join_type=join_type)
        self._stack.append(query)

    def visit_project(self, project: Project) -> None:
        self.visit_inputs(project)
        exprs: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, alias)
            for alias, col in project.columns.items()
        ]
        input_expr: Select = self._stack.pop()
        query: Select = self._merge_selects(exprs, input_expr, set())
        self._stack.append(query)

    def visit_filter(self, filter: Filter) -> None:
        self.visit_inputs(filter)
        input_expr: Select = self._stack.pop()
        cond = self._expr_visitor.relational_to_sqlglot(filter.condition)
        exprs: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, alias)
            for alias, col in filter.columns.items()
        ]
        query: Select
        # TODO: Refactor a simpler way to check dependent expressions.
        if (
            "group_by" in input_expr.args
            or "where" in input_expr.args
            or "order" in input_expr.args
            or "limit" in input_expr.args
        ):
            # Check if we already have a where clause or limit. We
            # cannot merge these yet.
            # TODO: Consider allowing combining where if limit isn't
            # present?
            query = self._build_subquery(input_expr, exprs)
        else:
            # Try merge the column sections
            query = self._merge_selects(exprs, input_expr, find_identifiers(cond))
        query = query.where(cond)
        self._stack.append(query)

    def visit_aggregate(self, aggregate: Aggregate) -> None:
        self.visit_inputs(aggregate)
        input_expr: Select = self._stack.pop()
        keys: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, alias)
            for alias, col in aggregate.keys.items()
        ]
        aggregations: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, alias)
            for alias, col in aggregate.aggregations.items()
        ]
        select_cols = keys + aggregations
        query: Select
        if (
            "group_by" in input_expr.args
            or "order" in input_expr.args
            or "limit" in input_expr.args
        ):
            query = self._build_subquery(input_expr, select_cols)
        else:
            query = self._merge_selects(
                select_cols, input_expr, find_identifiers_in_list(select_cols)
            )
        if keys:
            query = query.group_by(*keys)
        self._stack.append(query)

    def visit_limit(self, limit: Limit) -> None:
        self.visit_inputs(limit)
        input_expr: Select = self._stack.pop()
        assert isinstance(
            limit.limit, LiteralExpression
        ), "Limit currently only supports literals"
        limit_expr: SQLGlotExpression = self._expr_visitor.relational_to_sqlglot(
            limit.limit
        )
        exprs: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, alias)
            for alias, col in limit.columns.items()
        ]
        ordering_exprs: list[SQLGlotExpression] = self._convert_ordering(
            limit.orderings
        )
        query: Select
        if "order" in input_expr.args or "limit" in input_expr.args:
            query = self._build_subquery(input_expr, exprs)
        else:
            # Try merge the column sections
            query = self._merge_selects(
                exprs, input_expr, find_identifiers_in_list(ordering_exprs)
            )
        if ordering_exprs:
            query = query.order_by(*ordering_exprs)
        query = query.limit(limit_expr)
        self._stack.append(query)

    def visit_root(self, root: RelationalRoot) -> None:
        self.visit_inputs(root)
        input_expr: Select = self._stack.pop()
        # Pop the expressions in order.
        exprs: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, alias)
            for alias, col in root.ordered_columns
        ]
        ordering_exprs: list[SQLGlotExpression] = self._convert_ordering(root.orderings)
        query: Select
        if ordering_exprs and "order" in input_expr.args:
            query = self._build_subquery(input_expr, exprs)
        else:
            query = self._merge_selects(
                exprs, input_expr, find_identifiers_in_list(ordering_exprs)
            )
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
