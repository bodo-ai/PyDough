"""
Handle the conversion from the Relation Tree to a single
SQLGlot query.
"""

from collections.abc import MutableSequence

from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier, Select
from sqlglot.expressions import Literal as SQLGlotLiteral

from pydough.relational.relational_expressions import ColumnSortInfo, LiteralExpression
from pydough.relational.relational_nodes import (
    Aggregate,
    Filter,
    Join,
    Limit,
    Project,
    Relational,
    RelationalRoot,
    RelationalVisitor,
    Scan,
)

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
            isinstance(c, (SQLGlotLiteral, Identifier)) for c in new_columns
        )
        if can_merge:
            modified_new_columns = None
            modified_old_columns = []
            # Create a mapping for the old columns so we can replace column
            # references.
            old_column_map = {c.alias: c for c in old_columns}
            seen_cols: set[Identifier] = set()
            for new_column in new_columns:
                if isinstance(new_column, SQLGlotLiteral):
                    # If the new column is a literal, we can just add it to the old
                    # columns.
                    modified_old_columns.append(new_column)
                else:
                    expr = old_column_map[new_column.this]
                    # Note: This wouldn't be safe if we reused columns in
                    # multiple places, but this is currently okay.
                    expr.set("alias", new_column.alias)
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
            return Select().select(*new_exprs).from_(orig_select)

    def _convert_ordering(
        self, ordering: MutableSequence[ColumnSortInfo]
    ) -> list[SQLGlotExpression]:
        """
        Convert the orderings from the a relational operator into a variant
        that can be used in SQLGlot.

        Args:
            ordering (MutableSequence[ColumnSortInfo]): The orderings to convert.

        Returns:
            list[SQLGlotExpression]: The converted orderings.
        """
        col_exprs = []
        for col in ordering:
            col_expr = self._expr_visitor.relational_to_sqlglot(col.column)
            if col.ascending:
                col_expr = col_expr.asc(nulls_first=col.nulls_first)
            else:
                col_expr = col_expr.desc(nulls_first=col.nulls_first)
            col_exprs.append(col_expr)
        return col_exprs

    def reset(self) -> None:
        """
        Reset clears the stack and resets the expression visitor.
        """
        self._stack = []
        self._expr_visitor.reset()

    def visit(self, relational: Relational) -> None:
        raise NotImplementedError("SQLGlotRelationalVisitor.visit")

    def visit_scan(self, scan: Scan) -> None:
        exprs: list[SQLGlotExpression] = [
            self._expr_visitor.relational_to_sqlglot(col, alias)
            for alias, col in scan.columns.items()
        ]
        query: Select = Select().select(*exprs).from_(scan.table_name)
        self._stack.append(query)

    def visit_join(self, join: Join) -> None:
        raise NotImplementedError("SQLGlotRelationalVisitor.visit_join")

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
        if (
            "where" in input_expr.args
            or "order" in input_expr.args
            or "limit" in input_expr.args
        ):
            # Check if we already have a where clause or limit. We
            # cannot merge these yet.
            # TODO: Consider allowing combining where if limit isn't
            # present?
            query = Select().select(*exprs).from_(input_expr)
        else:
            # Try merge the column sections
            query = self._merge_selects(exprs, input_expr, find_identifiers(cond))
        query = query.where(cond)
        self._stack.append(query)

    def visit_aggregate(self, aggregate: Aggregate) -> None:
        raise NotImplementedError("SQLGlotRelationalVisitor.visit_aggregate")

    def visit_limit(self, limit: Limit) -> None:
        self.visit_inputs(limit)
        input_expr: Select = self._stack.pop()
        assert isinstance(
            limit.limit, LiteralExpression
        ), "Limit only supports literals"
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
            query = Select().select(*exprs).from_(input_expr)
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
        raise NotImplementedError("SQLGlotRelationalVisitor.visit_root")

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
