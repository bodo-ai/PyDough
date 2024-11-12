"""
Base abstract class for relational nodes that have a single input.
This is done to reduce code duplication.
"""

from abc import abstractmethod
from collections.abc import MutableSequence

from sqlglot.expressions import Expression, Identifier, Literal

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational


class SingleRelational(Relational):
    """
    Base abstract class for relational nodes that have a single input.
    """

    def __init__(
        self,
        input: Relational,
        columns: MutableSequence["Column"],
        orderings: MutableSequence["PyDoughExpressionAST"] | None,
    ) -> None:
        super().__init__(columns, orderings)
        self._input: Relational = input

    @property
    def inputs(self):
        return [self._input]

    @property
    def input(self) -> Relational:
        return self._input

    def node_equals(self, other: "Relational") -> bool:
        """
        Determine if two relational nodes are exactly identical,
        excluding column ordering. This should be extended to avoid
        duplicating equality logic shared across relational nodes.

        Args:
            other (Relational): The other relational node to compare against.

        Returns:
            bool: Are the two relational nodes equal.
        """
        # TODO: Do we need a fast path for caching the inputs?
        return isinstance(other, SingleRelational) and self.input.equals(other.input)

    def node_can_merge(self, other: "Relational") -> bool:
        """
        Determine if two relational nodes can be merged together.
        This should be extended to avoid duplicating merge logic shared
        across relational nodes.

        Args:
            other (Relational): The other relational node to merge against.

        Returns:
            bool: Can the two relational nodes be merged.
        """
        # TODO: Can the inputs be merged without being exactly equal?
        return isinstance(other, SingleRelational) and self.input.equals(other.input)

    def to_sqlglot(self) -> Expression:
        input_expr: Expression = self.input.to_sqlglot()
        return self.input_modifying_to_sqlglot(input_expr)

    @abstractmethod
    def input_modifying_to_sqlglot(self, input_expr: Expression) -> Expression:
        """
        Implementation of the to_sqlglot method that works by taking the already generated
        input expression and possibly modifying it to produce a new SQLGlot expression.
        This is useful in cases where we have stacked relational operators to avoid generating
        unnecessary nested sub queries.

        For example, imagine we have the following tree:

        Project(columns=["a": column(b), "c": Literal(1)])
            Scan(columns=[column(a), column(b)], table="table")

        Then input_expr would be:
            Select a as a, b as b from table

        Without modifying the input expression, the output would be:
            Select b, 1 from (Select a as a, b as b from table)

        But with modifying the input expression, the output would be:
            Select b, 1 from table

        Notably not every input can be modified. For example, if a where clause requires
        a column that is computed in the input we will generated a nested query rather than
        attempt to determine when we can/should inline the computation. This is the type of work
        that an optimizer should do.

        Args:
            input_expr (Expression): The original input expression. This may be modified directly or
                could be used as the from clause in a new query.

        Returns:
            Expression: The new SQLGlot expression for the input after possibly modifying the input.
        """

    @staticmethod
    def merge_sqlglot_columns(
        new_columns: list[Expression],
        old_columns: list[Expression],
        old_column_deps: set[str],
    ) -> tuple[list[Expression] | None, list[Expression]]:
        """
        Attempt to merge the new_columns with the old_columns whenever possible to reduce the amount of
        SQL that needs to be generated for a new given column list. In addition to the columns themselves,
        the old_columns could also produce dependencies for the new expression since it logically occurs
        before the new columns, so we need to determine those for tracking.

        The final result is presented as a tuple of two lists, the first list is the new columns
        that need to be produced in a separate query and the second list is the list of columns that
        can be placed in the original query. The new columns will be None if we can generate the SQL
        entirely in the original query.

        Args:
            new_columns (list[Expression]): The new columns that need to be the output of the current Relational node.
            old_columns (list[Expression]): The old columns that were the output of the previous Relational node.
            deps (set[str]): A set of column names that are dependencies of the old columns in some operator other than
                the "SELECT" component. For example a filter will need to include the column names of any WHERE conditions.

        Returns:
            tuple[list[Expression] | None, list[Expression]]: The columns that should be generated in a separate
                new select statement and the columns that can be placed in the original query.
        """
        if old_column_deps:
            # TODO: Support dependencies. We will implement this once
            # we have an operator that generates dependencies working (e.g. filter).
            return new_columns, old_columns
        # Only support fusing columns that are simple renames, reordering, or literals for now.
        # If we see just column references or literals though we can always merge.
        # TODO: Enable merging more complex expressions for example we can merge a + b
        # if a and b are both just simple columns in the input.
        can_merge: bool = all(isinstance(c, (Literal, Identifier)) for c in new_columns)
        if can_merge:
            modified_new_columns = None
            modified_old_columns = []
            # Create a mapping for the old columns so we can replace column references.
            old_column_map = {c.alias: c.this for c in old_columns}
            for new_column in new_columns:
                if isinstance(new_column, Literal):
                    # If the new column is a literal, we can just add it to the old columns.
                    modified_old_columns.append(new_column)
                else:
                    modified_old_columns.append(
                        Identifier(
                            alias=new_column.alias, this=old_column_map[new_column.this]
                        )
                    )
            return modified_new_columns, modified_old_columns
        else:
            return new_columns, old_columns
