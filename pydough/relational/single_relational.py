"""
Base abstract class for relational nodes that have a single input.
This is done to reduce code duplication.
"""

from abc import abstractmethod
from collections.abc import MutableSequence

from sqlglot.expressions import Expression

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
