"""
Representation of the a join node in a relational tree.
This node is responsible for holding all types of joins.
"""

from collections.abc import MutableMapping
from enum import Enum

from pydough.relational.relational_expressions import RelationalExpression
from pydough.types.boolean_type import BooleanType

from .abstract_node import Relational
from .relational_visitor import RelationalVisitor


class JoinType(Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL_OUTER = "full outer"


class Join(Relational):
    """
    Relational representation of any type of join operation, including
    inner, left, right, and full joins.
    """

    def __init__(
        self,
        left: Relational,
        right: Relational,
        condition: RelationalExpression,
        join_type: JoinType,
        columns: MutableMapping[str, RelationalExpression],
    ) -> None:
        super().__init__(columns)
        self._left: Relational = left
        self._right: Relational = right
        assert isinstance(
            condition.data_type, BooleanType
        ), "Join condition must be a boolean type"
        self._condition: RelationalExpression = condition
        self._join_type: JoinType = join_type

    @property
    def left(self) -> Relational:
        """
        The left input to the join.
        """
        return self._left

    @property
    def right(self) -> Relational:
        """
        The right input to the join.
        """
        return self._right

    @property
    def condition(self) -> RelationalExpression:
        """
        The condition for the join.
        """
        return self._condition

    @property
    def join_type(self) -> JoinType:
        """
        The type of the join.
        """
        return self._join_type

    @property
    def inputs(self):
        return [self.left, self.right]

    def node_equals(self, other: Relational) -> bool:
        return (
            isinstance(other, Join)
            and self.condition == other.condition
            and self.join_type == other.join_type
            and self.left.node_equals(other.left)
            and self.right.node_equals(other.right)
        )

    def to_string(self) -> str:
        return f"JOIN(cond={self.condition}, type={self.join_type.value}, columns={self.columns})"

    def accept(self, visitor: RelationalVisitor) -> None:
        visitor.visit_join(self)
