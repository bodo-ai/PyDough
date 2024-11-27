"""
Representation of the a join node in a relational tree.
This node is responsible for holding all types of joins.
"""

from collections.abc import MutableMapping, MutableSequence
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
    ANTI = "anti"
    SEMI = "semi"


class Join(Relational):
    """
    Relational representation of all join operations. This single
    node can represent multiple joins at once, similar to a multi-join
    in other systems to enable better lowering and easier translation
    from earlier stages in the pipeline.

    However, unlike a traditional Multi-Join in most relational algebra
    implementations, this join does not ensure that joins can be reordered
    and provides a specific join ordering that is the only guaranteed
    valid ordering.

    In particular if we have 3 inputs A, B, and C, with join types INNER
    and SEMI, then the join ordering is treated as:

    (A INNER B) SEMI C

    It should be noted that this isn't necessarily the only valid join ordering,
    but this node makes no guarantees that the inputs can be reordered.
    """

    def __init__(
        self,
        inputs: MutableSequence[Relational],
        conditions: list[RelationalExpression],
        join_types: list[JoinType],
        columns: MutableMapping[str, RelationalExpression],
    ) -> None:
        super().__init__(columns)
        num_inputs = len(inputs)
        num_conditions = len(conditions)
        num_join_types = len(join_types)
        assert (
            num_inputs >= 2
            and num_conditions == (num_inputs - 1)
            and num_conditions == num_join_types
        ), "Number of inputs, conditions, and join types must be the same"
        self._inputs = inputs
        assert all(
            isinstance(cond.data_type, BooleanType) for cond in conditions
        ), "Join condition must be a boolean type"
        self._conditions: list[RelationalExpression] = conditions
        self._join_types: list[JoinType] = join_types

    @property
    def conditions(self) -> list[RelationalExpression]:
        """
        The conditions for the joins.
        """
        return self._conditions

    @property
    def join_types(self) -> list[JoinType]:
        """
        The types of the joins.
        """
        return self._join_types

    @property
    def inputs(self) -> MutableSequence[Relational]:
        return self._inputs

    @property
    def default_input_aliases(self) -> list[str | None]:
        """
        Provide the default aliases for each input
        to this node. This is used when remapping the
        names of each input for differentiating columns.

        Note: The lowering steps are not required to use this alias
        and can choose any name they want.
        """
        return [f"t{i}" for i in range(len(self.inputs))]

    def node_equals(self, other: Relational) -> bool:
        return (
            isinstance(other, Join)
            and self.conditions == other.conditions
            and self.join_types == other.join_types
            and all(
                self.inputs[i].node_equals(other.inputs[i])
                for i in range(len(self.inputs))
            )
        )

    def to_string(self) -> str:
        return f"JOIN(conditions={self.conditions}, types={[t.value for t in self.join_types]}, columns={self.columns})"

    def accept(self, visitor: RelationalVisitor) -> None:
        visitor.visit_join(self)

    def node_copy(
        self,
        columns: MutableMapping[str, RelationalExpression],
        inputs: MutableSequence[Relational],
    ) -> Relational:
        return Join(inputs, self.conditions, self.join_types, columns)
