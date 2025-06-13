"""
Representation of the a join node in a relational tree.
This node is responsible for holding all types of joins.
"""

from enum import Enum

from pydough.relational.relational_expressions import RelationalExpression
from pydough.types.boolean_type import BooleanType

from .abstract_node import RelationalNode


class JoinType(Enum):
    """
    TODO: add description
    """

    INNER = "inner"
    LEFT = "left"
    ANTI = "anti"
    SEMI = "semi"


class JoinCardinality(Enum):
    """
    Enum describing the relationship between the LHS and RHS of a join in terms
    of whether the LHS matches onto 1 or more rows of the RHS, and whether the
    join can cause the LHS to be filtered or not.
    """

    SINGULAR_FILTER = 1
    SINGULAR_ACCESS = 2
    PLURAL_FILTER = 3
    PLURAL_ACCESS = 4
    UNKNOWN = 5

    def add_filter(self) -> "JoinCardinality":
        """
        Returns a new JoinCardinality referring to the current value but with
        filtering added.
        """
        if self == JoinCardinality.SINGULAR_ACCESS:
            return JoinCardinality.SINGULAR_FILTER
        elif self == JoinCardinality.PLURAL_ACCESS:
            return JoinCardinality.PLURAL_FILTER
        else:
            return self

    @property
    def potentially_filters(self) -> bool:
        """
        Returns whether this JoinCardinality indicates that the LHS is
        potentially filtered by being joined with the RHS.
        """
        return self in (
            JoinCardinality.SINGULAR_FILTER,
            JoinCardinality.PLURAL_FILTER,
            JoinCardinality.UNKNOWN,
        )

    @property
    def potentially_plural(self) -> bool:
        """
        Returns whether this JoinCardinality indicates that the LHS can
        potentially match with multiple records of the RHS.
        """
        return self in (
            JoinCardinality.PLURAL_FILTER,
            JoinCardinality.PLURAL_ACCESS,
            JoinCardinality.UNKNOWN,
        )


class Join(RelationalNode):
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
        inputs: list[RelationalNode],
        condition: RelationalExpression,
        join_type: JoinType,
        columns: dict[str, RelationalExpression],
        cardinality: JoinCardinality = JoinCardinality.UNKNOWN,
        correl_name: str | None = None,
    ) -> None:
        super().__init__(columns)
        assert len(inputs) == 2, f"Expected 2 inputs, received {len(inputs)}"
        self._inputs = inputs
        assert isinstance(condition.data_type, BooleanType), (
            "Join condition must be a boolean type"
        )
        self._condition: RelationalExpression = condition
        self._join_type: JoinType = join_type
        self._cardinality: JoinCardinality = cardinality
        self._correl_name: str | None = correl_name

    @property
    def correl_name(self) -> str | None:
        """
        The name used to refer to the first join input when subsequent inputs
        have correlated references.
        """
        return self._correl_name

    @property
    def condition(self) -> RelationalExpression:
        """
        The condition for the joins.
        """
        return self._condition

    @condition.setter
    def condition(self, cond: RelationalExpression) -> None:
        """
        The setter for the join condition
        """
        self._condition = cond

    @property
    def join_type(self) -> JoinType:
        """
        The type of the joins.
        """
        return self._join_type

    @join_type.setter
    def join_type(self, join_type: JoinType) -> None:
        """
        The setter for the join type
        """
        self._join_type = join_type

    @property
    def cardinality(self) -> JoinCardinality:
        """
        The type of the joins.
        """
        return self._cardinality

    @cardinality.setter
    def cardinality(self, cardinality: JoinCardinality) -> None:
        """
        The setter for the join cardinality.
        """
        self._cardinality = cardinality

    @property
    def inputs(self) -> list[RelationalNode]:
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

    def node_equals(self, other: RelationalNode) -> bool:
        return (
            isinstance(other, Join)
            and self.condition == other.condition
            and self.join_type == other.join_type
            and self.cardinality == other.cardinality
            and self.correl_name == other.correl_name
            and all(
                self.inputs[i].node_equals(other.inputs[i])
                for i in range(len(self.inputs))
            )
        )

    def to_string(self, compact: bool = False) -> str:
        correl_suffix: str = (
            "" if self.correl_name is None else f", correl_name={self.correl_name!r}"
        )
        cardinality_suffix: str = (
            ""
            if self.cardinality == JoinCardinality.UNKNOWN
            else f", cardinality={self.cardinality.name}"
        )
        return f"JOIN(condition={self.condition.to_string(compact)}, type={self.join_type.name}{cardinality_suffix}, columns={self.make_column_string(self.columns, compact)}{correl_suffix})"

    def accept(self, visitor: "RelationalVisitor") -> None:  # type: ignore # noqa
        visitor.visit_join(self)

    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list[RelationalNode],
    ) -> RelationalNode:
        return Join(
            inputs,
            self.condition,
            self.join_type,
            columns,
            self.cardinality,
            self.correl_name,
        )
