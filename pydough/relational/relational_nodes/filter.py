"""
This file contains the relational implementation for a "filter". This is our
relational representation statements that map to where, having, or qualify
in SQL.
"""

from collections.abc import MutableMapping

from pydough.relational.relational_expressions import RelationalExpression
from pydough.types.boolean_type import BooleanType

from .abstract_node import Relational
from .relational_visitor import RelationalVisitor
from .single_relational import SingleRelational


class Filter(SingleRelational):
    """
    The Filter node in the relational tree. This generally represents all the possible
    locations where filtering can be applied.
    """

    def __init__(
        self,
        input: Relational,
        condition: RelationalExpression,
        columns: MutableMapping[str, RelationalExpression],
    ) -> None:
        super().__init__(input, columns)
        assert isinstance(
            condition.data_type, BooleanType
        ), "Filter condition must be a boolean type"
        self._condition: RelationalExpression = condition

    @property
    def condition(self) -> RelationalExpression:
        """
        The condition that is being filtered on.
        """
        return self._condition

    def node_equals(self, other: Relational) -> bool:
        return (
            isinstance(other, Filter)
            and self.condition == other.condition
            and super().node_equals(other)
        )

    def to_string(self, compact: bool = False) -> str:
        return f"FILTER(condition={self.condition.to_string(compact)}, columns={self.make_column_string(self.columns, compact)})"

    def accept(self, visitor: RelationalVisitor) -> None:
        visitor.visit_filter(self)
