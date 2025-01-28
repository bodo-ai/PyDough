"""
TODO
"""

__all__ = ["CorrelatedReference"]

from pydough.types import PyDoughType

from .abstract_expression import RelationalExpression
from .relational_expression_shuttle import RelationalExpressionShuttle
from .relational_expression_visitor import RelationalExpressionVisitor


class CorrelatedReference(RelationalExpression):
    """
    TODO
    """

    def __init__(self, name: str, correl_name: str, data_type: PyDoughType) -> None:
        super().__init__(data_type)
        self._name: str = name
        self._correl_name: str = correl_name

    def __hash__(self) -> int:
        return hash((self.name, self.correl_name, self.data_type))

    @property
    def name(self) -> str:
        """
        The name of the column.
        """
        return self._name

    @property
    def correl_name(self) -> str:
        """
        The name of the correlation that the reference points to.
        """
        return self._correl_name

    def to_string(self, compact: bool = False) -> str:
        if compact:
            return f"{self.correl_name}.{self.name}"
        else:
            return f"CorrelatedReference(name={self.name}, correl_name={self.correl_name}, type={self.data_type})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, CorrelatedReference)
            and (self.name == other.name)
            and (self.correl_name == other.correl_name)
            and super().equals(other)
        )

    def accept(self, visitor: RelationalExpressionVisitor) -> None:
        visitor.visit_correlated_reference(self)

    def accept_shuttle(
        self, shuttle: RelationalExpressionShuttle
    ) -> RelationalExpression:
        return shuttle.visit_correlated_reference(self)
