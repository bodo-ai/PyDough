"""
The representation of a column function call for use in a relational tree.
"""

__all__ = ["WindowCallExpression"]


from pydough.pydough_operators import ExpressionWindowOperator
from pydough.types import PyDoughType

from .abstract_expression import RelationalExpression
from .expression_sort_info import ExpressionSortInfo


class WindowCallExpression(RelationalExpression):
    """
    The Expression implementation for calling a function
    on a relational node.
    """

    def __init__(
        self,
        op: ExpressionWindowOperator,
        return_type: PyDoughType,
        inputs: list[RelationalExpression],
        partition_inputs: list[RelationalExpression],
        order_inputs: list[ExpressionSortInfo],
        kwargs: dict[str, object],
    ) -> None:
        super().__init__(return_type)
        self._op: ExpressionWindowOperator = op
        self._inputs: list[RelationalExpression] = inputs
        # Repeated partition or order keys never change the result. Removing
        # them here covers every place a window expression is built or rebuilt
        # (translation, renaming, substitution, shuttles, simplification).
        self._partition_inputs: list[RelationalExpression] = []
        for partition_input in partition_inputs:
            if not any(
                partition_input.is_same_key(existing)
                for existing in self._partition_inputs
            ):
                self._partition_inputs.append(partition_input)
        self._order_inputs: list[ExpressionSortInfo] = ExpressionSortInfo.deduplicate(
            order_inputs
        )
        self._kwargs: dict[str, object] = kwargs

    @property
    def op(self) -> ExpressionWindowOperator:
        """
        The operation this call expression represents.
        """
        return self._op

    @property
    def is_aggregation(self) -> bool:
        return self.op.is_aggregation

    @property
    def inputs(self) -> list[RelationalExpression]:
        """
        The inputs to the operation.
        """
        return self._inputs

    @property
    def partition_inputs(self) -> list[RelationalExpression]:
        """
        The inputs used to partition the operation.
        """
        return self._partition_inputs

    @property
    def order_inputs(self) -> list[ExpressionSortInfo]:
        """
        The inputs to order the operation.
        """
        return self._order_inputs

    @property
    def kwargs(self) -> dict[str, object]:
        """
        The keyword arguments to the operation.
        """
        return self._kwargs

    def to_string(self, compact: bool = False) -> str:
        if compact:
            arg_strings: list[str] = [arg.to_string(compact) for arg in self.inputs]
            partition_strings: list[str] = [
                arg.to_string(compact) for arg in self.partition_inputs
            ]
            order_strings: list[str] = [
                arg.to_string(compact) for arg in self.order_inputs
            ]
            arg_string_lists: list[str] = [
                f"args=[{', '.join(arg_strings)}]",
                f"partition=[{', '.join(partition_strings)}]",
                f"order=[{', '.join(order_strings)}]",
            ]
            for kwarg in self.kwargs:
                arg_string_lists.append(f"{kwarg}={self.kwargs[kwarg]}")
            return self.op.to_string(arg_string_lists)
        else:
            return f"WindowCall(op={self.op}, inputs={self.inputs}, partition={self.partition_inputs}, order={self.order_inputs}, return_type={self.data_type}, kwargs={self.kwargs})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, WindowCallExpression)
            and (self.op == other.op)
            and (self.inputs == other.inputs)
            and (self.partition_inputs == other.partition_inputs)
            and (self.order_inputs == other.order_inputs)
            and (_typed_kwargs(self.kwargs) == _typed_kwargs(other.kwargs))
            and super().equals(other)
        )

    def __hash__(self) -> int:
        # Hash the same parts that `equals` compares, instead of the inherited
        # `to_string` hash, which prints the keyword arguments in insertion
        # order. Equal windows get equal hashes as long as their child
        # expressions do; this does not hold for children whose own hash comes
        # from `to_string`, e.g. `LiteralExpression` (1 == 1.0, but their
        # strings differ), which is a separate, pre-existing limitation.
        kwargs_hash: frozenset[tuple[str, int]] = frozenset(
            (name, _hash_or_zero(value))
            for name, value in _typed_kwargs(self.kwargs).items()
        )
        return hash(
            (
                self.op,
                self.data_type,
                tuple(self.inputs),
                tuple(self.partition_inputs),
                tuple(
                    (ordering.expr, ordering.ascending, ordering.nulls_first)
                    for ordering in self.order_inputs
                ),
                kwargs_hash,
            )
        )

    def accept(self, visitor: "RelationalExpressionVisitor") -> None:  # type: ignore # noqa
        visitor.visit_window_expression(self)

    def accept_shuttle(
        self,
        shuttle: "RelationalExpressionShuttle",  # type: ignore # noqa
    ) -> RelationalExpression:
        return shuttle.visit_window_expression(self)


def _typed_value(value: object) -> object:
    """
    Pairs a keyword argument value with its type, recursively inside tuples
    and lists, so that values that are `==` but have different types are not
    treated as equal. For example, `PREV(x, default=0)` and
    `PREV(x, default=0.0)` produce different SQL (`LAG(x, 1, 0)` versus
    `LAG(x, 1, 0.0)`) and must not be merged by the optimizer.

    Args:
        `value`: The keyword argument value.

    Returns:
        The value tagged with its type.
    """
    if isinstance(value, (tuple, list)):
        return (type(value), tuple(_typed_value(elem) for elem in value))
    return (type(value), value)


def _typed_kwargs(kwargs: dict[str, object]) -> dict[str, object]:
    """
    Returns the keyword arguments with every value tagged with its type, for
    comparing and hashing window expressions.

    Args:
        `kwargs`: The keyword arguments of a window expression.

    Returns:
        The keyword arguments with their values passed through
        `_typed_value`.
    """
    return {name: _typed_value(value) for name, value in kwargs.items()}


def _hash_or_zero(value: object) -> int:
    """
    Returns the hash of a value, or 0 if it is not hashable (e.g. a list), so
    that such values only contribute to a window's hash through their keyword
    name.

    Args:
        `value`: The value to hash.

    Returns:
        The hash of the value, or 0 if it is not hashable.
    """
    try:
        return hash(value)
    except TypeError:
        return 0
