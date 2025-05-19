"""
Definitions of UnqualifiedNode classes that are used as the first IR created by
PyDough whenever a user writes PyDough code.
"""

__all__ = [
    "UnqualifiedAccess",
    "UnqualifiedBinaryOperation",
    "UnqualifiedCalculate",
    "UnqualifiedLiteral",
    "UnqualifiedNode",
    "UnqualifiedOperation",
    "UnqualifiedOperator",
    "UnqualifiedOrderBy",
    "UnqualifiedPartition",
    "UnqualifiedRoot",
    "UnqualifiedTopK",
    "UnqualifiedWhere",
    "UnqualifiedWindow",
    "display_raw",
]

from abc import ABC
from collections.abc import Iterable
from datetime import date
from typing import Any, Union

import pydough.pydough_operators as pydop
from pydough.metadata import GraphMetadata
from pydough.metadata.errors import is_bool, is_integer, is_positive_int, is_string
from pydough.pydough_operators import get_operator_by_name
from pydough.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    Float64Type,
    Int64Type,
    PyDoughType,
    StringType,
    UnknownType,
)

from .errors import PyDoughUnqualifiedException


class UnqualifiedNode(ABC):
    """
    Base class used to describe PyDough nodes before they have been properly
    qualified. Note: every implementation class has a field `_parcel` storing
    a tuple of its core data fields. No properties should ever collide with
    this name.
    """

    def __repr__(self):
        return display_raw(self)

    @staticmethod
    def coerce_to_unqualified(obj: object) -> "UnqualifiedNode":
        """
        Attempts to coerce an arbitrary Python object to an UnqualifiedNode
        instance.

        Args:
            `obj`: the object to be coerced to an UnqualifiedNode.

        Returns:
            The coerced UnqualifiedNode.

        Raises:
            `PyDoughUnqualifiedException` if the object cannot be coerced, e.g.
            if it is a Python object that has no translation into a PyDough
            literal.
        """
        if isinstance(obj, UnqualifiedNode):
            return obj
        if isinstance(obj, bool):
            return UnqualifiedLiteral(obj, BooleanType())
        if isinstance(obj, int):
            return UnqualifiedLiteral(obj, Int64Type())
        if isinstance(obj, float):
            return UnqualifiedLiteral(obj, Float64Type())
        if isinstance(obj, str):
            return UnqualifiedLiteral(obj, StringType())
        if isinstance(obj, bytes):
            return UnqualifiedLiteral(obj, BinaryType())
        if isinstance(obj, date):
            return UnqualifiedLiteral(obj, DateType())
        if obj is None:
            return UnqualifiedLiteral(obj, UnknownType())
        if isinstance(obj, (list, tuple, set)):
            elems: list[UnqualifiedLiteral] = []
            typ: PyDoughType = UnknownType()
            for elem in obj:
                coerced_elem = UnqualifiedNode.coerce_to_unqualified(elem)
                assert isinstance(coerced_elem, UnqualifiedLiteral), (
                    f"Can only coerce list of literals to a literal, not {elem}"
                )
                elems.append(coerced_elem)
            return UnqualifiedLiteral(elems, ArrayType(typ))
        raise PyDoughUnqualifiedException(f"Cannot coerce {obj!r} to a PyDough node.")

    def __getattribute__(self, name: str) -> Any:
        try:
            result = super().__getattribute__(name)
            return result
        except AttributeError:
            return UnqualifiedAccess(self, name)

    def __setattr__(self, name: str, value: object) -> None:
        if name == "_parcel":
            super().__setattr__(name, value)
        else:
            # TODO: support using setattr to add/mutate properties.
            raise AttributeError(
                "PyDough objects do not yet support writing properties to them."
            )

    def __hash__(self):
        return hash(repr(self))

    def __getitem__(self, key):
        if isinstance(key, slice):
            args: list[UnqualifiedNode] = [self]
            for arg in (key.start, key.stop, key.step):
                coerced_elem = UnqualifiedNode.coerce_to_unqualified(arg)
                if not isinstance(coerced_elem, UnqualifiedLiteral):
                    raise PyDoughUnqualifiedException(
                        "PyDough objects are currently not supported to be used as indices in Python slices."
                    )
                args.append(coerced_elem)
            operator = get_operator_by_name("SLICE")
            return UnqualifiedOperation(operator, args)
        else:
            raise PyDoughUnqualifiedException(
                f"Cannot index into PyDough object {self} with {key!r}"
            )

    def __call__(self, *args, **kwargs):
        raise PyDoughUnqualifiedException(
            f"PyDough nodes {self!r} is not callable. Did you mean to use a function?"
        )

    def __bool__(self):
        raise PyDoughUnqualifiedException(
            "PyDough code cannot be treated as a boolean. If you intend to do a logical operation, use `|`, `&` and `~` instead of `or`, `and` and `not`."
        )

    def __add__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("+", self, other_unqualified)

    def __radd__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("+", other_unqualified, self)

    def __sub__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("-", self, other_unqualified)

    def __rsub__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("-", other_unqualified, self)

    def __mul__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("*", self, other_unqualified)

    def __rmul__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("*", other_unqualified, self)

    def __truediv__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("/", self, other_unqualified)

    def __rtruediv__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("/", other_unqualified, self)

    def __pow__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("**", self, other_unqualified)

    def __rpow__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("**", other_unqualified, self)

    def __mod__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("%", self, other_unqualified)

    def __rmod__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("%", other_unqualified, self)

    def __eq__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("==", self, other_unqualified)

    def __ne__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("!=", self, other_unqualified)

    def __lt__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("<", self, other_unqualified)

    def __le__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("<=", self, other_unqualified)

    def __gt__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation(">", self, other_unqualified)

    def __ge__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation(">=", self, other_unqualified)

    def __and__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("&", self, other_unqualified)

    def __rand__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("&", other_unqualified, self)

    def __or__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("|", self, other_unqualified)

    def __ror__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("|", other_unqualified, self)

    def __xor__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("^", self, other_unqualified)

    def __rxor__(self, other: object):
        other_unqualified: UnqualifiedNode = self.coerce_to_unqualified(other)
        return UnqualifiedBinaryOperation("^", other_unqualified, self)

    def __pos__(self):
        return self

    def __neg__(self):
        return 0 - self

    def __invert__(self):
        operator = get_operator_by_name("NOT")
        return UnqualifiedOperation(operator, [self])

    def CALCULATE(self, *args, **kwargs: dict[str, object]):
        calc_args: list[tuple[str, UnqualifiedNode]] = []
        counter = 0
        for arg in args:
            unqualified_arg: UnqualifiedNode = self.coerce_to_unqualified(arg)
            name: str
            if isinstance(unqualified_arg, UnqualifiedAccess):
                name = unqualified_arg._parcel[1]
            else:
                while True:
                    name = f"_expr{counter}"
                    counter += 1
                    if name not in kwargs:
                        break
            calc_args.append((name, unqualified_arg))
        for name, arg in kwargs.items():
            calc_args.append((name, self.coerce_to_unqualified(arg)))
        return UnqualifiedCalculate(self, calc_args)

    def __abs__(self):
        operator = get_operator_by_name("ABS")
        return UnqualifiedOperation(operator, [self])

    def __round__(self, n=None):
        if n is None:
            n = 0
        n_unqualified = self.coerce_to_unqualified(n)
        operator = get_operator_by_name("ROUND")
        return UnqualifiedOperation(operator, [self, n_unqualified])

    def __floor__(self):
        raise PyDoughUnqualifiedException(
            "PyDough does not support the math.floor function at this time."
        )

    def __ceil__(self):
        raise PyDoughUnqualifiedException(
            "PyDough does not support the math.ceil function at this time."
        )

    def __trunc__(self):
        raise PyDoughUnqualifiedException(
            "PyDough does not support the math.trunc function at this time."
        )

    def __reversed__(self):
        raise PyDoughUnqualifiedException(
            "PyDough does not support the reversed function at this time."
        )

    def __int__(self):
        raise PyDoughUnqualifiedException("PyDough objects cannot be cast to int.")

    def __float__(self):
        raise PyDoughUnqualifiedException("PyDough objects cannot be cast to float.")

    def __complex__(self):
        raise PyDoughUnqualifiedException("PyDough objects cannot be cast to complex.")

    def __index__(self):
        raise PyDoughUnqualifiedException(
            "PyDough objects cannot be used as indices in Python slices."
        )

    def __nonzero__(self):
        return self.__bool__()

    def __len__(self):
        raise PyDoughUnqualifiedException(
            "PyDough objects cannot be used with the len function."
        )

    def __contains__(self, item):
        raise PyDoughUnqualifiedException(
            "PyDough objects cannot be used with the 'in' operator."
        )

    def __setitem__(self, key, value):
        raise PyDoughUnqualifiedException(
            "PyDough objects cannot support item assignment."
        )

    def WHERE(self, cond: object) -> "UnqualifiedWhere":
        cond_unqualified: UnqualifiedNode = self.coerce_to_unqualified(cond)
        return UnqualifiedWhere(self, cond_unqualified)

    def ORDER_BY(self, *keys) -> "UnqualifiedOrderBy":
        keys_unqualified: list[UnqualifiedNode] = [
            self.coerce_to_unqualified(key) for key in keys
        ]
        return UnqualifiedOrderBy(self, keys_unqualified)

    def TOP_K(
        self, k: int, by: object | Iterable[object] | None = None
    ) -> "UnqualifiedTopK":
        if by is None:
            return UnqualifiedTopK(self, k, None)
        else:
            keys_unqualified: list[UnqualifiedNode]
            if isinstance(by, Iterable):
                keys_unqualified = [self.coerce_to_unqualified(key) for key in by]
            else:
                keys_unqualified = [self.coerce_to_unqualified(by)]
            return UnqualifiedTopK(self, k, keys_unqualified)

    def ASC(self, na_pos: str = "first") -> "UnqualifiedCollation":
        assert na_pos in (
            "first",
            "last",
        ), f"Unrecognized `na_pos` value for `ASC`: {na_pos!r}"
        return UnqualifiedCollation(self, True, na_pos == "last")

    def DESC(self, na_pos: str = "last") -> "UnqualifiedCollation":
        assert na_pos in (
            "first",
            "last",
        ), f"Unrecognized `na_pos` value for `DESC`: {na_pos!r}"
        return UnqualifiedCollation(self, False, na_pos == "last")

    def PARTITION(
        self,
        name: str,
        by: Union[Iterable["UnqualifiedNode"], "UnqualifiedNode"],
    ) -> "UnqualifiedPartition":
        """
        Method used to create a PARTITION node.
        """
        if isinstance(by, UnqualifiedNode):
            return UnqualifiedPartition(self, name, [by])
        else:
            return UnqualifiedPartition(self, name, list(by))

    def SINGULAR(self) -> "UnqualifiedSingular":
        """
        Method used to create a SINGULAR node.
        """
        return UnqualifiedSingular(self)

    def BEST(
        self,
        by: Union[Iterable["UnqualifiedNode"], "UnqualifiedNode"],
        per: str | None = None,
        allow_ties: bool = False,
        n_best: int = 1,
    ) -> "UnqualifiedNode":
        """
        Method used to create the BEST logic, where x.y.BEST(by=z, per="x") is later
        expanded into `x.y.WHERE(RANKING(by=z) == 1, per="x").SINGULAR()`,
        with the following variations:
        - If `allow_ties` is True: `x.y.WHERE(RANKING(by=z, per="x", allow_ties=True) == 1)`
        - If `n_best` > 1 : `x.y.WHERE(RANKING(by=z, per="x") <= n_best)`

        Args:
            `node`: the data to find the best entry of with regards to the
            current node.
            `by`: the collation terms to order by in the `RANKING` call.
            `per`: the ancestor that the `BEST` computation is happening with
            regards to, or None if it is a global optimum being sought.
            `allow_ties`: whether to allow ties in the ranking. If True, it
            means that if there are multiple entries with the same rank, they
            will all be considered as "best" candidates. This cannot be used
            with `n_best > 1`.
            `n_best`: the number of best entries to consider. If greater than
            1, it will find all entries with a rank less than or equal to
            `n_best`. This cannot be used when `allow_ties` is True.

        Returns:
            The unqualified node for the relevant expression.
        """
        # Verify the n_best & allow_ties arguments are well formed
        if n_best < 1:
            raise PyDoughUnqualifiedException(f"Invalid n_best value: {n_best}")
        if n_best > 1 and allow_ties:
            raise PyDoughUnqualifiedException(
                "Cannot allow ties when multiple best values are requested"
            )

        if isinstance(by, UnqualifiedNode):
            by = [by]

        return UnqualifiedBest(self, by, per, allow_ties, n_best)


class UnqualifiedRoot(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a root, meaning that
    anything pointing to this node as an ancestor/predecessor must be derivable
    at the top level from the graph, or is impossible to derive until placed
    within a context.
    """

    def __init__(self, graph: GraphMetadata):
        self._parcel: tuple[GraphMetadata, set[str]] = (
            graph,
            {
                operator_name
                for operator_name, operator in pydop.builtin_registered_operators().items()
                if not isinstance(operator, pydop.BinaryOperator)
            },
        )

    def __getattribute__(self, name: str) -> Any:
        if name in super(UnqualifiedNode, self).__getattribute__("_parcel")[1]:
            return UnqualifiedOperator(name)
        else:
            return super().__getattribute__(name)


class UnqualifiedLiteral(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a literal whose value is
    a Python operation.
    """

    def __init__(self, literal: object, typ: PyDoughType):
        self._parcel: tuple[object, PyDoughType] = (literal, typ)


class UnqualifiedCollation(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a collation expression.
    """

    def __init__(self, node: UnqualifiedNode, asc: bool, na_pos: bool):
        self._parcel: tuple[UnqualifiedNode, bool, bool] = (node, asc, na_pos)


def get_by_arg(
    kwargs: dict[str, object],
    window_operator: pydop.ExpressionWindowOperator,
) -> list[UnqualifiedNode]:
    """
    Extracts the `by` argument from the keyword arguments to a window function,
    verifying that it exists, removing it from the kwargs, and converting it to
    an iterable if it was a single unqualified node.

    Args:
        `kwargs`: the keyword arguments.
        `window_operator`: the function whose `by` argument being extracted.

    Returns:
        The list of unqualified nodes represented by the `by` argument, which
        is removed from `kwargs`.

    Raises:
        `PyDoughUnqualifiedException` if the `by` argument is missing or the
        wrong type.
    """
    is_cumulative: bool = bool(kwargs.get("cumulative", False))
    is_frame: bool = "frame" in kwargs
    has_by = "by" in kwargs
    # Verify the arguments are well formed when cumulative or frame are provided
    if is_cumulative:
        if not window_operator.allows_frame:
            raise PyDoughUnqualifiedException(
                f"The function `{window_operator.function_name}` does not allow the `cumulative` argument"
            )
        if not has_by:
            raise PyDoughUnqualifiedException(
                f"The `by` argument to `{window_operator.function_name}` must be provided when the `cumulative` argument is True"
            )
        if is_frame:
            raise PyDoughUnqualifiedException(
                f"The `cumulative` argument to `{window_operator.function_name}` cannot be used with the `frame` argument"
            )
    elif is_frame:
        if not window_operator.allows_frame:
            raise PyDoughUnqualifiedException(
                f"The function `{window_operator.function_name}` does not allow the `frame` argument"
            )
        if not has_by:
            raise PyDoughUnqualifiedException(
                f"The `by` argument to `{window_operator.function_name}` must be provided when the `frame` argument is provided"
            )
        frame = kwargs.get("frame")
        if not isinstance(frame, tuple) or len(frame) != 2:
            raise PyDoughUnqualifiedException(
                f"Malformed `frame` argument to `{window_operator.function_name}`: {frame!r} (must be a tuple of two integers or None values)"
            )
        lower, upper = frame
        if not isinstance(lower, (int, type(None))) or not isinstance(
            upper, (int, type(None))
        ):
            raise PyDoughUnqualifiedException(
                f"Malformed `frame` argument to `{window_operator.function_name}`: {frame!r} (must be a tuple of two integers or None values)"
            )
        if lower is not None and upper is not None and lower > upper:
            raise PyDoughUnqualifiedException(
                f"Malformed `frame` argument to `{window_operator.function_name}`: {frame!r} (lower bound must be less than or equal to upper bound)"
            )
    if has_by:
        # Verify the arguments are well formed when by is provided.
        if window_operator.allows_frame:
            if not (is_cumulative or is_frame):
                raise PyDoughUnqualifiedException(
                    f"When the `by` argument to `{window_operator.function_name}` is provided, either `cumulative=True` or the `frame` argument must be provided"
                )
        elif not window_operator.requires_order:
            raise PyDoughUnqualifiedException(
                f"The `{window_operator.function_name}` function does not allow a `by` argument"
            )
    else:
        # Verify the arguments are well formed when by is not provided.
        if window_operator.requires_order:
            raise PyDoughUnqualifiedException(
                f"The `by` argument to `{window_operator.function_name}` must be provided"
            )
        else:
            return []
    # Now that the arguments have been verified, extract the by argument and
    # convert to a singleton list if it is a single unqualified node.
    by = kwargs.pop("by")
    if isinstance(by, UnqualifiedNode):
        by = [by]
    elif not (
        isinstance(by, (tuple, list))
        and all(isinstance(arg, UnqualifiedNode) for arg in by)
        and len(by) > 0
    ):
        raise PyDoughUnqualifiedException(
            f"The `by` argument to `{window_operator.function_name}` must be a single expression or a non-empty list/tuple of expressions. "
            "Please refer to the config documentation for more information."
        )
    return list(by)


class UnqualifiedOperator(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a function that has
    yet to be called.
    """

    def __init__(self, name: str):
        self._parcel: tuple[str] = (name,)

    def __call__(self, *args, **kwargs):
        per: str | None = None
        window_operator: pydop.ExpressionWindowOperator
        is_window: bool = True
        operands: list[UnqualifiedNode] = []
        func_str: str = self._parcel[0]
        for arg in args:
            operands.append(self.coerce_to_unqualified(arg))
        match func_str:
            case "PERCENTILE":
                window_operator = pydop.PERCENTILE
                is_positive_int.verify(
                    kwargs.get("n_buckets", 100), "`n_buckets` argument"
                )
            case "RANKING":
                window_operator = pydop.RANKING
                is_bool.verify(kwargs.get("allow_ties", False), "`allow_ties` argument")
                is_bool.verify(kwargs.get("dense", False), "`dense` argument")
            case "PREV" | "NEXT":
                window_operator = pydop.PREV if func_str == "PREV" else pydop.NEXT
                is_integer.verify(kwargs.get("n", 1), "`n` argument")
                if len(args) > 1:
                    is_integer.verify(args[1], "`n` argument")
            case "RELSUM":
                window_operator = pydop.RELSUM
            case "RELAVG":
                window_operator = pydop.RELAVG
            case "RELCOUNT":
                window_operator = pydop.RELCOUNT
            case "RELSIZE":
                window_operator = pydop.RELSIZE
            case func_str:
                is_window = False
                operator = get_operator_by_name(func_str, **kwargs)
        if is_window:
            by: Iterable[UnqualifiedNode] = get_by_arg(kwargs, window_operator)
            if "per" in kwargs:
                per_arg = kwargs.pop("per")
                is_string.verify(per_arg, "`per` argument")
                per = per_arg
            return UnqualifiedWindow(
                window_operator,
                operands,
                by,
                per,
                kwargs,
            )
        return UnqualifiedOperation(operator, operands)


class UnqualifiedOperation(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to any operation done onto
    1+ expressions/collections.
    """

    def __init__(
        self,
        operation: pydop.ExpressionFunctionOperator,
        operands: list[UnqualifiedNode],
    ):
        self._parcel: tuple[pydop.ExpressionFunctionOperator, list[UnqualifiedNode]] = (
            operation,
            operands,
        )


class UnqualifiedWindow(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a WINDOW call.
    """

    def __init__(
        self,
        operator: pydop.ExpressionWindowOperator,
        arguments: Iterable[UnqualifiedNode],
        by: Iterable[UnqualifiedNode],
        per: str | None,
        kwargs: dict[str, object],
    ):
        self._parcel: tuple[
            pydop.ExpressionWindowOperator,
            Iterable[UnqualifiedNode],
            Iterable[UnqualifiedNode],
            str | None,
            dict[str, object],
        ] = (operator, arguments, by, per, kwargs)


class UnqualifiedBinaryOperation(UnqualifiedNode):
    """
    Variant of UnqualifiedOperation specifically for builtin Python binops.
    """

    def __init__(self, operator: str, lhs: UnqualifiedNode, rhs: UnqualifiedNode):
        self._parcel: tuple[str, UnqualifiedNode, UnqualifiedNode] = (
            operator,
            lhs,
            rhs,
        )


class UnqualifiedAccess(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to accessing a property
    from another UnqualifiedNode node.
    """

    def __init__(self, predecessor: UnqualifiedNode, name: str):
        self._parcel: tuple[UnqualifiedNode, str] = (predecessor, name)


class UnqualifiedCalculate(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a CALCULATE clause being
    done onto another UnqualifiedNode.
    """

    def __init__(
        self, predecessor: UnqualifiedNode, terms: list[tuple[str, UnqualifiedNode]]
    ):
        self._parcel: tuple[UnqualifiedNode, list[tuple[str, UnqualifiedNode]]] = (
            predecessor,
            terms,
        )


class UnqualifiedWhere(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a WHERE clause being
    done onto another UnqualifiedNode.
    """

    def __init__(self, predecessor: UnqualifiedNode, cond: UnqualifiedNode):
        self._parcel: tuple[UnqualifiedNode, UnqualifiedNode] = (predecessor, cond)


class UnqualifiedOrderBy(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a ORDER BY clause being
    done onto another UnqualifiedNode.
    """

    def __init__(self, predecessor: UnqualifiedNode, keys: list[UnqualifiedNode]):
        self._parcel: tuple[UnqualifiedNode, list[UnqualifiedNode]] = (
            predecessor,
            keys,
        )


class UnqualifiedTopK(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a TOP K clause being
    done onto another UnqualifiedNode.
    """

    def __init__(
        self,
        predecessor: UnqualifiedNode,
        k: int,
        keys: list[UnqualifiedNode] | None = None,
    ):
        self._parcel: tuple[UnqualifiedNode, int, list[UnqualifiedNode] | None] = (
            predecessor,
            k,
            keys,
        )


class UnqualifiedPartition(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a PARTITION clause.
    """

    def __init__(
        self,
        parent: UnqualifiedNode,
        name: str,
        keys: list[UnqualifiedNode],
    ):
        self._parcel: tuple[UnqualifiedNode, str, list[UnqualifiedNode]] = (
            parent,
            name,
            keys,
        )


class UnqualifiedSingular(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a SINGULAR clause.
    """

    def __init__(self, predecessor: UnqualifiedNode):
        self._parcel: tuple[UnqualifiedNode] = (predecessor,)


class UnqualifiedBest(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a BEST clause.
    """

    def __init__(
        self,
        data: UnqualifiedNode,
        by: Iterable[UnqualifiedNode],
        per: str | None,
        allow_ties: bool,
        n_best: int,
    ):
        self._parcel: tuple[
            UnqualifiedNode, Iterable[UnqualifiedNode], str | None, bool, int
        ] = (data, by, per, allow_ties, n_best)


def display_raw(unqualified: UnqualifiedNode) -> str:
    """
    Prints an unqualified node in a human-readable manner that shows its
    structure before qualification.

    Args:
        `unqualified`: the unqualified node being converted to a string.

    Returns:
        The string representation of the unqualified node.
    """
    term_strings: list[str] = []
    result: str
    operands_str: str
    match unqualified:
        case UnqualifiedRoot():
            return unqualified._parcel[0].name
        case UnqualifiedLiteral():
            literal_value: Any = unqualified._parcel[0]
            match literal_value:
                case list() | tuple():
                    return f"[{', '.join(display_raw(elem) for elem in literal_value)}]"
                case dict():
                    return (
                        "{"
                        + ", ".join(
                            f"{key}: {display_raw(value)}"
                            for key, value in literal_value.items()
                        )
                        + "}"
                    )
                case _:
                    return repr(literal_value)
        case UnqualifiedOperator():
            return unqualified._parcel[0]
        case UnqualifiedOperation():
            operands_str = ", ".join(
                [display_raw(operand) for operand in unqualified._parcel[1]]
            )
            return f"{unqualified._parcel[0].function_name}({operands_str})"
        case UnqualifiedWindow():
            operands_str = ""
            for operand in unqualified._parcel[1]:
                operands_str += f"{display_raw(operand)}, "
            operands_str += f"by=({', '.join([display_raw(operand) for operand in unqualified._parcel[2]])}"
            if unqualified._parcel[3] is not None:
                operands_str += f", per={unqualified._parcel[3]!r}"
            for kwarg, val in unqualified._parcel[4].items():
                operands_str += f", {kwarg}={val!r}"
            return f"{unqualified._parcel[0].function_name}({operands_str})"
        case UnqualifiedBinaryOperation():
            return f"({display_raw(unqualified._parcel[1])} {unqualified._parcel[0]} {display_raw(unqualified._parcel[2])})"
        case UnqualifiedCollation():
            method: str = "ASC" if unqualified._parcel[1] else "DESC"
            pos: str = "'last'" if unqualified._parcel[2] else "'first'"
            return f"{display_raw(unqualified._parcel[0])}.{method}(na_pos={pos})"
        case UnqualifiedAccess():
            if isinstance(unqualified._parcel[0], UnqualifiedRoot):
                return unqualified._parcel[1]
            return f"{display_raw(unqualified._parcel[0])}.{unqualified._parcel[1]}"
        case UnqualifiedCalculate():
            for name, node in unqualified._parcel[1]:
                term_strings.append(f"{name}={display_raw(node)}")
            return f"{display_raw(unqualified._parcel[0])}.CALCULATE({', '.join(term_strings)})"
        case UnqualifiedWhere():
            return f"{display_raw(unqualified._parcel[0])}.WHERE({display_raw(unqualified._parcel[1])})"
        case UnqualifiedTopK():
            if unqualified._parcel[2] is None:
                return f"{display_raw(unqualified._parcel[0])}.TOP_K({unqualified._parcel[1]})"
            for node in unqualified._parcel[2]:
                term_strings.append(display_raw(node))
            return f"{display_raw(unqualified._parcel[0])}.TOP_K({unqualified._parcel[1]}, by=({', '.join(term_strings)}))"
        case UnqualifiedOrderBy():
            for node in unqualified._parcel[1]:
                term_strings.append(display_raw(node))
            return f"{display_raw(unqualified._parcel[0])}.ORDER_BY({', '.join(term_strings)})"
        case UnqualifiedPartition():
            for node in unqualified._parcel[2]:
                term_strings.append(display_raw(node))
            result = f"PARTITION(name={unqualified._parcel[1]!r}, by=({', '.join(term_strings)}))"
            if not isinstance(unqualified._parcel[0], UnqualifiedRoot):
                result = f"{display_raw(unqualified._parcel[0])}.{result}"
            return result
        case UnqualifiedSingular():
            return f"{display_raw(unqualified._parcel[0])}.SINGULAR()"
        case UnqualifiedBest():
            result = f"{display_raw(unqualified._parcel[0])}.BEST("
            result += f"by=({', '.join(display_raw(node) for node in unqualified._parcel[1])})"
            if unqualified._parcel[2]:
                result += f", per={unqualified._parcel[3]!r}"
            if unqualified._parcel[3]:
                result += ", allow_ties=True"
            if unqualified._parcel[4] > 1:
                result += f", n_best={unqualified._parcel[4]}"
            return result + ")"
        case _:
            raise PyDoughUnqualifiedException(
                f"Unsupported unqualified node: {unqualified.__class__.__name__}"
            )
