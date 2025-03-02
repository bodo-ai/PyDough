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
from collections.abc import Iterable, MutableSequence, Sequence
from datetime import date
from typing import Any, Union

import pydough.pydough_operators as pydop
from pydough.metadata import GraphMetadata
from pydough.metadata.errors import is_bool, is_positive_int
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
                assert isinstance(
                    coerced_elem, UnqualifiedLiteral
                ), f"Can only coerce list of literals to a literal, not {elem}"
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
            args: MutableSequence[UnqualifiedNode] = [self]
            for arg in (key.start, key.stop, key.step):
                coerced_elem = UnqualifiedNode.coerce_to_unqualified(arg)
                if not isinstance(coerced_elem, UnqualifiedLiteral):
                    raise PyDoughUnqualifiedException(
                        "PyDough objects are currently not supported to be used as indices in Python slices."
                    )
                args.append(coerced_elem)
            return UnqualifiedOperation("SLICE", args)
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
        return UnqualifiedOperation("NOT", [self])

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
        return UnqualifiedOperation("ABS", [self])

    def __round__(self, n=None):
        if n is None:
            n = 0
        n_unqualified = self.coerce_to_unqualified(n)
        return UnqualifiedOperation("ROUND", [self, n_unqualified])

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
        keys_unqualified: MutableSequence[UnqualifiedNode] = [
            self.coerce_to_unqualified(key) for key in keys
        ]
        return UnqualifiedOrderBy(self, keys_unqualified)

    def TOP_K(
        self, k: int, by: object | Iterable[object] | None = None
    ) -> "UnqualifiedTopK":
        if by is None:
            return UnqualifiedTopK(self, k, None)
        else:
            keys_unqualified: MutableSequence[UnqualifiedNode]
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
        data: "UnqualifiedNode",
        name: str,
        by: Union[Iterable["UnqualifiedNode"], "UnqualifiedNode"],
    ) -> "UnqualifiedPartition":
        """
        Method used to create a PARTITION node.
        """
        if isinstance(by, UnqualifiedNode):
            return UnqualifiedPartition(self, data, name, [by])
        else:
            return UnqualifiedPartition(self, data, name, list(by))


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
    func_name: str,
) -> Sequence[UnqualifiedNode]:
    """
    Extracts the `by` argument from the keyword arguments to a window function,
    verifying that it exists, removing it from the kwargs, and converting it to
    an iterable if it was a single unqualified node.

    Args:
        `kwargs`: the keyword arguments.
        `func_name`: the function whose `by` argument being extracted.

    Returns:
        The list of unqualified nodes represented by the `by` argument, which
        is removed from `kwargs`.

    Raises:
        `PyDoughUnqualifiedException` if the `by` argument is missing or the
        wrong type.
    """
    if "by" not in kwargs:
        raise PyDoughUnqualifiedException(
            f"The `by` argument to `{func_name}` must be provided"
        )
    by = kwargs.pop("by")
    if isinstance(by, UnqualifiedCollation):
        by = [by]
    elif not (
        isinstance(by, Sequence)
        and all(isinstance(arg, UnqualifiedCollation) for arg in by)
        and len(by) > 0
    ):
        raise PyDoughUnqualifiedException(
            f"The `by` argument to `{func_name}` must be a single collation expression or a non-empty iterable of collation expressions"
        )
    return by


class UnqualifiedOperator(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a function that has
    yet to be called.
    """

    def __init__(self, name: str):
        self._parcel: tuple[str] = (name,)

    def __call__(self, *args, **kwargs):
        levels: int | None = None
        window_operator: pydop.ExpressionWindowOperator
        is_window: bool = True
        match self._parcel[0]:
            case "PERCENTILE":
                window_operator = pydop.PERCENTILE
                is_positive_int.verify(
                    kwargs.get("n_buckets", 100), "`n_buckets` argument"
                )
            case "RANKING":
                window_operator = pydop.RANKING
                is_bool.verify(kwargs.get("allow_ties", False), "`allow_ties` argument")
                is_bool.verify(kwargs.get("dense", False), "`dense` argument")
            case func:
                is_window = False
                if len(kwargs) > 0:
                    raise PyDoughUnqualifiedException(
                        f"PyDough function call {func} does not support keyword arguments at this time"
                    )
        if is_window:
            by: Iterable[UnqualifiedNode] = get_by_arg(kwargs, self._parcel[0])
            if "levels" in kwargs:
                levels_arg = kwargs.pop("levels")
                is_positive_int.verify(levels_arg, "`levels` argument")
                levels = levels_arg
            return UnqualifiedWindow(
                window_operator,
                by,
                levels,
                kwargs,
            )
        operands: MutableSequence[UnqualifiedNode] = []
        for arg in args:
            operands.append(self.coerce_to_unqualified(arg))
        return UnqualifiedOperation(self._parcel[0], operands)


class UnqualifiedOperation(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to any operation done onto
    1+ expressions/collections.
    """

    def __init__(self, operation_name: str, operands: MutableSequence[UnqualifiedNode]):
        self._parcel: tuple[str, MutableSequence[UnqualifiedNode]] = (
            operation_name,
            operands,
        )


class UnqualifiedWindow(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a WINDOW call.
    """

    def __init__(
        self,
        operator: pydop.ExpressionWindowOperator,
        by: Iterable[UnqualifiedNode],
        levels: int | None,
        kwargs: dict[str, object],
    ):
        self._parcel: tuple[
            pydop.ExpressionWindowOperator,
            Iterable[UnqualifiedNode],
            int | None,
            dict[str, object],
        ] = (operator, by, levels, kwargs)


class UnqualifiedBinaryOperation(UnqualifiedNode):
    """
    Variant of UnqualifiedOperation specifically for builtin Python binops.
    """

    def __init__(self, operation_name: str, lhs: UnqualifiedNode, rhs: UnqualifiedNode):
        self._parcel: tuple[str, UnqualifiedNode, UnqualifiedNode] = (
            operation_name,
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

    def __init__(
        self, predecessor: UnqualifiedNode, keys: MutableSequence[UnqualifiedNode]
    ):
        self._parcel: tuple[UnqualifiedNode, MutableSequence[UnqualifiedNode]] = (
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
        keys: MutableSequence[UnqualifiedNode] | None = None,
    ):
        self._parcel: tuple[
            UnqualifiedNode, int, MutableSequence[UnqualifiedNode] | None
        ] = (
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
        data: UnqualifiedNode,
        name: str,
        keys: MutableSequence[UnqualifiedNode],
    ):
        self._parcel: tuple[
            UnqualifiedNode, UnqualifiedNode, str, MutableSequence[UnqualifiedNode]
        ] = (
            parent,
            data,
            name,
            keys,
        )


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
            return f"{unqualified._parcel[0]}({operands_str})"
        case UnqualifiedWindow():
            operands_str = f'by=({", ".join([display_raw(operand) for operand in unqualified._parcel[1]])}'
            if unqualified._parcel[2] is not None:
                operands_str += ", levels=" + str(unqualified._parcel[2])
            for kwarg, val in unqualified._parcel[3].items():
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
            for node in unqualified._parcel[3]:
                term_strings.append(display_raw(node))
            if isinstance(unqualified._parcel[0], UnqualifiedRoot):
                return f"PARTITION({display_raw(unqualified._parcel[1])}, name={unqualified._parcel[2]!r}, by=({', '.join(term_strings)}))"
            return f"{display_raw(unqualified._parcel[0])}.PARTITION({display_raw(unqualified._parcel[1])}, name={unqualified._parcel[2]!r}, by=({', '.join(term_strings)}))"
        case _:
            raise PyDoughUnqualifiedException(
                f"Unsupported unqualified node: {unqualified.__class__.__name__}"
            )
