"""
TODO: add file-level docstring
"""

__all__ = [
    "UnqualifiedAccess",
    "UnqualifiedBinaryOperation",
    "UnqualifiedCalc",
    "UnqualifiedNode",
    "UnqualifiedOperation",
    "UnqualifiedOperator",
    "UnqualifiedOrderBy",
    "UnqualifiedPartition",
    "UnqualifiedRoot",
    "UnqualifiedTopK",
    "UnqualifiedWhere",
    "UnqualifiedLiteral",
    "UnqualifiedBack",
]

from abc import ABC
from collections.abc import Iterable, MutableSequence
from datetime import date
from typing import Any, Union

from pydough.metadata import GraphMetadata
from pydough.pydough_ast import pydough_operators as pydop
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

    def __hash__(self):
        return hash(repr(self))

    def __getitem__(self, key):
        if isinstance(key, slice):
            args: MutableSequence[UnqualifiedNode] = [self]
            for arg in (key.start, key.stop, key.step):
                coerced_elem = UnqualifiedNode.coerce_to_unqualified(arg)
                args.append(coerced_elem)
            return UnqualifiedOperation("SLICE", args)
        else:
            raise PyDoughUnqualifiedException(
                f"Cannot index into PyDough object {self} with {key!r}"
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

    def __call__(self, *args, **kwargs: dict[str, object]):
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
        return UnqualifiedCalc(self, calc_args)

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

    def ASC(self, na_pos="last") -> "UnqualifiedCollation":
        assert na_pos in (
            "first",
            "last",
        ), f"Unrecognized `na_pos` value for `ASC`: {na_pos!r}"
        return UnqualifiedCollation(self, True, na_pos)

    def DESC(self, na_pos="last") -> "UnqualifiedCollation":
        assert na_pos in (
            "first",
            "last",
        ), f"Unrecognized `na_pos` value for `DESC`: {na_pos!r}"
        return UnqualifiedCollation(self, False, na_pos)

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

    def BACK(self, levels: int) -> "UnqualifiedBack":
        """
        Method used to create a BACK node.
        """
        return UnqualifiedBack(levels)


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

    def __repr__(self):
        return "?"


class UnqualifiedBack(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a BACK node, meaning that
    anything pointing to this node as an ancestor/predecessor must be derivable
    by looking at the ancestors of the context it is placed within.
    """

    def __init__(self, levels: int):
        self._parcel: tuple[int] = (levels,)

    def __repr__(self):
        return f"BACK({self._parcel[0]})"


class UnqualifiedLiteral(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a literal whose value is
    a Python operation.
    """

    def __init__(self, literal: object, typ: PyDoughType):
        self._parcel: tuple[object, PyDoughType] = (literal, typ)

    def __repr__(self):
        return f"{self._parcel[0]!r}"


class UnqualifiedCollation(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a collation expression.
    """

    def __init__(self, node: UnqualifiedNode, asc: bool, na_pos: bool):
        self._parcel: tuple[UnqualifiedNode, bool, bool] = (node, asc, na_pos)

    def __repr__(self):
        method = "ASC" if self._parcel[1] else "DESC"
        return f"{self._parcel[0]!r}.{method}(na_pos={self._parcel[2]!r})"


class UnqualifiedOperator(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a function that has
    yet to be called.
    """

    def __init__(self, name: str):
        self._parcel: tuple[str] = (name,)

    def __repr__(self):
        return self._parcel[0]

    def __call__(self, *args, **kwargs):
        assert (
            len(kwargs) == 0
        ), "PyDough function calls do not support keyword arguments at this time"
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

    def __repr__(self):
        operands_str: str = ", ".join([repr(operand) for operand in self._parcel[1]])
        return f"{self._parcel[0]}({operands_str})"


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

    def __repr__(self):
        return f"({self._parcel[1]!r} {self._parcel[0]} {self._parcel[2]!r})"


class UnqualifiedAccess(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to accessing a property
    from another UnqualifiedNode node.
    """

    def __init__(self, predecessor: UnqualifiedNode, name: str):
        self._parcel: tuple[UnqualifiedNode, str] = (predecessor, name)

    def __repr__(self):
        return f"{self._parcel[0]!r}.{self._parcel[1]}"


class UnqualifiedCalc(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a CALC clause being
    done onto another UnqualifiedNode.
    """

    def __init__(
        self, predecessor: UnqualifiedNode, terms: list[tuple[str, UnqualifiedNode]]
    ):
        self._parcel: tuple[UnqualifiedNode, list[tuple[str, UnqualifiedNode]]] = (
            predecessor,
            terms,
        )

    def __repr__(self):
        calc_strings: list[str] = []
        for name, node in self._parcel[1]:
            calc_strings.append(f"{name}={node!r}")
        return f"{self._parcel[0]!r}({', '.join(calc_strings)})"


class UnqualifiedWhere(UnqualifiedNode):
    """
    Implementation of UnqualifiedNode used to refer to a WHERE clause being
    done onto another UnqualifiedNode.
    """

    def __init__(self, predecessor: UnqualifiedNode, cond: UnqualifiedNode):
        self._parcel: tuple[UnqualifiedNode, UnqualifiedNode] = (predecessor, cond)

    def __repr__(self):
        return f"{self._parcel[0]!r}.WHERE({self._parcel[1]!r})"


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

    def __repr__(self):
        key_strings: list[str] = []
        for node in self._parcel[1]:
            key_strings.append(repr(node))
        return f"{self._parcel[0]!r}.ORDER_BY({', '.join(key_strings)})"


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

    def __repr__(self):
        if self._parcel[2] is None:
            return f"{self._parcel[0]!r}.TOP_K({self._parcel[1]})"
        key_strings: list[str] = []
        for node in self._parcel[2]:
            key_strings.append(f"{node!r}")
        return f"{self._parcel[0]!r}.TOP_K({self._parcel[1]}, by=({', '.join(key_strings)}))"


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

    def __repr__(self):
        key_strings: list[str] = []
        for node in self._parcel[3]:
            key_strings.append(f"{node!r}")
        return f"{self._parcel[0]!r}.PARTITION({self._parcel[1]!r}, name={self._parcel[2]!r}, by=({', '.join(key_strings)}))"
