"""
TODO: add file-level docstring
"""

__all__ = [
    "HybridExpr",
    "HybridCollation",
    "HybridColumnExpr",
    "HybridRefExpr",
    "HybridBackRefExpr",
    "HybridChildRefExpr",
    "HybridLiteralExpr",
    "HybridFunctionExpr",
    "HybridOperation",
    "HybridRoot",
    "HybridCollectionAccess",
    "HybridFilter",
    "HybridCalc",
    "HybridOrder",
    "HybridLimit",
    "HybridTree",
    "make_hybrid_tree",
]


from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

import pydough.pydough_ast.pydough_operators as pydop
from pydough.pydough_ast import (
    Calc,
    CollectionAccess,
    ColumnProperty,
    CompoundSubCollection,
    GlobalContext,
    Literal,
    OrderBy,
    PyDoughCollectionAST,
    PyDoughExpressionAST,
    Reference,
    SubCollection,
    TableCollection,
    TopK,
    Where,
)
from pydough.types import PyDoughType


class HybridExpr(ABC):
    """
    TODO: add class docstring
    """

    def __init__(self, typ: PyDoughType):
        self.typ: PyDoughType = typ

    def __eq__(self, other):
        return type(self) is type(other) and repr(self) == repr(other)

    def __hash__(self):
        return hash(repr(self))

    @abstractmethod
    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        """
        TODO: add function docstring
        """


class HybridCollation(HybridExpr):
    """
    TODO: add class docstring
    """

    def __init__(self, expr: HybridExpr, asc: bool, na_first: bool):
        self.expr: HybridExpr = expr
        self.asc: bool = asc
        self.na_first: bool = na_first

    def __repr__(self):
        suffix: str = (
            f"{'asc' if self.asc else 'desc'}_{'first' if self.na_first else 'last'}"
        )
        return f"({self.expr!r}):{suffix}"

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        return HybridCollation(
            self.expr.apply_renamings(renamings), self.asc, self.na_first
        )


class HybridColumnExpr(HybridExpr):
    """
    TODO: add class docstring
    """

    def __init__(self, column: ColumnProperty):
        super().__init__(column.pydough_type)
        self.column: ColumnProperty = column

    def __repr__(self):
        return repr(self.column)

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        return self


class HybridRefExpr(HybridExpr):
    """
    TODO: add class docstring
    """

    def __init__(self, name: str, typ: PyDoughType):
        super().__init__(typ)
        self.name: str = name

    def __repr__(self):
        return self.name

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        if self.name in renamings:
            return HybridRefExpr(renamings[self.name], self.typ)
        return self


class HybridChildRefExpr(HybridExpr):
    """
    TODO: add class docstring
    """

    def __init__(self, name: str, child_idx: int, typ: PyDoughType):
        super().__init__(typ)
        self.name: str = name
        self.child_idx: int = child_idx

    def __repr__(self):
        return f"${self.child_idx}.{self.name}"

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        return self


class HybridBackRefExpr(HybridExpr):
    """
    TODO: add class docstring
    """

    def __init__(self, name: str, back_idx: int, typ: PyDoughType):
        super().__init__(typ)
        self.name: str = name
        self.back_idx: int = back_idx

    def __repr__(self):
        return f"BACK({self.back_idx}).{self.name}"

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        return self


class HybridLiteralExpr(HybridExpr):
    """
    TODO: add class docstring
    """

    def __init__(self, literal: Literal):
        super().__init__(literal.pydough_type)
        self.literal: Literal = literal

    def __repr__(self):
        return repr(self.literal)

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        return self


class HybridFunctionExpr(HybridExpr):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        operator: pydop.PyDoughOperatorAST,
        args: list[HybridExpr],
        typ: PyDoughType,
    ):
        super().__init__(typ)
        self.operator: pydop.PyDoughOperatorAST = operator
        self.args: list[HybridExpr] = args

    def __repr__(self):
        arg_strings: list[str] = [
            f"({arg!r})"
            if isinstance(self.operator, pydop.BinaryOperator)
            and isinstance(arg, HybridFunctionExpr)
            and isinstance(arg.operator, pydop.BinaryOperator)
            else repr(arg)
            for arg in self.args
        ]
        return self.operator.to_string(arg_strings)

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        return HybridFunctionExpr(
            self.operator,
            [arg.apply_renamings(renamings) for arg in self.args],
            self.typ,
        )


class HybridOperation:
    """
    TODO: add class docstring
    """

    def __init__(self, terms: dict[str, HybridExpr], renamings: dict[str, str]):
        self.terms: dict[str, HybridExpr] = terms
        self.renamings: dict[str, str] = renamings


class HybridRoot(HybridOperation):
    """
    TODO: add class docstring
    """

    def __init__(self):
        super().__init__({}, {})

    def __repr__(self):
        return "ROOT"


class HybridCollectionAccess(HybridOperation):
    """
    TODO: add class docstring
    """

    def __init__(self, collection: CollectionAccess):
        self.collection: CollectionAccess = collection
        terms: dict[str, HybridExpr] = {}
        for name in collection.calc_terms:
            expr = collection.get_expr(name)
            assert isinstance(expr, ColumnProperty)
            terms[name] = HybridColumnExpr(expr)
        super().__init__(terms, {})

    def __repr__(self):
        return f"COLLECTION[{self.collection.collection.name}]"


class HybridCalc(HybridOperation):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        predecessor: HybridOperation,
        calc: Calc,
        new_expressions: dict[str, HybridExpr],
    ):
        terms: dict[str, HybridExpr] = {}
        renamings: dict[str, str] = {}
        for name, expr in predecessor.terms.items():
            terms[name] = HybridRefExpr(name, expr.typ)
        renamings.update(predecessor.renamings)
        for name, expr in new_expressions.items():
            if name in terms and terms[name] == expr:
                continue
            expr = expr.apply_renamings(predecessor.renamings)
            used_name: str = name
            idx: int = 0
            while used_name in terms or used_name in renamings:
                used_name = f"{name}_{idx}"
                idx += 1
            terms[used_name] = expr
            renamings[name] = used_name
        super().__init__(terms, renamings)
        self.calc = Calc
        self.new_expressions = new_expressions

    def __repr__(self):
        return f"CALC[{self.new_expressions}]"


class HybridFilter(HybridOperation):
    """
    TODO: add class docstring
    """

    def __init__(
        self, predecessor: HybridOperation, where: Where, condition: HybridExpr
    ):
        super().__init__(predecessor.terms, {})
        self.predecessor: HybridOperation = predecessor
        self.where: Where = where
        self.condition: HybridExpr = condition

    def __repr__(self):
        return f"FILTER[{self.condition}]"


class HybridOrder(HybridOperation):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        predecessor: HybridOperation,
        order: OrderBy,
        collation: list[HybridCollation],
    ):
        super().__init__(predecessor.terms, {})
        self.predecessor: HybridOperation = predecessor
        self.order: OrderBy = order
        self.collation: list[HybridCollation] = collation

    def __repr__(self):
        return f"ORDER[{self.collation}]"


class HybridLimit(HybridOperation):
    """
    TODO: add class docstring
    """

    def __init__(
        self, predecessor: HybridOperation, topk: TopK, collation: list[HybridCollation]
    ):
        super().__init__(predecessor.terms, {})
        self.predecessor: HybridOperation = predecessor
        self.topk: TopK = topk
        self.collation: list[HybridCollation] = collation

    def __repr__(self):
        return f"LIMIT_{self.topk.records_to_keep}[{self.collation}]"


@dataclass
class HybridConnection:
    parent: "HybridTree"
    subtree: "HybridTree"
    is_singular: bool = True
    is_aggregation: bool = False
    is_count: bool = False
    is_ndistinct: bool = False
    is_has: bool = False
    is_hasnot: bool = False


class HybridTree:
    def __init__(
        self,
        root_operation: HybridOperation,
        is_hidden_level: bool = False,
        is_connection_root: bool = False,
    ):
        self._pipeline: list[HybridOperation] = [root_operation]
        self._children: list[HybridConnection] = []
        self._successor: HybridTree | None = None
        self._parent: HybridTree | None = None
        self._is_hidden_level: bool = is_hidden_level
        self._is_connection_root: bool = is_connection_root

    def __repr__(self):
        lines = []
        lines.append(" -> ".join(repr(operation) for operation in self.pipeline))
        prefix = " " if self.successor is None else "↓"
        for idx, child in enumerate(self.children):
            lines.append(
                f"{prefix} child #{idx} ({'agg' if child.is_aggregation else 'sing'}):"
            )
            for line in repr(child.subtree).splitlines():
                lines.append(f"{prefix} {line}")
        if self.successor is not None:
            lines.extend(repr(self.successor).splitlines())
        return "\n".join(lines)

    @property
    def pipeline(self) -> list[HybridOperation]:
        """
        TODO
        """
        return self._pipeline

    @property
    def children(self) -> list[HybridConnection]:
        """
        TODO
        """
        return self._children

    @property
    def successor(self) -> Optional["HybridTree"]:
        """
        TODO
        """
        return self._successor

    @property
    def parent(self) -> Optional["HybridTree"]:
        """
        TODO
        """
        return self._parent

    @property
    def is_hidden_level(self) -> bool:
        """
        TODO
        """
        return self._is_hidden_level

    @property
    def is_connection_root(self) -> bool:
        """
        TODO
        """
        return self._is_connection_root

    def add_child(
        self,
        child: "HybridTree",
        is_singular: bool = True,
        is_aggregation: bool = False,
        is_count: bool = False,
        is_ndistinct: bool = False,
        is_has: bool = False,
        is_hasnot: bool = False,
    ) -> int:
        """
        TODO
        """
        connection: HybridConnection = HybridConnection(
            self,
            child,
            is_singular,
            is_aggregation,
            is_count,
            is_ndistinct,
            is_has,
            is_hasnot,
        )
        for idx, existing_child in enumerate(self.children):
            if child == existing_child:
                return idx
        self._children.append(connection)
        child._parent = self
        return len(self.children) - 1

    def add_successor(self, successor: "HybridTree") -> None:
        """
        TODO
        """
        if self._successor is not None:
            raise Exception("Duplicate successor")
        self._successor = successor
        successor._parent = self


def make_hybrid_expr(hybrid: HybridTree, expr: PyDoughExpressionAST) -> HybridExpr:
    """
    TODO: Add function docstring
    """
    match expr:
        case Literal():
            return HybridLiteralExpr(expr)
        case ColumnProperty():
            return HybridColumnExpr(expr)
        case Reference():
            return HybridRefExpr(expr.term_name, expr.pydough_type)
        case _:
            raise NotImplementedError(
                f"TODO: support converting {expr.__class__.__name__}"
            )


def make_hybrid_tree(node: PyDoughCollectionAST) -> HybridTree:
    """
    TODO: Add function docstring
    """
    hybrid: HybridTree
    successor_hybrid: HybridTree
    expr: HybridExpr
    match node:
        case GlobalContext():
            return HybridTree(HybridRoot())
        case CompoundSubCollection():
            raise NotImplementedError(f"{node.__class__.__name__}")
        case TableCollection() | SubCollection():
            hybrid = make_hybrid_tree(node.ancestor_context)
            successor_hybrid = HybridTree(HybridCollectionAccess(node))
            hybrid.add_successor(successor_hybrid)
            return successor_hybrid
        case Calc():
            hybrid = make_hybrid_tree(node.preceding_context)
            new_expressions: dict[str, HybridExpr] = {}
            for name in node.calc_terms:
                expr = make_hybrid_expr(hybrid, node.get_expr(name))
                new_expressions[name] = expr
            hybrid.pipeline.append(
                HybridCalc(hybrid.pipeline[-1], node, new_expressions)
            )
            return hybrid
        case Where():
            hybrid = make_hybrid_tree(node.preceding_context)
            expr = make_hybrid_expr(hybrid, node.condition)
            hybrid.pipeline.append(HybridFilter(hybrid.pipeline[-1], node, expr))
            return hybrid
        case _:
            raise NotImplementedError(f"{node.__class__.__name__}")
