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
from enum import Enum
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
    The base class for expression nodes within a hybrid operation.
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
        Renames references in an expression if contained in a renaming
        dictionary.

        Args:
            `renamings`: a dictionary mapping names of any references to the
            new name that they should adopt.

        Returns:
            The transformed copy of self, if necessary, otherwise
            just returns self.
        """

    def make_into_ref(self, name: str) -> "HybridRefExpr":
        """
        Converts a HybridExpr into a reference with the desired name.

        Args:
            `name`: the name of the desired reference.

        Returns:
            A HybridRefExpr corresponding to `self` but with the provided name,
            or just `self` if `self` is already a HybridRefExpr with that name.
        """
        if isinstance(self, HybridRefExpr) and self.name == name:
            return self
        return HybridRefExpr(name, self.typ)


class HybridCollation(HybridExpr):
    """
    Class for HybridExpr terms that are another HybridExpr term wrapped in
    information about how to sort by them.
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
        renamed_expr: HybridExpr = self.expr.apply_renamings(renamings)
        if renamed_expr is self.expr:
            return self
        return HybridCollation(renamed_expr, self.asc, self.na_first)


class HybridColumnExpr(HybridExpr):
    """
    Class for HybridExpr terms that are references to a column from a table.
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
    Class for HybridExpr terms that are references to a term from a preceding
    HybridOperation.
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
    Class for HybridExpr terms that are references to a term from a child
    operation.
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
    Class for HybridExpr terms that are references to a term from an
    ancestor operation.
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
    Class for HybridExpr terms that are literals.
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
    Class for HybridExpr terms that are function calls.
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
        renamed_args: list[HybridExpr] = [
            arg.apply_renamings(renamings) for arg in self.args
        ]
        if all(
            expr is renamed_expr for expr, renamed_expr in zip(self.args, renamed_args)
        ):
            return self
        return HybridFunctionExpr(self.operator, renamed_args, self.typ)


class HybridOperation:
    """
    Base class for an operation done within a pipeline of a HybridTree, such
    as a filter or table collection access. Every such class contains the
    following:
    - `terms`: mapping of names to expressions accessible from that point in
               the pipeline execution.
    - `renamings`: mapping of names to a new name that should be used to access
               them from within `terms`. This is used when a `CALC` overrides a
               term name so that future invocations of the term name use the
               renamed version, while key operations like joins can still
               access the original version.
    """

    def __init__(self, terms: dict[str, HybridExpr], renamings: dict[str, str]):
        self.terms: dict[str, HybridExpr] = terms
        self.renamings: dict[str, str] = renamings


class HybridRoot(HybridOperation):
    """
    Class for HybridOperation corresponding to the "root" context.
    """

    def __init__(self):
        super().__init__({}, {})

    def __repr__(self):
        return "ROOT"


class HybridCollectionAccess(HybridOperation):
    """
    Class for HybridOperation corresponding to accessing a collection (either
    directly or as a subcollection).
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
    Class for HybridOperation corresponding to a CALC operation.
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
    Class for HybridOperation corresponding to a WHERE operation.
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
    Class for HybridOperation corresponding to an ORDER BY operation.
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
    Class for HybridOperation corresponding to a TOP K operation.
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


class ConnectionType(Enum):
    """
    An enum describing how a hybrid tree is connected to a child tree.
    """

    SINGULAR = 0
    """
    The child should be 1:1 with regards to the parent, and can thus be
    accessed via a simple left-join without having to worry about cardinality
    contamination.
    """

    AGGREGATION = 1
    """
    The child is being accessed for the purposes of aggregating its columns.
    """

    COUNT = 2
    """
    The child is being accessed for the purposes of counting how many rows it
    has.
    """

    NDISTINCT = 3
    """
    The child is being accessed for the purposes of counting how many
    distinct elements it has.
    """

    HAS = 4
    """
    The child is being used as a semi-join.
    """

    HASNOT = 5
    """
    The child is being used as an anti-join.
    """


@dataclass
class HybridConnection:
    """
    Parcel class corresponding to information about one of the children
    of a HybridTree. Contains the following information:
    - `parent`: the HybridTree that the connection exists within.
    - `subtree`: the HybridTree corresponding to the child itself, starting
      from the bottom.
    - `connection_type`: an enum indicating which connection type is being
       used.
    """

    parent: "HybridTree"
    subtree: "HybridTree"
    connection_type: ConnectionType


class HybridTree:
    """
    The datastructure class used to keep track of the overall computation in
    a tree structure where each level has a pipeline of operations, possibly
    has a singular predecessor and/or successor, and can have children that
    the operations in the pipeline can access.
    """

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
            lines.append(f"{prefix} child #{idx} ({child.connection_type}):")
            for line in repr(child.subtree).splitlines():
                lines.append(f"{prefix} {line}")
        if self.successor is not None:
            lines.extend(repr(self.successor).splitlines())
        return "\n".join(lines)

    @property
    def pipeline(self) -> list[HybridOperation]:
        """
        The sequence of operations done in the current level of the hybrid
        tree.
        """
        return self._pipeline

    @property
    def children(self) -> list[HybridConnection]:
        """
        The child operations evaluated so that they can be used by operations
        in the pipeline.
        """
        return self._children

    @property
    def successor(self) -> Optional["HybridTree"]:
        """
        The next level below in the HybridTree, if present.
        """
        return self._successor

    @property
    def parent(self) -> Optional["HybridTree"]:
        """
        The previous level above in the HybridTree, if present.
        """
        return self._parent

    @property
    def is_hidden_level(self) -> bool:
        """
        True if the current level should be disregarded when converting
        PyDoughAST BACK terms to HybridExpr BACK terms.
        """
        return self._is_hidden_level

    @property
    def is_connection_root(self) -> bool:
        """
        True if the current level is the top of a subtree located inside of
        a HybridConnection.
        """
        return self._is_connection_root

    def add_child(
        self,
        child: "HybridTree",
        connection_type: ConnectionType,
    ) -> int:
        """
        Adds a new child operation to the current level so that operations in
        the pipeline can make use of it.

        Args:
            `child`: the subtree to be connected to `self` as a child
            (starting at the bottom of the subtree).
            `connection_type`: enum indcating what kind of connection is to be
            used to link `self` to `child`.
        """
        connection: HybridConnection = HybridConnection(
            self,
            child,
            connection_type,
        )
        for idx, existing_child in enumerate(self.children):
            if child == existing_child:
                return idx
        self._children.append(connection)
        child._parent = self
        return len(self.children) - 1

    def add_successor(self, successor: "HybridTree") -> None:
        """
        Marks two hybrid trees in a predecessor-successor relationship.

        Args:
            `successor`: the HybridTree to be marked as one level below `self`.
        """
        if self._successor is not None:
            raise Exception("Duplicate successor")
        self._successor = successor
        successor._parent = self


def make_hybrid_expr(hybrid: HybridTree, expr: PyDoughExpressionAST) -> HybridExpr:
    """
    Converts an AST expression into a HybridExpr. Currently only supports
    literals, tale columns, and references.

    Args:
        `hybrid`: the hybrid tree that should be used to derive the translation
        of `expr`, as it is the context in which the `expr` will live.
        `expr`: the AST expression to be converted.

    Returns:
        The HybridExpr node corresponding to `expr`
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
    Converts a collection AST into the HybridTree format.

    Args:
        `node`: the collection AST to be converted.

    Returns:
        The HybridTree representation of `node`.
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
