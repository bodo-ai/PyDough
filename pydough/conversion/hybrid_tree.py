"""
The definitions of the hybrid classes used as an intermediary representation
during QDAG to Relational conversion, as well as the conversion logic from QDAG
nodes to said hybrid nodes.
"""

__all__ = [
    "HybridBackRefExpr",
    "HybridCalc",
    "HybridChildRefExpr",
    "HybridCollation",
    "HybridCollectionAccess",
    "HybridColumnExpr",
    "HybridExpr",
    "HybridFilter",
    "HybridFunctionExpr",
    "HybridLimit",
    "HybridLiteralExpr",
    "HybridOperation",
    "HybridPartition",
    "HybridPartitionChild",
    "HybridRefExpr",
    "HybridRoot",
    "HybridTranslator",
    "HybridTree",
]

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

import pydough.pydough_operators as pydop
from pydough.configs import PyDoughConfigs
from pydough.metadata import (
    CartesianProductMetadata,
    SimpleJoinMetadata,
    SubcollectionRelationshipMetadata,
)
from pydough.qdag import (
    BackReferenceExpression,
    Calc,
    ChildOperator,
    ChildOperatorChildAccess,
    ChildReferenceCollection,
    ChildReferenceExpression,
    CollationExpression,
    CollectionAccess,
    ColumnProperty,
    CompoundSubCollection,
    ExpressionFunctionCall,
    GlobalContext,
    Literal,
    OrderBy,
    PartitionBy,
    PartitionChild,
    PartitionKey,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    Reference,
    SubCollection,
    TableCollection,
    TopK,
    Where,
    WindowCall,
)
from pydough.relational import JoinType
from pydough.types import BooleanType, Int64Type, PyDoughType


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

    @abstractmethod
    def shift_back(self, levels: int) -> Optional["HybridExpr"]:
        """
        Promotes a HybridRefExpr into a HybridBackRefExpr with the specified
        number of levels, or increases the number of levels of a
        HybridBackRefExpr by the specified number of levels. Returns None if
        the expression cannot be shifted back (e.g. a child reference).

        Args:
            `levels`: the amount of back levels to increase by.

        Returns:
            The transformed HybridBackRefExpr.
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


class HybridCollation:
    """
    Class for HybridExpr terms that are another HybridExpr term wrapped in
    information about how to sort by them.
    """

    def __init__(self, expr: "HybridExpr", asc: bool, na_first: bool):
        self.expr: HybridExpr = expr
        self.asc: bool = asc
        self.na_first: bool = na_first

    def __repr__(self):
        suffix: str = (
            f"{'asc' if self.asc else 'desc'}_{'first' if self.na_first else 'last'}"
        )
        return f"({self.expr!r}):{suffix}"


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

    def shift_back(self, levels: int) -> HybridExpr | None:
        return HybridBackRefExpr(self.column.column_property.name, levels, self.typ)


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

    def shift_back(self, levels: int) -> HybridExpr | None:
        return HybridBackRefExpr(self.name, levels, self.typ)


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

    def shift_back(self, levels: int) -> HybridExpr | None:
        return None


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

    def shift_back(self, levels: int) -> HybridExpr | None:
        return HybridBackRefExpr(self.name, self.back_idx + levels, self.typ)


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

    def shift_back(self, levels: int) -> HybridExpr | None:
        return None


class HybridFunctionExpr(HybridExpr):
    """
    Class for HybridExpr terms that are function calls.
    """

    def __init__(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[HybridExpr],
        typ: PyDoughType,
    ):
        super().__init__(typ)
        self.operator: pydop.PyDoughExpressionOperator = operator
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
        if all_same(self.args, renamed_args):
            return self
        return HybridFunctionExpr(self.operator, renamed_args, self.typ)

    def shift_back(self, levels: int) -> HybridExpr | None:
        return None


class HybridWindowExpr(HybridExpr):
    """
    Class for HybridExpr terms that are window function calls.
    """

    def __init__(
        self,
        window_func: pydop.ExpressionWindowOperator,
        args: list[HybridExpr],
        partition_args: list[HybridExpr],
        order_args: list[HybridCollation],
        typ: PyDoughType,
        kwargs: dict[str, object],
    ):
        super().__init__(typ)
        self.window_func: pydop.ExpressionWindowOperator = window_func
        self.args: list[HybridExpr] = args
        self.partition_args: list[HybridExpr] = partition_args
        self.order_args: list[HybridCollation] = order_args
        self.kwargs: dict[str, object] = kwargs

    def __repr__(self):
        args_str = ""
        args_str += f"by=[{', '.join([str(arg) for arg in self.args])}]"
        args_str += (
            f", partition=[{', '.join([str(arg) for arg in self.partition_args])}]"
        )
        args_str += f", order=[{', '.join([str(arg) for arg in self.order_args])}]"
        if "allow_ties" in self.kwargs:
            args_str += f", allow_ties={self.kwargs['allow_ties']}"
            if "dense" in self.kwargs:
                args_str += f", dense={self.kwargs['dense']}"
        return f"{self.window_func.function_name}({args_str})"

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        renamed_args: list[HybridExpr] = [
            arg.apply_renamings(renamings) for arg in self.args
        ]
        renamed_partition_args: list[HybridExpr] = [
            arg.apply_renamings(renamings) for arg in self.partition_args
        ]
        renamed_order_args: list[HybridCollation] = []
        for col_arg in self.order_args:
            collation_expr: HybridExpr = col_arg.expr
            renamed_expr: HybridExpr = collation_expr.apply_renamings(renamings)
            if renamed_expr is collation_expr:
                renamed_order_args.append(col_arg)
            else:
                renamed_order_args.append(
                    HybridCollation(renamed_expr, col_arg.asc, col_arg.na_first)
                )
        if (
            all_same(self.args, renamed_args)
            and all_same(self.partition_args, renamed_partition_args)
            and all_same(
                [arg.expr for arg in self.order_args],
                [arg.expr for arg in renamed_order_args],
            )
        ):
            return self
        return HybridWindowExpr(
            self.window_func,
            renamed_args,
            renamed_partition_args,
            renamed_order_args,
            self.typ,
            self.kwargs,
        )

    def shift_back(self, levels: int) -> HybridExpr | None:
        return None


def all_same(exprs: list[HybridExpr], renamed_exprs: list[HybridExpr]) -> bool:
    """
    Returns whether two lists of hybrid expressions are identical, down to
    identity.
    """
    return len(exprs) == len(renamed_exprs) and all(
        expr is renamed_expr for expr, renamed_expr in zip(exprs, renamed_exprs)
    )


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
    - `orderings`: list of collation expressions that specify the order
               that a hybrid operation is sorted by.
    - `unique_exprs`: list of expressions that are used to uniquely identify
               records within the current level of the hybrid tree.
    """

    def __init__(
        self,
        terms: dict[str, HybridExpr],
        renamings: dict[str, str],
        orderings: list[HybridCollation],
        unique_exprs: list[HybridExpr],
    ):
        self.terms: dict[str, HybridExpr] = terms
        self.renamings: dict[str, str] = renamings
        self.orderings: list[HybridCollation] = orderings
        self.unique_exprs: list[HybridExpr] = unique_exprs


class HybridRoot(HybridOperation):
    """
    Class for HybridOperation corresponding to the "root" context.
    """

    def __init__(self):
        super().__init__({}, {}, [], [])

    def __repr__(self):
        return "ROOT"


class HybridCollectionAccess(HybridOperation):
    """
    Class for HybridOperation corresponding to accessing a collection (either
    directly or as a subcollection).
    """

    def __init__(self, collection: CollectionAccess):
        expr: PyDoughExpressionQDAG
        self.collection: CollectionAccess = collection
        terms: dict[str, HybridExpr] = {}
        for name in collection.calc_terms:
            expr = collection.get_expr(name)
            assert isinstance(expr, ColumnProperty)
            terms[name] = HybridColumnExpr(expr)
        unique_exprs: list[HybridExpr] = []
        for name in collection.unique_terms:
            expr = collection.get_expr(name)
            unique_exprs.append(HybridRefExpr(name, expr.pydough_type))
        super().__init__(terms, {}, [], unique_exprs)

    def __repr__(self):
        return f"COLLECTION[{self.collection.collection.name}]"


class HybridPartitionChild(HybridOperation):
    """
    Class for HybridOperation corresponding to accessing the data of a
    PARTITION as a child.
    """

    def __init__(self, subtree: "HybridTree"):
        self.subtree: HybridTree = subtree
        super().__init__(
            subtree.pipeline[-1].terms,
            subtree.pipeline[-1].renamings,
            subtree.pipeline[-1].orderings,
            subtree.pipeline[-1].unique_exprs,
        )

    def __repr__(self):
        return "PARTITION_CHILD[*]"


class HybridCalc(HybridOperation):
    """
    Class for HybridOperation corresponding to a CALC operation.
    """

    def __init__(
        self,
        predecessor: HybridOperation,
        new_expressions: dict[str, HybridExpr],
        orderings: list[HybridCollation],
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
        super().__init__(terms, renamings, orderings, predecessor.unique_exprs)
        self.calc = Calc
        self.new_expressions = new_expressions

    def __repr__(self):
        return f"CALC[{self.new_expressions}]"


class HybridFilter(HybridOperation):
    """
    Class for HybridOperation corresponding to a WHERE operation.
    """

    def __init__(self, predecessor: HybridOperation, condition: HybridExpr):
        super().__init__(
            predecessor.terms, {}, predecessor.orderings, predecessor.unique_exprs
        )
        self.predecessor: HybridOperation = predecessor
        self.condition: HybridExpr = condition

    def __repr__(self):
        return f"FILTER[{self.condition}]"


class HybridPartition(HybridOperation):
    """
    Class for HybridOperation corresponding to a PARTITION operation.
    """

    def __init__(self):
        super().__init__({}, {}, [], [])
        self.key_names: list[str] = []

    def __repr__(self):
        key_map = {name: self.terms[name] for name in self.key_names}
        return f"PARTITION[{key_map}]"

    def add_key(self, key_name: str, key_expr: HybridExpr) -> None:
        """
        Adds a new key to the HybridPartition.

        Args:
            `key_name`: the name of the partitioning key.
            `key_expr`: the expression used to partition.
        """
        self.key_names.append(key_name)
        self.terms[key_name] = key_expr
        self.unique_exprs.append(HybridRefExpr(key_name, key_expr.typ))


class HybridLimit(HybridOperation):
    """
    Class for HybridOperation corresponding to a TOP K operation.
    """

    def __init__(
        self,
        predecessor: HybridOperation,
        records_to_keep: int,
    ):
        super().__init__(
            predecessor.terms, {}, predecessor.orderings, predecessor.unique_exprs
        )
        self.predecessor: HybridOperation = predecessor
        self.records_to_keep: int = records_to_keep

    def __repr__(self):
        return f"LIMIT_{self.records_to_keep}[{self.orderings}]"


class ConnectionType(Enum):
    """
    An enum describing how a hybrid tree is connected to a child tree.
    """

    SINGULAR = 0
    """
    The child should be 1:1 with regards to the parent, and can thus be
    accessed via a simple left join without having to worry about cardinality
    contamination.
    """

    AGGREGATION = 1
    """
    The child is being accessed for the purposes of aggregating its columns.
    The aggregation is done on top of the translated subtree before it is
    combined with the parent tree via a left join. The aggregate call may be
    augmented after the left join, e.g. to coalesce with a default value if the
    left join was not used. The grouping keys for the aggregate are the keys
    used to join the parent tree output onto the subtree ouput.

    If this is used as a child access of a `PARTITION` node, there is no left
    join, though some of the post-processing steps may still occur.
    """

    NDISTINCT = 2
    """
    The child is being accessed for the purposes of counting how many
    distinct elements it has. This is implemented by grouping the child subtree
    on both the original grouping keys as well as the unique columns of the
    subcollection without any aggregations, then having the `aggs` list contain
    a solitary `COUNT` term before being left-joined. The result is coalesced
    with 0, unless this is used as a child access of a `PARTITION` node.
    """

    SEMI = 3
    """
    The child is being used as a semi-join.
    """

    SINGULAR_ONLY_MATCH = 4
    """
    If a SINGULAR connection overlaps with a SEMI connection, then they are
    fused into a variant of SINGULAR that can use an INNER join instead of a
    LEFT join.
    """

    AGGREGATION_ONLY_MATCH = 5
    """
    If an AGGREGATION connection overlaps with a SEMI connection, then they are
    fused into a variant of AGGREGATION that can use an INNER join instead of a
    LEFT join.
    """

    NDISTINCT_ONLY_MATCH = 6
    """
    If a NDISTINCT connection overlaps with a SEMI connection, then they are
    fused into a variant of NDISTINCT that can use an INNER join instead of a
    LEFT join.
    """

    ANTI = 7
    """
    The child is being used as an anti-join.
    """

    NO_MATCH_SINGULAR = 8
    """
    If a SINGULAR connection overlaps with an ANTI connection, then it
    becomes this connection which still functions as an ANTI but replaces
    all of the child references with NULL.
    """

    NO_MATCH_AGGREGATION = 9
    """
    If an AGGREGATION connection overlaps with an ANTI connection, then it
    becomes this connection which still functions as an ANTI but replaces
    all of the aggregation outputs with NULL.
    """

    NO_MATCH_NDISTINCT = 10
    """
    If a NDISTINCT connection overlaps with an ANTI connection, then it
    becomes this connection which still functions as an ANTI but replaces
    the NDISTINCT output with 0.
    """

    @property
    def is_singular(self) -> bool:
        """
        Whether the connection type corresponds to one of the 3 SINGULAR
        cases.
        """
        return self in (
            ConnectionType.SINGULAR,
            ConnectionType.SINGULAR_ONLY_MATCH,
            ConnectionType.NO_MATCH_SINGULAR,
        )

    @property
    def is_aggregation(self) -> bool:
        """
        Whether the connection type corresponds to one of the 3 AGGREGATION
        cases.
        """
        return self in (
            ConnectionType.AGGREGATION,
            ConnectionType.AGGREGATION_ONLY_MATCH,
            ConnectionType.NO_MATCH_AGGREGATION,
        )

    @property
    def is_ndistinct(self) -> bool:
        """
        Whether the connection type corresponds to one of the 3 NDISTINCT
        cases.
        """
        return self in (
            ConnectionType.NDISTINCT,
            ConnectionType.NDISTINCT_ONLY_MATCH,
            ConnectionType.NO_MATCH_NDISTINCT,
        )

    @property
    def is_semi(self) -> bool:
        """
        Whether the connection type corresponds to one of the 4 SEMI cases.
        """
        return self in (
            ConnectionType.SEMI,
            ConnectionType.SINGULAR_ONLY_MATCH,
            ConnectionType.AGGREGATION_ONLY_MATCH,
            ConnectionType.NDISTINCT_ONLY_MATCH,
        )

    @property
    def is_anti(self) -> bool:
        """
        Whether the connection type corresponds to one of the 4 ANTI cases.
        """
        return self in (
            ConnectionType.ANTI,
            ConnectionType.NO_MATCH_SINGULAR,
            ConnectionType.NO_MATCH_AGGREGATION,
            ConnectionType.NO_MATCH_AGGREGATION,
        )

    @property
    def is_neutral_matching(self) -> bool:
        """
        Whether the connection type is neutral with regards to how it accesses
        any child terms.
        """
        return self in (ConnectionType.SEMI, ConnectionType.ANTI)

    @property
    def is_singular_compatible(self) -> bool:
        """
        Whether the connection type can be reconciled with SINGULAR.
        """
        return self.is_singular or self.is_neutral_matching

    @property
    def is_aggregation_compatible(self) -> bool:
        """
        Whether the connection type can be reconciled with AGGREGATION.
        """
        return self.is_aggregation or self.is_neutral_matching

    @property
    def is_ndistinct_compatible(self) -> bool:
        """
        Whether the connection type can be reconciled with NDISTINCT.
        """
        return self.is_ndistinct or self.is_neutral_matching

    @property
    def join_type(self) -> JoinType:
        """
        The type of join that the connection type corresponds to.
        """
        match self:
            case (
                ConnectionType.SINGULAR
                | ConnectionType.AGGREGATION
                | ConnectionType.NDISTINCT
            ):
                # A regular connection without SEMI or ANTI has to be a LEFT
                # join since parent records without subcollection instances
                # must be maintained.
                return JoinType.LEFT
            case (
                ConnectionType.SINGULAR_ONLY_MATCH
                | ConnectionType.AGGREGATION_ONLY_MATCH
                | ConnectionType.NDISTINCT_ONLY_MATCH
            ):
                # A regular connection combined with SEMI can be an INNER join
                # since records without matches can be dropped.
                return JoinType.INNER
            case ConnectionType.SEMI:
                # A standalone SEMI connection just becomes a SEMI join.
                return JoinType.SEMI
            case (
                ConnectionType.ANTI
                | ConnectionType.NO_MATCH_SINGULAR
                | ConnectionType.NO_MATCH_AGGREGATION
                | ConnectionType.NO_MATCH_NDISTINCT
            ):
                # Any type of ANTI connection just becomes an ANTI join; the
                # relational conversion step is responsible for converting any
                # references to the child expressions/aggregations to NULL
                # since they do not exist.
                return JoinType.ANTI
            case _:
                raise ValueError(f"Connection type {self} does not have a join type")

    def reconcile_connection_types(self, other: "ConnectionType") -> "ConnectionType":
        """
        Combines two connection types and returns the resulting connection
        type used when they overlap.

        Args:
            `other`: the other connection type that is to be reconciled
            with `self`.

        Returns:
            The connection type produced when `self` and `other` overlap.
        """
        # For duplicates, the connection type is unmodified
        if self == other:
            return self

        # Determine whether the connection types are being reconciled into
        # a combination that keeps matches or drops matches (has to be
        # exactly one of these).
        either_semi: bool = self.is_semi or other.is_semi
        either_anti: bool = self.is_anti or other.is_anti
        only_match: bool
        if either_semi and not either_anti:
            only_match = True
        elif either_anti and not either_semi:
            only_match = False
        else:
            raise ValueError(
                f"Malformed or unsupported combination of connection types: {self} and {other}"
            )

        # Determine if the connection types are being resolved into a SINGULAR
        # combination.
        if self.is_singular_compatible and other.is_singular_compatible:
            if only_match:
                return ConnectionType.SINGULAR_ONLY_MATCH
            else:
                return ConnectionType.NO_MATCH_SINGULAR

        # Determine if the connection types are being resolved into an
        # AGGREGATION combination.
        if self.is_aggregation_compatible and other.is_aggregation_compatible:
            if only_match:
                return ConnectionType.AGGREGATION_ONLY_MATCH
            else:
                return ConnectionType.NO_MATCH_AGGREGATION

        # Determine if the connection types are being resolved into a NDISTINCT
        # combination.
        if self.is_ndistinct_compatible and other.is_ndistinct_compatible:
            if only_match:
                return ConnectionType.NDISTINCT_ONLY_MATCH
            else:
                return ConnectionType.NO_MATCH_NDISTINCT

        # Every other combination is malformed
        raise ValueError(
            f"Malformed combination of connection types: {self} and {other}"
        )


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
    - `required_steps`: an index indicating which step in the pipeline must be
       completed before the child can be defined.
    - `aggs`: a mapping of aggregation calls made onto expressions relative to the
       context of `subtree`.
    """

    parent: "HybridTree"
    subtree: "HybridTree"
    connection_type: ConnectionType
    required_steps: int
    aggs: dict[str, HybridFunctionExpr]


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
        self._agg_keys: list[HybridExpr] | None = None
        self._join_keys: list[tuple[HybridExpr, HybridExpr]] | None = None

    def __repr__(self):
        lines = []
        if self.parent is not None:
            lines.extend(repr(self.parent).splitlines())
        lines.append(" -> ".join(repr(operation) for operation in self.pipeline))
        prefix = " " if self.successor is None else "↓"
        for idx, child in enumerate(self.children):
            lines.append(f"{prefix} child #{idx}:")
            if child.subtree.agg_keys is not None:
                lines.append(
                    f"{prefix}  aggregate: {child.subtree.agg_keys} -> {child.aggs}:"
                )
            if child.subtree.join_keys is not None:
                lines.append(f"{prefix}  join: {child.subtree.join_keys}:")
            for line in repr(child.subtree).splitlines():
                lines.append(f"{prefix} {line}")
        return "\n".join(lines)

    def __eq__(self, other):
        return type(self) is type(other) and repr(self) == repr(other)

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
        PyDoughQDAG BACK terms to HybridExpr BACK terms.
        """
        return self._is_hidden_level

    @property
    def is_connection_root(self) -> bool:
        """
        True if the current level is the top of a subtree located inside of
        a HybridConnection.
        """
        return self._is_connection_root

    @property
    def agg_keys(self) -> list[HybridExpr] | None:
        """
        The list of keys used to aggregate this HybridTree relative to its
        ancestor, if it is the base of a HybridConnection.
        """
        return self._agg_keys

    @agg_keys.setter
    def agg_keys(self, agg_keys: list[HybridExpr]) -> None:
        """
        Assigns the aggregation keys to a hybrid tree.
        """
        self._agg_keys = agg_keys

    @property
    def join_keys(self) -> list[tuple[HybridExpr, HybridExpr]] | None:
        """
        The list of keys used to join this HybridTree relative to its
        ancestor, if it is the base of a HybridConnection.
        """
        return self._join_keys

    @join_keys.setter
    def join_keys(self, join_keys: list[tuple[HybridExpr, HybridExpr]]) -> None:
        """
        Assigns the join keys to a hybrid tree.
        """
        self._join_keys = join_keys

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
            `connection_type`: enum indicating what kind of connection is to be
            used to link `self` to `child`.

        Returns:
            The index of the newly inserted child (or the index of an existing
            child that matches it).
        """
        connection: HybridConnection = HybridConnection(
            self, child, connection_type, len(self.pipeline) - 1, {}
        )
        for idx, existing_connection in enumerate(self.children):
            if child == existing_connection.subtree:
                connection_type = connection_type.reconcile_connection_types(
                    existing_connection.connection_type
                )
                existing_connection.connection_type = connection_type
                return idx
        self._children.append(connection)
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
        shifted_expr: HybridExpr | None
        # Shift the aggregation keys and rhs of join keys back by 1 level to
        # account for the fact that the successor must use the same aggregation
        # and join keys as `self`, but they have now become backreferences.
        if self.agg_keys is not None:
            successor_agg_keys: list[HybridExpr] = []
            for key in self.agg_keys:
                shifted_expr = key.shift_back(1)
                assert shifted_expr is not None
                successor_agg_keys.append(shifted_expr)
            successor._agg_keys = successor_agg_keys
        if self.join_keys is not None:
            successor_join_keys: list[tuple[HybridExpr, HybridExpr]] = []
            for lhs_key, rhs_key in self.join_keys:
                shifted_expr = rhs_key.shift_back(1)
                assert shifted_expr is not None
                successor_join_keys.append((lhs_key, shifted_expr))
            successor._join_keys = successor_join_keys


class HybridTranslator:
    """
    Class used to translate PyDough QDAG nodes into the HybridTree structure.
    """

    def __init__(self, configs: PyDoughConfigs):
        self.configs = configs
        # An index used for creating fake column names for aliases
        self.alias_counter: int = 0

    @staticmethod
    def get_join_keys(
        parent_tree: HybridTree,
        subcollection_property: SubcollectionRelationshipMetadata,
        child_node: HybridOperation,
    ) -> list[tuple[HybridExpr, HybridExpr]]:
        """
        Fetches the list of keys used to join a child node relative to its
        parent node, specifically when the child is a subcollection access.

        Args:
            `parent_tree`: the HybridTree corresponding to the parent to access
            from.
            `subcollection_property`: the metadata for the subcollection
            access.
            `child_node`: the HybridOperation node corresponding to the access.

        Returns:
            The list of tuples expressions used to join the child, expressed in
            terms of its level, to its parent, where the first tuple element is
            the parent key and the second one is the child key.
        """
        join_keys: list[tuple[HybridExpr, HybridExpr]] = []
        if isinstance(subcollection_property, SimpleJoinMetadata):
            # If the subcollection is a simple join property, extract the keys.
            for lhs_name in subcollection_property.keys:
                lhs_key: HybridExpr = (
                    parent_tree.pipeline[-1].terms[lhs_name].make_into_ref(lhs_name)
                )
                for rhs_name in subcollection_property.keys[lhs_name]:
                    rhs_key: HybridExpr = child_node.terms[rhs_name].make_into_ref(
                        rhs_name
                    )
                    join_keys.append((lhs_key, rhs_key))
        elif not isinstance(subcollection_property, CartesianProductMetadata):
            raise NotImplementedError(
                f"Unsupported subcollection property type used for accessing a subcollection: {subcollection_property.__class__.__name__}"
            )
        return join_keys

    @staticmethod
    def get_subcollection_join_keys(
        subcollection_property: SubcollectionRelationshipMetadata,
        parent_node: HybridOperation,
        child_node: HybridOperation,
    ) -> list[tuple[HybridExpr, HybridExpr]]:
        """
        Fetches the list of pairs of keys used to join a parent node onto its
        child node

        Args:
            `subcollection_property`: the metadata for the subcollection
            access.
            `parent_node`: the HybridOperation node corresponding to the parent.
            `child_node`: the HybridOperation node corresponding to the access.

        Returns:
            A list of tuples in the form `(lhs_key, rhs_key)` where each
            `lhs_key` is the join key from the parent's perspective and each
            `rhs_key` is the join key from the child's perspective.
        """
        join_keys: list[tuple[HybridExpr, HybridExpr]] = []
        if isinstance(subcollection_property, SimpleJoinMetadata):
            # If the subcollection is a simple join property, extract the keys
            # and build the corresponding (lhs_key == rhs_key) conditions
            for lhs_name in subcollection_property.keys:
                lhs_key: HybridExpr = parent_node.terms[lhs_name].make_into_ref(
                    lhs_name
                )
                for rhs_name in subcollection_property.keys[lhs_name]:
                    rhs_key: HybridExpr = child_node.terms[rhs_name].make_into_ref(
                        rhs_name
                    )
                    join_keys.append((lhs_key, rhs_key))
        elif not isinstance(subcollection_property, CartesianProductMetadata):
            raise NotImplementedError(
                f"Unsupported subcollection property type used for accessing a subcollection: {subcollection_property.__class__.__name__}"
            )
        return join_keys

    @staticmethod
    def identify_connection_types(
        expr: PyDoughExpressionQDAG,
        child_idx: int,
        reference_types: set[ConnectionType],
        inside_aggregation: bool = False,
    ) -> None:
        """
        Recursively identifies what types ways a child collection is referenced
        by its parent context.

        Args:
            `expr`: the expression being recursively checked for references
            to the child collection.
            `child_idx`: the index of the child that is being searched for
            references to it.
            `reference_types`: the set of known connection types that the
            are used when referencing the child; the function should mutate
            this set if it finds any new connections.
            `inside_aggregation`: True if `expr` is inside of a call to an
            aggregation function.
        """
        match expr:
            # If `expr` is a reference to the child in question, add
            # a reference that is either singular or aggregation depending
            # on the `inside_aggregation` argument
            case ChildReferenceExpression() if expr.child_idx == child_idx:
                reference_types.add(
                    ConnectionType.AGGREGATION
                    if inside_aggregation
                    else ConnectionType.SINGULAR
                )
            case WindowCall():
                # Otherwise, mutate `reference_types` based on the arguments
                # to the window call.
                for col in expr.collation_args:
                    HybridTranslator.identify_connection_types(
                        col.expr, child_idx, reference_types, inside_aggregation
                    )
            case ExpressionFunctionCall():
                # If `expr` is a `HAS` call on the child in question, add a
                # semi-join connection.
                if expr.operator == pydop.HAS:
                    arg = expr.args[0]
                    assert isinstance(arg, ChildReferenceCollection)
                    if arg.child_idx == child_idx:
                        reference_types.add(ConnectionType.SEMI)
                # If `expr` is a `HASNOT` call on the child in question, add a
                # anti-join connection.
                elif expr.operator == pydop.HASNOT:
                    arg = expr.args[0]
                    assert isinstance(arg, ChildReferenceCollection)
                    if arg.child_idx == child_idx:
                        reference_types.add(ConnectionType.ANTI)
                # Otherwise, mutate `reference_types` based on the arguments
                # to the function call.
                else:
                    for arg in expr.args:
                        if isinstance(arg, ChildReferenceCollection):
                            # If the argument is a refernece to a child,
                            # collection, e.g. `COUNT(X)`, treat as an
                            # aggregation reference if it refers to the child
                            # in question.
                            if arg.child_idx == child_idx:
                                reference_types.add(ConnectionType.AGGREGATION)
                        else:
                            # Otherwise, recursively check the arguments to the
                            # function, promoting `inside_aggregation` to True
                            # if the function is an aggfunc.
                            assert isinstance(arg, PyDoughExpressionQDAG)
                            inside_aggregation = (
                                inside_aggregation or expr.operator.is_aggregation
                            )
                            HybridTranslator.identify_connection_types(
                                arg, child_idx, reference_types, inside_aggregation
                            )
            case _:
                return

    def populate_children(
        self,
        hybrid: HybridTree,
        child_operator: ChildOperator,
        child_idx_mapping: dict[int, int],
    ) -> None:
        """
        Helper utility that takes any children of a child operator (CALC,
        WHERE, etc.) and builds the corresponding HybridTree subtree,
        where the parent of the subtree's root is absent instead of the
        current level, and inserts the corresponding HybridConnection node.

        Args:
            `hybrid`: the HybridTree having children added to it.
            `child_operator`: the collection QDAG node (CALC, WHERE, etc.)
            containing the children.
            `child_idx_mapping`: a mapping of indices of children of the
            original `child_operator` to the indices of children of the hybrid
            tree level, since the hybrid tree contains the children of all
            pipeline operators of the current level and therefore the indices
            get changes. When the child is inserted, this mapping is mutated
            accordingly so expressions using the child indices know what hybrid
            connection index to use.
        """
        for child_idx, child in enumerate(child_operator.children):
            subtree: HybridTree = self.make_hybrid_tree(child, hybrid)
            reference_types: set[ConnectionType] = set()
            match child_operator:
                case Where():
                    self.identify_connection_types(
                        child_operator.condition, child_idx, reference_types
                    )
                case OrderBy():
                    for col in child_operator.collation:
                        self.identify_connection_types(
                            col.expr, child_idx, reference_types
                        )
                case Calc():
                    for expr in child_operator.calc_term_values.values():
                        self.identify_connection_types(expr, child_idx, reference_types)
                case PartitionBy():
                    reference_types.add(ConnectionType.AGGREGATION)
            if len(reference_types) == 0:
                raise ValueError(
                    f"Bad call to populate_children: child {child_idx} of {child_operator} is never used"
                )
            connection_type: ConnectionType = reference_types.pop()
            for con_typ in reference_types:
                connection_type = connection_type.reconcile_connection_types(con_typ)
            child_idx_mapping[child_idx] = hybrid.add_child(subtree, connection_type)

    def make_hybrid_agg_expr(
        self,
        hybrid: HybridTree,
        expr: PyDoughExpressionQDAG,
        child_ref_mapping: dict[int, int],
    ) -> tuple[HybridExpr, int | None]:
        """
        Converts a QDAG expression into a HybridExpr specifically with the
        intent of making it the input to an aggregation call. Returns the
        converted function argument, as well as an index indicating what child
        subtree the aggregation's arguments belong to. NOTE: the HybridExpr is
        phrased relative to the child subtree, rather than relative to `hybrid`
        itself.

        Args:
            `hybrid`: the hybrid tree that should be used to derive the
            translation of `expr`, as it is the context in which the `expr`
            will live.
            `expr`: the QDAG expression to be converted.
            `child_ref_mapping`: mapping of indices used by child references
            in the original expressions to the index of the child hybrid tree
            relative to the current level.

        Returns:
            The HybridExpr node corresponding to `expr`, as well as the index
            of the child it belongs to (e.g. which subtree does this
            aggregation need to be done on top of).
        """
        hybrid_result: HybridExpr
        # This value starts out as None since we do not know the child index
        # that `expr` correspond to yet. It may still be None at the end, since
        # it is possible that `expr` does not correspond to any child index.
        child_idx: int | None = None
        match expr:
            case PartitionKey():
                return self.make_hybrid_agg_expr(hybrid, expr.expr, child_ref_mapping)
            case Literal():
                # Literals are kept as-is.
                hybrid_result = HybridLiteralExpr(expr)
            case ChildReferenceExpression():
                # Child references become regular references because the
                # expression is phrased as if we were inside the child rather
                # than the parent.
                child_idx = child_ref_mapping[expr.child_idx]
                child_connection = hybrid.children[child_idx]
                expr_name = child_connection.subtree.pipeline[-1].renamings.get(
                    expr.term_name, expr.term_name
                )
                hybrid_result = HybridRefExpr(expr_name, expr.pydough_type)
            case ExpressionFunctionCall():
                if expr.operator.is_aggregation:
                    raise NotImplementedError(
                        "PyDough does not yet support calling aggregations inside of aggregations"
                    )
                # Every argument must be translated in the same manner as a
                # regular function argument, except that the child index it
                # corresponds to must be reconciled with the child index value
                # accumulated so far.
                args: list[HybridExpr] = []
                for arg in expr.args:
                    if not isinstance(arg, PyDoughExpressionQDAG):
                        raise NotImplementedError(
                            f"TODO: support converting {arg.__class__.__name__} as a function argument"
                        )
                    hybrid_arg, hybrid_child_index = self.make_hybrid_agg_expr(
                        hybrid, arg, child_ref_mapping
                    )
                    if hybrid_child_index is not None:
                        if child_idx is None:
                            # In this case, the argument is the first one seen that
                            # has an index, so that index is chosen.
                            child_idx = hybrid_child_index
                        elif hybrid_child_index != child_idx:
                            # In this case, multiple arguments correspond to
                            # different children, which cannot be handled yet
                            # because it means it is impossible to push the agg
                            # call into a single HybridConnection node.
                            raise NotImplementedError(
                                "Unsupported case: multiple child indices referenced by aggregation arguments"
                            )
                    args.append(hybrid_arg)
                hybrid_result = HybridFunctionExpr(
                    expr.operator, args, expr.pydough_type
                )
            case BackReferenceExpression():
                raise NotImplementedError(
                    "PyDough does yet support aggregations whose arguments mix between subcollection data of the current context and fields of an ancestor of the current context"
                )
            case Reference():
                raise NotImplementedError(
                    "PyDough does yet support aggregations whose arguments mix between subcollection data of the current context and fields of the context itself"
                )
            case WindowCall():
                raise NotImplementedError(
                    "PyDough does yet support aggregations whose arguments mix between subcollection data of the current context and window functions"
                )
            case _:
                raise NotImplementedError(
                    f"TODO: support converting {expr.__class__.__name__} in aggregations"
                )
        return hybrid_result, child_idx

    def postprocess_agg_output(
        self, agg_call: HybridFunctionExpr, agg_ref: HybridExpr, joins_can_nullify: bool
    ) -> HybridExpr:
        """
        Transforms an aggregation function call in any ways that are necessary
        due to configs, such as coalescing the output with zero.

        Args:
            `agg_call`: the aggregation call whose reference must be
            transformed if the configs demand it.
            `agg_ref`: the reference to the aggregation call that is
            transformed if the configs demand it.
            `joins_can_nullify`: True if the aggregation is fed into a left
            join, which creates the requirement for some aggregations like
            `COUNT` to have their defaults replaced.

        Returns:
            The transformed version of `agg_ref`, if postprocessing is required,
        """
        # If doing a SUM or AVG, and the configs are set to default those
        # functions to zero when there are no values, decorate the result
        # with `DEFAULT_TO(x, 0)`. Also, always does this step with COUNT for
        # left joins since the semantics of that function never allow returning
        # NULL.
        if (
            (agg_call.operator == pydop.SUM and self.configs.sum_default_zero)
            or (agg_call.operator == pydop.AVG and self.configs.avg_default_zero)
            or (agg_call.operator == pydop.COUNT and joins_can_nullify)
        ):
            agg_ref = HybridFunctionExpr(
                pydop.DEFAULT_TO,
                [agg_ref, HybridLiteralExpr(Literal(0, Int64Type()))],
                agg_call.typ,
            )
        return agg_ref

    def get_agg_name(self, connection: "HybridConnection") -> str:
        """
        Generates a unique name for an aggregation function's output that
        is not already used.

        Args:
            `connection`: the HybridConnection in which the aggregation
            is being defined. The name cannot overlap with any other agg
            names or term names of the connection.

        Returns:
            The new name to be used.
        """
        return self.get_internal_name(
            "agg", [connection.subtree.pipeline[-1].terms, connection.aggs]
        )

    def get_ordering_name(self, hybrid: HybridTree) -> str:
        return self.get_internal_name("ordering", [hybrid.pipeline[-1].terms])

    def get_internal_name(
        self, prefix: str, reserved_names: list[dict[str, Any]]
    ) -> str:
        """
        Generates a name to be used in the terms of a HybridTree with a
        specified prefix that does not overlap with certain names that have
        already been taken in that context.

        Args:
            `prefix`: the prefix that the generated name should start with.
            `reserved_names`: a list of mappings where the keys in each mapping
            are names that cannot be used because they have already been taken.

        Returns:
            The string of the name chosen with the corresponding prefix that
            does not overlap with the reserved name.
        """
        name = f"{prefix}_{self.alias_counter}"
        while any(name in s for s in reserved_names):
            self.alias_counter += 1
            name = f"{prefix}_{self.alias_counter}"
        self.alias_counter += 1
        return name

    def handle_collection_count(
        self,
        hybrid: HybridTree,
        expr: ExpressionFunctionCall,
        child_ref_mapping: dict[int, int],
    ) -> HybridExpr:
        """
        Special case of `make_hybrid_expr` specifically for expressions that
        are the COUNT of a subcollection.

        Args:
            `hybrid`: the hybrid tree that should be used to derive the
            translation of `expr`, as it is the context in which the `expr`
            will live.
            `expr`: the QDAG expression to be converted.
            `child_ref_mapping`: mapping of indices used by child references in
            the original expressions to the index of the child hybrid tree
            relative to the current level.

        Returns:
            The HybridExpr node corresponding to `expr`
        """
        assert (
            expr.operator == pydop.COUNT
        ), f"Malformed call to handle_collection_count: {expr}"
        assert len(expr.args) == 1, f"Malformed call to handle_collection_count: {expr}"
        collection_arg = expr.args[0]
        assert isinstance(
            collection_arg, ChildReferenceCollection
        ), f"Malformed call to handle_collection_count: {expr}"
        count_call: HybridFunctionExpr = HybridFunctionExpr(
            pydop.COUNT, [], expr.pydough_type
        )
        child_idx: int = child_ref_mapping[collection_arg.child_idx]
        child_connection: HybridConnection = hybrid.children[child_idx]
        # Generate a unique name for the agg call to push into the child
        # connection.
        agg_name: str = self.get_agg_name(child_connection)
        child_connection.aggs[agg_name] = count_call
        result_ref: HybridExpr = HybridChildRefExpr(
            agg_name, child_idx, expr.pydough_type
        )
        # The null-adding join is not done if this is the root level, since
        # that just means all the aggregations are no-groupby aggregations.
        joins_can_nullify: bool = not isinstance(hybrid.pipeline[0], HybridRoot)
        return self.postprocess_agg_output(count_call, result_ref, joins_can_nullify)

    def handle_has_hasnot(
        self,
        hybrid: HybridTree,
        expr: ExpressionFunctionCall,
        child_ref_mapping: dict[int, int],
    ) -> HybridExpr:
        """
        Handler function for translating a `HAS` or `HASNOT` expression by
        mutating the referenced HybridConnection so it enforces that predicate,
        then returning an expression indicating that the condition has been
        met.
        """
        assert expr.operator in (
            pydop.HAS,
            pydop.HASNOT,
        ), f"Malformed call to handle_has_hasnot: {expr}"
        assert len(expr.args) == 1, f"Malformed call to handle_has_hasnot: {expr}"
        collection_arg = expr.args[0]
        assert isinstance(
            collection_arg, ChildReferenceCollection
        ), f"Malformed call to handle_has_hasnot: {expr}"
        # Reconcile the existing connection type with either SEMI or ANTI
        child_idx: int = child_ref_mapping[collection_arg.child_idx]
        child_connection: HybridConnection = hybrid.children[child_idx]
        new_conn_type: ConnectionType = (
            ConnectionType.SEMI if expr.operator == pydop.HAS else ConnectionType.ANTI
        )
        child_connection.connection_type = (
            child_connection.connection_type.reconcile_connection_types(new_conn_type)
        )
        # Since the connection has been mutated to be a semi/anti join, the
        # has / hasnot condition is now known to be true.
        return HybridLiteralExpr(Literal(True, BooleanType()))

    def make_hybrid_expr(
        self,
        hybrid: HybridTree,
        expr: PyDoughExpressionQDAG,
        child_ref_mapping: dict[int, int],
    ) -> HybridExpr:
        """
        Converts a QDAG expression into a HybridExpr.

        Args:
            `hybrid`: the hybrid tree that should be used to derive the
            translation of `expr`, as it is the context in which the `expr`
            will live.
            `expr`: the QDAG expression to be converted.
            `child_ref_mapping`: mapping of indices used by child references in
            the original expressions to the index of the child hybrid tree
            relative to the current level.

        Returns:
            The HybridExpr node corresponding to `expr`
        """
        expr_name: str
        child_connection: HybridConnection
        args: list[HybridExpr] = []
        hybrid_arg: HybridExpr
        ancestor_tree: HybridTree
        match expr:
            case PartitionKey():
                return self.make_hybrid_expr(hybrid, expr.expr, child_ref_mapping)
            case Literal():
                return HybridLiteralExpr(expr)
            case ColumnProperty():
                return HybridColumnExpr(expr)
            case ChildReferenceExpression():
                # A reference to an expression from a child subcollection
                # becomes a reference to one of the terms of one of the child
                # subtrees of the current hybrid tree.
                hybrid_child_index: int = child_ref_mapping[expr.child_idx]
                child_connection = hybrid.children[hybrid_child_index]
                expr_name = child_connection.subtree.pipeline[-1].renamings.get(
                    expr.term_name, expr.term_name
                )
                return HybridChildRefExpr(
                    expr_name, hybrid_child_index, expr.pydough_type
                )
            case BackReferenceExpression():
                # A reference to an expression from an ancestor becomes a
                # reference to one of the terms of a parent level of the hybrid
                # tree. This does not yet support cases where the back
                # reference steps outside of a child subtree and back into its
                # parent subtree, since that breaks the independence between
                # the parent and child.
                ancestor_tree = hybrid
                back_idx: int = 0
                true_steps_back: int = 0
                # Keep stepping backward until `expr.back_levels` non-hidden
                # steps have been taken (to ignore steps that are part of a
                # compound).
                while true_steps_back < expr.back_levels:
                    if ancestor_tree.parent is None:
                        raise NotImplementedError(
                            "TODO: (gh #141) support BACK references that step from a child subtree back into a parent context."
                        )
                    ancestor_tree = ancestor_tree.parent
                    back_idx += true_steps_back
                    if not ancestor_tree.is_hidden_level:
                        true_steps_back += 1
                expr_name = ancestor_tree.pipeline[-1].renamings.get(
                    expr.term_name, expr.term_name
                )
                return HybridBackRefExpr(expr_name, expr.back_levels, expr.pydough_type)
            case Reference():
                expr_name = hybrid.pipeline[-1].renamings.get(
                    expr.term_name, expr.term_name
                )
                return HybridRefExpr(expr_name, expr.pydough_type)
            case ExpressionFunctionCall() if not expr.operator.is_aggregation:
                # For non-aggregate function calls, translate their arguments
                # normally and build the function call. Does not support any
                # such function that takes in a collection, as none currently
                # exist that are not aggregations.
                for arg in expr.args:
                    if not isinstance(arg, PyDoughExpressionQDAG):
                        raise NotImplementedError(
                            "PyDough does not yet support converting collections as function arguments to a non-aggregation function"
                        )
                    args.append(self.make_hybrid_expr(hybrid, arg, child_ref_mapping))
                return HybridFunctionExpr(expr.operator, args, expr.pydough_type)
            case ExpressionFunctionCall() if expr.operator.is_aggregation:
                # For aggregate function calls, their arguments are translated in
                # a manner that identifies what child subtree they correspond too,
                # by index, and translates them relative to the subtree. Then, the
                # aggregation calls are placed into the `aggs` mapping of the
                # corresponding child connection, and the aggregation call becomes
                # a child reference (referring to the aggs list), since after
                # translation, an aggregated child subtree only has the grouping
                # keys & the aggregation calls as opposed to its other terms.
                child_idx: int | None = None
                arg_child_idx: int | None = None
                for arg in expr.args:
                    if isinstance(arg, PyDoughExpressionQDAG):
                        hybrid_arg, arg_child_idx = self.make_hybrid_agg_expr(
                            hybrid, arg, child_ref_mapping
                        )
                    else:
                        if not isinstance(arg, ChildReferenceCollection):
                            raise NotImplementedError("Cannot process argument")
                        # TODO: (gh #148) handle collection-level NDISTINCT
                        if expr.operator == pydop.COUNT:
                            return self.handle_collection_count(
                                hybrid, expr, child_ref_mapping
                            )
                        elif expr.operator in (pydop.HAS, pydop.HASNOT):
                            return self.handle_has_hasnot(
                                hybrid, expr, child_ref_mapping
                            )
                        else:
                            raise NotImplementedError(
                                f"PyDough does not yet support collection arguments for aggregation function {expr.operator}"
                            )
                    # Accumulate the `arg_child_idx` value from the argument across
                    # all function arguments, ensuring that at the end there is
                    # exactly one child subtree that the agg call corresponds to.
                    if arg_child_idx is not None:
                        if child_idx is None:
                            child_idx = arg_child_idx
                        elif arg_child_idx != child_idx:
                            raise NotImplementedError(
                                "Unsupported case: multiple child indices referenced by aggregation arguments"
                            )
                    args.append(hybrid_arg)
                if child_idx is None:
                    raise NotImplementedError(
                        "Unsupported case: no child indices referenced by aggregation arguments"
                    )
                hybrid_call: HybridFunctionExpr = HybridFunctionExpr(
                    expr.operator, args, expr.pydough_type
                )
                child_connection = hybrid.children[child_idx]
                # Generate a unique name for the agg call to push into the child
                # connection.
                agg_name: str = self.get_agg_name(child_connection)
                child_connection.aggs[agg_name] = hybrid_call
                result_ref: HybridExpr = HybridChildRefExpr(
                    agg_name, child_idx, expr.pydough_type
                )
                joins_can_nullify: bool = not isinstance(hybrid.pipeline[0], HybridRoot)
                return self.postprocess_agg_output(
                    hybrid_call, result_ref, joins_can_nullify
                )
            case WindowCall():
                partition_args: list[HybridExpr] = []
                order_args: list[HybridCollation] = []
                if expr.levels is not None:
                    ancestor_tree = hybrid
                    for _ in range(expr.levels):
                        if ancestor_tree.parent is None:
                            raise ValueError("Window function references too far back")
                        ancestor_tree = ancestor_tree.parent
                    for unique_term in ancestor_tree.pipeline[-1].unique_exprs:
                        shifted_arg: HybridExpr | None = unique_term.shift_back(
                            expr.levels
                        )
                        assert shifted_arg is not None
                        partition_args.append(shifted_arg)
                for arg in expr.collation_args:
                    hybrid_arg = self.make_hybrid_expr(
                        hybrid, arg.expr, child_ref_mapping
                    )
                    order_args.append(HybridCollation(hybrid_arg, arg.asc, arg.na_last))
                return HybridWindowExpr(
                    expr.window_operator,
                    [],
                    partition_args,
                    order_args,
                    expr.pydough_type,
                    expr.kwargs,
                )
            case _:
                raise NotImplementedError(
                    f"TODO: support converting {expr.__class__.__name__}"
                )

    def process_hybrid_collations(
        self,
        hybrid: HybridTree,
        collations: list[CollationExpression],
        child_ref_mapping: dict[int, int],
    ) -> tuple[dict[str, HybridExpr], list[HybridCollation]]:
        """_summary_

        Args:
            `hybrid` The hybrid tree used to handle ordering expressions.
            `collations` The collations to process and convert to
                HybridCollation values.
            `child_ref_mapping` The child mapping to track for handling
                child references in the collations.

        Returns:
            A tuple containing a dictionary of new expressions for generating
            a calc and a list of the new HybridCollation values.
        """
        new_expressions: dict[str, HybridExpr] = {}
        hybrid_orderings: list[HybridCollation] = []
        for collation in collations:
            name = self.get_ordering_name(hybrid)
            expr = self.make_hybrid_expr(hybrid, collation.expr, child_ref_mapping)
            new_expressions[name] = expr
            new_collation: HybridCollation = HybridCollation(
                HybridRefExpr(name, collation.expr.pydough_type),
                collation.asc,
                not collation.na_last,
            )
            hybrid_orderings.append(new_collation)
        return new_expressions, hybrid_orderings

    def make_hybrid_tree(
        self, node: PyDoughCollectionQDAG, parent: HybridTree | None = None
    ) -> HybridTree:
        """
        Converts a collection QDAG into the HybridTree format.

        Args:
            `node`: the collection QDAG to be converted.
            `parent`: optional hybrid tree of the parent context that `node` is
            a child of.

        Returns:
            The HybridTree representation of `node`.
        """
        hybrid: HybridTree
        successor_hybrid: HybridTree
        expr: HybridExpr
        child_ref_mapping: dict[int, int] = {}
        key_exprs: list[HybridExpr] = []
        join_key_exprs: list[tuple[HybridExpr, HybridExpr]] = []
        match node:
            case GlobalContext():
                return HybridTree(HybridRoot())
            case CompoundSubCollection():
                raise NotImplementedError(f"{node.__class__.__name__}")
            case TableCollection() | SubCollection():
                successor_hybrid = HybridTree(HybridCollectionAccess(node))
                hybrid = self.make_hybrid_tree(node.ancestor_context, parent)
                hybrid.add_successor(successor_hybrid)
                return successor_hybrid
            case PartitionChild():
                hybrid = self.make_hybrid_tree(node.ancestor_context, parent)
                successor_hybrid = HybridTree(
                    HybridPartitionChild(hybrid.children[0].subtree)
                )
                hybrid.add_successor(successor_hybrid)
                return successor_hybrid
            case Calc():
                hybrid = self.make_hybrid_tree(node.preceding_context, parent)
                self.populate_children(hybrid, node, child_ref_mapping)
                new_expressions: dict[str, HybridExpr] = {}
                for name in sorted(node.calc_terms):
                    expr = self.make_hybrid_expr(
                        hybrid, node.get_expr(name), child_ref_mapping
                    )
                    new_expressions[name] = expr
                hybrid.pipeline.append(
                    HybridCalc(
                        hybrid.pipeline[-1],
                        new_expressions,
                        hybrid.pipeline[-1].orderings,
                    )
                )
                return hybrid
            case Where():
                hybrid = self.make_hybrid_tree(node.preceding_context, parent)
                self.populate_children(hybrid, node, child_ref_mapping)
                expr = self.make_hybrid_expr(hybrid, node.condition, child_ref_mapping)
                hybrid.pipeline.append(HybridFilter(hybrid.pipeline[-1], expr))
                return hybrid
            case PartitionBy():
                hybrid = self.make_hybrid_tree(node.ancestor_context, parent)
                partition: HybridPartition = HybridPartition()
                successor_hybrid = HybridTree(partition)
                hybrid.add_successor(successor_hybrid)
                self.populate_children(successor_hybrid, node, child_ref_mapping)
                partition_child_idx: int = child_ref_mapping[0]
                for key_name in node.calc_terms:
                    key = node.get_expr(key_name)
                    expr = self.make_hybrid_expr(
                        successor_hybrid, key, child_ref_mapping
                    )
                    partition.add_key(key_name, expr)
                    key_exprs.append(HybridRefExpr(key_name, expr.typ))
                successor_hybrid.children[
                    partition_child_idx
                ].subtree.agg_keys = key_exprs
                return successor_hybrid
            case OrderBy() | TopK():
                hybrid = self.make_hybrid_tree(node.preceding_context, parent)
                self.populate_children(hybrid, node, child_ref_mapping)
                new_nodes: dict[str, HybridExpr]
                hybrid_orderings: list[HybridCollation]
                new_nodes, hybrid_orderings = self.process_hybrid_collations(
                    hybrid, node.collation, child_ref_mapping
                )
                hybrid.pipeline.append(
                    HybridCalc(hybrid.pipeline[-1], new_nodes, hybrid_orderings)
                )
                if isinstance(node, TopK):
                    hybrid.pipeline.append(
                        HybridLimit(hybrid.pipeline[-1], node.records_to_keep)
                    )
                return hybrid
            case ChildOperatorChildAccess():
                assert parent is not None
                match node.child_access:
                    case TableCollection() | SubCollection() if not isinstance(
                        node.child_access, CompoundSubCollection
                    ):
                        successor_hybrid = HybridTree(
                            HybridCollectionAccess(node.child_access)
                        )
                        if isinstance(node.child_access, SubCollection):
                            join_key_exprs = HybridTranslator.get_join_keys(
                                parent,
                                node.child_access.subcollection_property,
                                successor_hybrid.pipeline[-1],
                            )
                    case PartitionChild():
                        successor_hybrid = self.make_hybrid_tree(
                            node.child_access.child_access, parent
                        )
                        partition_by = node.child_access.ancestor_context
                        assert isinstance(partition_by, PartitionBy)
                        for key in partition_by.keys:
                            rhs_expr: HybridExpr = self.make_hybrid_expr(
                                successor_hybrid,
                                Reference(node.child_access, key.expr.term_name),
                                child_ref_mapping,
                            )
                            assert isinstance(rhs_expr, HybridRefExpr)
                            lhs_expr: HybridExpr = HybridChildRefExpr(
                                rhs_expr.name, 0, rhs_expr.typ
                            )
                            join_key_exprs.append((lhs_expr, rhs_expr))
                    case _:
                        raise NotImplementedError(
                            f"{node.__class__.__name__} (child is {node.child_access.__class__.__name__})"
                        )
                successor_hybrid.agg_keys = [rhs_key for _, rhs_key in join_key_exprs]
                successor_hybrid.join_keys = join_key_exprs
                return successor_hybrid
            case _:
                raise NotImplementedError(f"{node.__class__.__name__}")
