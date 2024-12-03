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
    "HybridLimit",
    "HybridTree",
    "HybridTranslator",
]


from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pydough.pydough_ast.pydough_operators as pydop
from pydough.configs import PyDoughConfigs
from pydough.pydough_ast import (
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
    PyDoughCollectionAST,
    PyDoughExpressionAST,
    Reference,
    SubCollection,
    TableCollection,
    TopK,
    Where,
)
from pydough.types import Int64Type, PyDoughType


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

    def shift_back(self, levels: int) -> HybridExpr | None:
        raise NotImplementedError


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
        operator: pydop.PyDoughExpressionOperatorAST,
        args: list[HybridExpr],
        typ: PyDoughType,
    ):
        super().__init__(typ)
        self.operator: pydop.PyDoughExpressionOperatorAST = operator
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

    def shift_back(self, levels: int) -> HybridExpr | None:
        return None


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

    def __init__(self, predecessor: HybridOperation, condition: HybridExpr):
        super().__init__(predecessor.terms, {})
        self.predecessor: HybridOperation = predecessor
        self.condition: HybridExpr = condition

    def __repr__(self):
        return f"FILTER[{self.condition}]"


class HybridLimit(HybridOperation):
    """
    Class for HybridOperation corresponding to a TOP K operation.
    """

    def __init__(
        self,
        predecessor: HybridOperation,
        records_to_keep: int,
        collation: list[HybridCollation],
    ):
        super().__init__(predecessor.terms, {})
        self.predecessor: HybridOperation = predecessor
        self.records_to_keep: int = records_to_keep
        self.collation: list[HybridCollation] = collation

    def __repr__(self):
        return f"LIMIT_{self.records_to_keep}[{self.collation}]"


class ConnectionType(Enum):
    """
    An enum describing how a hybrid tree is connected to a child tree.
    """

    SINGULAR = 0
    """
    The child should be 1:1 with regards to the parent, and can thus be
    accessed via a simple left join without having to worry about cardinality
    contamination.

    If this overlaps with a `HAS` connection, then the left join becomes an
    INNER join.

    If this overlaps with a `HASNOT` connection, then this connection becomes
    a `HASNOT` connection and all accesses to it are replaced with `NULL`.
    """

    AGGREGATION = 1
    """
    The child is being accessed for the purposes of aggregating its columns.
    The aggregation is done on top of the translated subtree before it is
    combined with the parent tree via a left join. The aggregate call may be
    augmented after the left join, e.g. to coalesce with a default value if the
    left join was not used. The grouping keys for the aggregate are the keys
    used to join the parent tree output onto the subtree ouput.

    If this overlaps with a `HAS` connection, then the left join becomes an
    INNER join, and the post-processing is not required.

    If this connection overlaps with a `HASNOT` connection, then this
    connection becomes a `HASNOT` connection and all accesses to it are
    replaced with `NULL` (augmented by the usual post-processing step).

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

    If this overlaps with a `HAS` connection, then the left join becomes an
    INNER join, and the coalescing is skipped.

    If this connection overlaps with a `HASNOT` connection, then this
    connection becomes a `HASNOT` connection and the `COUNT` is replaced with
    a constant zero.
    """

    HAS = 3
    """
    The child is being used as a semi-join. NOTE: if there is ever an overlap
    between this case & another usage (e.g. `Nations.WHERE(HAS(x))(res=x.y)` or
    `Nations.WHERE(HAS(x))(res=SUM(x.y))`), see the other enums for
    explanations of what happens in those overlap cases.
    """

    HASNOT = 4
    """
    The child is being used as an anti-join. NOTE: if there is ever an overlap
    between this case & another usage (e.g. `Nations.WHERE(HASNOT(x))(res=x.y)`
    or `Nations.WHERE(HASNOT(x))(res=SUM(x.y))`, see the other enums for
    explanations of what happens in those overlap cases.
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
    - `required_steps`: an index indicating which step in the pipeline must be
       completed before the child can be defined.
    - `aggs`: a mapping of aggregation calls made onto expressions relative to the
       context of `subtree`.
    - `only_keep_matches`: a boolean to indicate whether the parent subtree's
       records can be discarded if they have no matches in the child subtree.
       If this is True, it means any LEFT joins can be replaced with INNER
       joins. This occurs if, for example, a `SINGULAR` connection overlaps
       with a `HAS` connection.
    """

    parent: "HybridTree"
    subtree: "HybridTree"
    connection_type: ConnectionType
    required_steps: int
    aggs: dict[str, HybridFunctionExpr]
    only_keep_matches: bool


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
        self._ordering: list[HybridCollation] = []

    def __repr__(self):
        lines = []
        if self.parent is not None:
            lines.extend(repr(self.parent).splitlines())
        lines.append(" -> ".join(repr(operation) for operation in self.pipeline))
        prefix = " " if self.successor is None else "↓"
        for idx, child in enumerate(self.children):
            lines.append(f"{prefix} child #{idx} ({child.connection_type}):")
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

    @property
    def ordering(self) -> list[HybridCollation]:
        """
        The ordering of the records in the current level.
        """
        return self._ordering

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
            self, child, connection_type, len(self.pipeline) - 1, {}, False
        )
        for idx, existing_connection in enumerate(self.children):
            if (
                existing_connection.connection_type == connection_type
                and child == existing_connection.subtree
            ):
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


class HybridTranslator:
    """
    Class used to translate PyDough AST nodes into the HybridTree structure.
    """

    def __init__(self, configs: PyDoughConfigs):
        self.configs = configs
        # An index used for creating fake column names for aggregations
        self.agg_counter: int = 0

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
            `child_operator`: the collection AST node (CALC, WHERE, etc.)
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
            subtree: HybridTree = self.make_hybrid_tree(child)
            connection_type: ConnectionType
            if child.is_singular(child_operator.starting_predecessor):
                connection_type = ConnectionType.SINGULAR
            else:
                # TODO: parse out the finer differences in aggregation types
                # for COUNT, NDISTINCT, HAS, and HASNOT, versus just general
                # aggregation.
                connection_type = ConnectionType.AGGREGATION
            child_idx_mapping[child_idx] = hybrid.add_child(subtree, connection_type)

    def make_hybrid_agg_expr(
        self,
        hybrid: HybridTree,
        expr: PyDoughExpressionAST,
        child_ref_mapping: dict[int, int],
    ) -> tuple[HybridExpr, int | None]:
        """
        Converts an AST expression into a HybridExpr specifically with the
        intent of making it the input to an aggregation call. Returns the
        converted function argument, as well as an index indicating what child
        subtree the aggregation's arguments belong to. NOTE: the HybridExpr is
        phrased relative to the child subtree, rather than relative to `hybrid`
        itself.

        Args:
            `hybrid`: the hybrid tree that should be used to derive the
            translation of `expr`, as it is the context in which the `expr`
            will live.
            `expr`: the AST expression to be converted.
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
                    if not isinstance(arg, PyDoughExpressionAST):
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
        agg_name: str = f"agg_{self.agg_counter}"
        while (
            agg_name in connection.subtree.pipeline[-1].terms
            or agg_name in connection.aggs
        ):
            self.agg_counter += 1
            agg_name = f"agg_{self.agg_counter}"
        self.agg_counter += 1
        return agg_name

    def handle_collection_count(
        self,
        hybrid: HybridTree,
        expr: ExpressionFunctionCall,
        child_ref_mapping: dict[int, int],
    ) -> HybridExpr:
        """
        TODO
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

    def make_hybrid_expr(
        self,
        hybrid: HybridTree,
        expr: PyDoughExpressionAST,
        child_ref_mapping: dict[int, int],
    ) -> HybridExpr:
        """
        Converts an AST expression into a HybridExpr.

        Args:
            `hybrid`: the hybrid tree that should be used to derive the
            translation of `expr`, as it is the context in which the `expr`
            will live.
            `expr`: the AST expression to be converted.
            `child_ref_mapping`: mapping of indices used by child references in
            the original expressions to the index of the child hybrid tree
            relative to the current level.

        Returns:
            The HybridExpr node corresponding to `expr`
        """
        expr_name: str
        child_connection: HybridConnection
        args: list[HybridExpr] = []
        match expr:
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
                ancestor_tree: HybridTree = hybrid
                back_idx: int = 0
                true_steps_back: int = 0
                # Keep stepping backward until `expr.back_levels` non-hidden
                # steps have been taken (to ignore steps that are part of a
                # compound).
                while true_steps_back < expr.back_levels:
                    if ancestor_tree.parent is None:
                        raise NotImplementedError(
                            "TODO: support BACK references that step from a child subtree back into a parent context."
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
                    if not isinstance(arg, PyDoughExpressionAST):
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
                hybrid_arg: HybridExpr
                child_idx: int | None = None
                arg_child_idx: int | None = None
                for arg in expr.args:
                    if isinstance(arg, PyDoughExpressionAST):
                        hybrid_arg, arg_child_idx = self.make_hybrid_agg_expr(
                            hybrid, arg, child_ref_mapping
                        )
                    else:
                        if not isinstance(arg, ChildReferenceCollection):
                            raise NotImplementedError("Cannot process argument")
                        # TODO: handle NDISTINCT
                        if expr.operator == pydop.COUNT:
                            return self.handle_collection_count(
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
            case _:
                raise NotImplementedError(
                    f"TODO: support converting {expr.__class__.__name__}"
                )

    def process_hybrid_collations(
        self,
        hybrid: HybridTree,
        collations: list[CollationExpression],
        child_ref_mapping: dict[int, int],
    ) -> dict[str, HybridExpr]:
        new_expressions: dict[str, HybridExpr] = {}
        hybrid.ordering.clear()
        for i, collation in enumerate(collations):
            new_collation: HybridCollation
            # TODO: Fix the name of the ordering.
            name = f"_ordering_{i}"
            expr = self.make_hybrid_expr(hybrid, collation.expr, child_ref_mapping)
            new_expressions[name] = expr
            new_collation = HybridCollation(
                HybridRefExpr(name, collation.expr.pydough_type),
                collation.asc,
                not collation.na_last,
            )
            hybrid.ordering.append(new_collation)
        return new_expressions

    def make_hybrid_tree(self, node: PyDoughCollectionAST) -> HybridTree:
        """
        Converts a collection AST into the HybridTree format.

        Args:
            `node`: the collection AST to be converted.
            `is_root_of_child`:

        Returns:
            The HybridTree representation of `node`.
        """
        hybrid: HybridTree
        successor_hybrid: HybridTree
        expr: HybridExpr
        child_ref_mapping: dict[int, int] = {}
        match node:
            case GlobalContext():
                return HybridTree(HybridRoot())
            case CompoundSubCollection():
                raise NotImplementedError(f"{node.__class__.__name__}")
            case TableCollection() | SubCollection():
                successor_hybrid = HybridTree(HybridCollectionAccess(node))
                hybrid = self.make_hybrid_tree(node.ancestor_context)
                hybrid.add_successor(successor_hybrid)
                return successor_hybrid
            case Calc():
                hybrid = self.make_hybrid_tree(node.preceding_context)
                self.populate_children(hybrid, node, child_ref_mapping)
                new_expressions: dict[str, HybridExpr] = {}
                for name in sorted(node.calc_terms):
                    expr = self.make_hybrid_expr(
                        hybrid, node.get_expr(name), child_ref_mapping
                    )
                    new_expressions[name] = expr
                hybrid.pipeline.append(HybridCalc(hybrid.pipeline[-1], new_expressions))
                return hybrid
            case Where():
                hybrid = self.make_hybrid_tree(node.preceding_context)
                self.populate_children(hybrid, node, child_ref_mapping)
                expr = self.make_hybrid_expr(hybrid, node.condition, child_ref_mapping)
                hybrid.pipeline.append(HybridFilter(hybrid.pipeline[-1], expr))
                return hybrid
            case OrderBy() | TopK():
                hybrid = self.make_hybrid_tree(node.preceding_context)
                self.populate_children(hybrid, node, child_ref_mapping)
                new_nodes = self.process_hybrid_collations(
                    hybrid, node.collation, child_ref_mapping
                )
                hybrid.pipeline.append(HybridCalc(hybrid.pipeline[-1], new_nodes))
                if isinstance(node, TopK):
                    hybrid.pipeline.append(
                        HybridLimit(
                            hybrid.pipeline[-1],
                            node.records_to_keep,
                            hybrid.ordering.copy(),
                        )
                    )
                return hybrid
            case ChildOperatorChildAccess():
                match node.child_access:
                    case TableCollection() | SubCollection() if not isinstance(
                        node.child_access, CompoundSubCollection
                    ):
                        return HybridTree(HybridCollectionAccess(node.child_access))
                    case _:
                        raise NotImplementedError(
                            f"{node.__class__.__name__} (child is {node.child_access.__class__.__name__})"
                        )
            case _:
                raise NotImplementedError(f"{node.__class__.__name__}")
