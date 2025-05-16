"""
The definitions of the hybrid classes used as an intermediary representation
during QDAG to Relational conversion, as well as the conversion logic from QDAG
nodes to said hybrid nodes.
"""

__all__ = [
    "HybridBackRefExpr",
    "HybridCalculate",
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
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pydough.pydough_operators as pydop
from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseDialect
from pydough.metadata import (
    CartesianProductMetadata,
    GeneralJoinMetadata,
    SimpleJoinMetadata,
    SubcollectionRelationshipMetadata,
)
from pydough.qdag import (
    BackReferenceExpression,
    Calculate,
    ChildOperator,
    ChildOperatorChildAccess,
    ChildReferenceCollection,
    ChildReferenceExpression,
    CollationExpression,
    CollectionAccess,
    ColumnProperty,
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
    SidedReference,
    Singular,
    SubCollection,
    TableCollection,
    TopK,
    Where,
    WindowCall,
)
from pydough.relational import JoinType
from pydough.types import BooleanType, NumericType, PyDoughType


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

    def shift_back(self, levels: int) -> HybridExpr:
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

    def shift_back(self, levels: int) -> HybridExpr:
        if levels == 0:
            return self
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

    def shift_back(self, levels: int) -> None:
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

    def shift_back(self, levels: int) -> HybridExpr:
        return HybridBackRefExpr(self.name, self.back_idx + levels, self.typ)


class HybridSidedRefExpr(HybridExpr):
    """
    Class for HybridExpr terms inside of a general join condition that point to
    the parent side of the join (similar to a correlated reference).
    """

    def __init__(self, name: str, typ: PyDoughType):
        super().__init__(typ)
        self.name: str = name

    def __repr__(self):
        return f"PARENT.{self.name}"

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        return self

    def shift_back(self, levels: int) -> HybridExpr:
        return self


class HybridCorrelExpr(HybridExpr):
    """
    Class for HybridExpr terms that are expressions from a parent hybrid tree
    rather than an ancestor, which requires a correlated reference.
    """

    def __init__(self, hybrid: "HybridTree", expr: HybridExpr):
        super().__init__(expr.typ)
        self.hybrid = hybrid
        self.expr: HybridExpr = expr

    def __repr__(self):
        return f"CORREL({self.expr})"

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        return self

    def shift_back(self, levels: int) -> HybridExpr:
        shifted_expr: HybridExpr | None = self.expr.shift_back(levels)
        assert shifted_expr is not None
        return HybridCorrelExpr(self.hybrid, shifted_expr)


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

    def shift_back(self, levels: int) -> HybridExpr:
        return self


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
        # Shift all of the inputs to the function. Return None if any of them
        # cannot be shifted.
        shifted_args: list[HybridExpr] = []
        for arg in self.args:
            shifted_arg: HybridExpr | None = arg.shift_back(levels)
            if shifted_arg is None:
                return None
            shifted_args.append(shifted_arg)
        return HybridFunctionExpr(self.operator, shifted_args, self.typ)


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
        # Shift all of the inputs to the window function (including regular,
        # partition, and order inputs). Return None if any of them cannot
        # be shifted.
        shifted_args: list[HybridExpr] = []
        shifted_partition_args: list[HybridExpr] = []
        shifted_order_args: list[HybridCollation] = []
        shifted_arg: HybridExpr | None
        for arg in self.args:
            shifted_arg = arg.shift_back(levels)
            if shifted_arg is None:
                return None
            shifted_args.append(shifted_arg)
        for arg in self.partition_args:
            shifted_arg = arg.shift_back(levels)
            if shifted_arg is None:
                return None
            shifted_partition_args.append(shifted_arg)
        for order_arg in self.order_args:
            shifted_arg = order_arg.expr.shift_back(levels)
            if shifted_arg is None:
                return None
            shifted_order_args.append(
                HybridCollation(shifted_arg, order_arg.asc, order_arg.na_first)
            )
        return HybridWindowExpr(
            self.window_func,
            shifted_args,
            shifted_partition_args,
            shifted_order_args,
            self.typ,
            self.kwargs,
        )


def all_same(exprs: list[HybridExpr], renamed_exprs: list[HybridExpr]) -> bool:
    """
    Returns whether two lists of hybrid expressions are identical, down to
    identity.
    """
    return len(exprs) == len(renamed_exprs) and all(
        expr is renamed_expr for expr, renamed_expr in zip(exprs, renamed_exprs)
    )


def shift_join_condition(expr: HybridExpr) -> HybridExpr:
    """
    Shifts an expression used as a general join condition back 1 level, where
    expressions inside of function calls are also shifted, but expressions
    inside correlated references are left alone since they refer to the parent.
    """
    match expr:
        case HybridRefExpr() | HybridBackRefExpr():
            return expr.shift_back(1)
        case HybridFunctionExpr():
            return HybridFunctionExpr(
                expr.operator,
                [shift_join_condition(arg) for arg in expr.args],
                expr.typ,
            )
        case HybridWindowExpr():
            return HybridWindowExpr(
                expr.window_func,
                [shift_join_condition(arg) for arg in expr.args],
                [shift_join_condition(arg) for arg in expr.partition_args],
                [
                    HybridCollation(
                        shift_join_condition(order_arg.expr),
                        order_arg.asc,
                        order_arg.na_first,
                    )
                    for order_arg in expr.order_args
                ],
                expr.typ,
                expr.kwargs,
            )
        case _:
            return expr


class HybridOperation:
    """
    Base class for an operation done within a pipeline of a HybridTree, such
    as a filter or table collection access. Every such class contains the
    following:
    - `terms`: mapping of names to expressions accessible from that point in
               the pipeline execution.
    - `renamings`: mapping of names to a new name that should be used to access
               them from within `terms`. This is used when a `CALCULATE`
               overrides a term name so that future invocations of the term
               name use the renamed version, while key operations like joins
               can still access the original version.
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

    def search_term_definition(self, name: str) -> HybridExpr | None:
        return self.terms.get(name, None)


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
        self.collection: CollectionAccess = collection
        terms: dict[str, HybridExpr] = {}
        for name in collection.calc_terms:
            raw_expr = collection.get_term_from_property(name)
            assert isinstance(raw_expr, ColumnProperty)
            terms[name] = HybridColumnExpr(raw_expr)
        unique_exprs: list[HybridExpr] = []
        for name in sorted(collection.unique_terms, key=str):
            expr: PyDoughExpressionQDAG = collection.get_expr(name)
            unique_exprs.append(HybridRefExpr(name, expr.pydough_type))
        self.general_condition: HybridExpr | None = None
        super().__init__(terms, {}, [], unique_exprs)

    def __repr__(self):
        return f"COLLECTION[{self.collection.name}]"


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


class HybridCalculate(HybridOperation):
    """
    Class for HybridOperation corresponding to a CALCULATE operation.
    """

    def __init__(
        self,
        predecessor: HybridOperation,
        new_expressions: dict[str, HybridExpr],
        orderings: list[HybridCollation],
    ):
        self.predecessor: HybridOperation = predecessor
        terms: dict[str, HybridExpr] = {}
        renamings: dict[str, str] = {}
        for name, expr in predecessor.terms.items():
            terms[name] = HybridRefExpr(name, expr.typ)
        renamings.update(predecessor.renamings)
        new_renamings: dict[str, str] = {}
        for name, expr in new_expressions.items():
            if name in terms and terms[name] == expr:
                continue
            expr = expr.apply_renamings(predecessor.renamings)
            used_name: str = name
            idx: int = 0
            while (
                used_name in terms
                or used_name in renamings
                or used_name in new_renamings
            ):
                if (
                    (used_name not in renamings)
                    and (used_name not in new_renamings)
                    and (self.predecessor.search_term_definition(used_name) == expr)
                ):
                    break
                used_name = f"{name}_{idx}"
                idx += 1
                new_renamings[name] = used_name
            terms[used_name] = expr
        renamings.update(new_renamings)
        for old_name, new_name in new_renamings.items():
            expr = new_expressions.pop(old_name)
            new_expressions[new_name] = expr
        super().__init__(terms, renamings, orderings, predecessor.unique_exprs)
        self.new_expressions = new_expressions

    def __repr__(self):
        return f"CALCULATE[{self.new_expressions}]"

    def search_term_definition(self, name: str) -> HybridExpr | None:
        if name in self.new_expressions:
            expr: HybridExpr = self.new_expressions[name]
            if not (isinstance(expr, HybridRefExpr) and expr.name == name):
                return self.new_expressions[name]
        return self.predecessor.search_term_definition(name)


class HybridFilter(HybridOperation):
    """
    Class for HybridOperation corresponding to a WHERE operation.
    """

    def __init__(self, predecessor: HybridOperation, condition: HybridExpr):
        super().__init__(
            predecessor.terms,
            predecessor.renamings,
            predecessor.orderings,
            predecessor.unique_exprs,
        )
        self.predecessor: HybridOperation = predecessor
        self.condition: HybridExpr = condition

    def __repr__(self):
        return f"FILTER[{self.condition}]"

    def search_term_definition(self, name: str) -> HybridExpr | None:
        return self.predecessor.search_term_definition(name)


class HybridChildPullUp(HybridOperation):
    """
    Class for HybridOperation corresponding to evaluating all of the logic from
    a child subtree of the current pipeline then treating it as the current
    level.
    """

    def __init__(
        self,
        hybrid: "HybridTree",
        child_idx: int,
        original_child_height: int,
    ):
        self.child: HybridConnection = hybrid.children[child_idx]
        self.child_idx: int = child_idx
        self.pullup_remapping: dict[HybridExpr, HybridExpr] = {}

        # Find the level from the child tree that is the equivalent of the
        # level from the child tree that is being replaced.
        current_level: HybridTree = self.child.subtree
        for _ in range(original_child_height):
            assert current_level.parent is not None
            current_level = current_level.parent

        # Snapshot the renamings from the current level, and use its unique
        # terms as the unique terms for this level.
        renamings: dict[str, str] = current_level.pipeline[-1].renamings
        unique_exprs: list[HybridExpr] = []
        for unique_expr in current_level.pipeline[-1].unique_exprs:
            new_unique_expr: HybridExpr | None = unique_expr.shift_back(
                original_child_height
            )
            assert new_unique_expr is not None
            unique_exprs.append(new_unique_expr)

        # Start by adding terms from the bottom level of the child as child ref
        # expressions accessible from the parent.
        terms: dict[str, HybridExpr] = {}
        for term_name, term_expr in current_level.pipeline[-1].terms.items():
            child_ref: HybridChildRefExpr = HybridChildRefExpr(
                term_name, child_idx, term_expr.typ
            )
            terms[term_name] = child_ref

        # Iterate through the level identified earlier & its ancestors to find
        # all of their terms and add them to the parent via accesses to
        # backreferences from the child. These terms are placed in the pullup
        # remapping dictionary so to provide hints on how to translate
        # expressions with regards to the parent level into lookups from within
        # the child subtree.
        extra_height: int = 0
        agg_idx: int = 0
        while True:
            current_terms: dict[str, HybridExpr] = current_level.pipeline[-1].terms
            for term_name in sorted(current_terms):
                # Identify the expression that is being accessed from one of
                # the levels of the child subtree.
                current_expr: HybridExpr = HybridRefExpr(
                    term_name, current_terms[term_name].typ
                )
                shifted_expr: HybridExpr | None = current_expr.shift_back(extra_height)
                assert shifted_expr is not None
                current_expr = shifted_expr
                back_expr: HybridExpr = HybridBackRefExpr(
                    term_name,
                    original_child_height + extra_height,
                    current_terms[term_name].typ,
                )
                if self.child.connection_type.is_aggregation:
                    # If aggregating, wrap the backreference in an ANYTHING
                    # call that is added to the agg calls list so it can be
                    # passed through the aggregation.
                    passthrough_agg: HybridFunctionExpr = HybridFunctionExpr(
                        pydop.ANYTHING, [back_expr], back_expr.typ
                    )
                    agg_name: str
                    # If the aggregation already exists, use it. Otherwise
                    # insert a new aggregation.
                    if passthrough_agg in self.child.aggs.values():
                        agg_name = self.child.fetch_agg_name(passthrough_agg)
                    else:
                        agg_name = f"agg_{agg_idx}"
                        while (
                            agg_name in self.child.aggs
                            or agg_name in self.child.subtree.pipeline[-1].terms
                        ):
                            agg_idx += 1
                            agg_name = f"agg_{agg_idx}"
                        self.child.aggs[agg_name] = passthrough_agg
                        self.pullup_remapping[current_expr] = HybridRefExpr(
                            agg_name, back_expr.typ
                        )
                else:
                    # Otherwise, add an access to the backreference to the
                    # pullup remapping.
                    self.pullup_remapping[current_expr] = back_expr
            if current_level.parent is None:
                break
            current_level = current_level.parent
            extra_height += 1

        super().__init__(terms, renamings, [], unique_exprs)

    def __repr__(self):
        return f"PULLUP[${self.child_idx}: {self.pullup_remapping}]"


class HybridNoop(HybridOperation):
    """
    Class for HybridOperation corresponding to a no-op.
    """

    def __init__(self, last_operation: HybridOperation):
        super().__init__(
            last_operation.terms,
            last_operation.renamings,
            last_operation.orderings,
            last_operation.unique_exprs,
        )

    def __repr__(self):
        return "NOOP"


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
            predecessor.terms,
            predecessor.renamings,
            predecessor.orderings,
            predecessor.unique_exprs,
        )
        self.predecessor: HybridOperation = predecessor
        self.records_to_keep: int = records_to_keep

    def __repr__(self):
        return f"LIMIT_{self.records_to_keep}[{self.orderings}]"

    def search_term_definition(self, name: str) -> HybridExpr | None:
        return self.predecessor.search_term_definition(name)


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
    used to join the parent tree output onto the subtree output.

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

    def fetch_agg_name(self, call: HybridFunctionExpr) -> str:
        """
        Returns the name of an aggregation call within the connection. Throws
        an error if the aggregation call is not found.

        Args:
            `call`: The aggregation call whose name within the child connection
            is being sought.

        Returns:
            The string `name` such that `self.aggs[name]` returns call.

        Raises:
            `ValueError`: if the aggregation call is not found.
        """
        try:
            return next(name for name, agg in self.aggs.items() if agg == call)
        except StopIteration:
            raise ValueError(f"Aggregation call {call} not found in {self.aggs}")


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
        ancestral_mapping: dict[str, int],
        is_hidden_level: bool = False,
        is_connection_root: bool = False,
    ):
        self._pipeline: list[HybridOperation] = [root_operation]
        self._children: list[HybridConnection] = []
        self._ancestral_mapping: dict[str, int] = dict(ancestral_mapping)
        self._successor: HybridTree | None = None
        self._parent: HybridTree | None = None
        self._is_hidden_level: bool = is_hidden_level
        self._is_connection_root: bool = is_connection_root
        self._agg_keys: list[HybridExpr] | None = None
        self._join_keys: list[tuple[HybridExpr, HybridExpr]] | None = None
        self._general_join_condition: HybridExpr | None = None
        self._correlated_children: set[int] = set()
        if isinstance(root_operation, HybridPartition):
            self._join_keys = []

    def __repr__(self):
        lines = []
        if self.parent is not None:
            lines.extend(repr(self.parent).splitlines())
        lines.append(" -> ".join(repr(operation) for operation in self.pipeline))
        prefix = " " if self.successor is None else "↓"
        for idx, child in enumerate(self.children):
            lines.append(f"{prefix} child #{idx} ({child.connection_type.name}):")
            if child.subtree.agg_keys is not None:
                lines.append(f"{prefix}  aggregate: {child.subtree.agg_keys}")
            if len(child.aggs):
                lines.append(f"{prefix}  aggs: {child.aggs}:")
            if child.subtree.join_keys is not None:
                lines.append(f"{prefix}  join: {child.subtree.join_keys}")
            if child.subtree.general_join_condition is not None:
                lines.append(f"{prefix}  join: {child.subtree.general_join_condition}")
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
    def ancestral_mapping(self) -> dict[str, int]:
        """
        The mapping used to identify terms that are references to an alias
        defined in an ancestor.
        """
        return self._ancestral_mapping

    @property
    def correlated_children(self) -> set[int]:
        """
        The set of indices of children that contain correlated references to
        the current hybrid tree.
        """
        return self._correlated_children

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

    @property
    def general_join_condition(self) -> HybridExpr | None:
        """
        A hybrid expression used as a general join condition joining this
        HybridTree to its ancestor, if it is the base of a HybridConnection.
        """
        return self._general_join_condition

    @general_join_condition.setter
    def general_join_condition(self, condition: HybridExpr) -> None:
        """
        Assigns the general join condition to a hybrid tree.
        """
        self._general_join_condition = condition

    def add_child(
        self,
        child: "HybridTree",
        connection_type: ConnectionType,
        original_correlated_children: set[int],
    ) -> int:
        """
        Adds a new child operation to the current level so that operations in
        the pipeline can make use of it.

        Args:
            `child`: the subtree to be connected to `self` as a child
            (starting at the bottom of the subtree).
            `connection_type`: enum indicating what kind of connection is to be
            used to link `self` to `child`.
            `original_correlated_children`: the set of indices of children that
            contain correlated references to the current hybrid tree before
            adding the child.

        Returns:
            The index of the newly inserted child (or the index of an existing
            child that matches it).
        """
        for idx, existing_connection in enumerate(self.children):
            if (
                child == existing_connection.subtree
                and (child.join_keys, child.general_join_condition, child.agg_keys)
                == (
                    existing_connection.subtree.join_keys,
                    existing_connection.subtree.general_join_condition,
                    existing_connection.subtree.agg_keys,
                )
            ) or (
                isinstance(self.pipeline[0], HybridPartition)
                and (child.parent is None)
                and (len(child.pipeline) == 1)
                and isinstance(child.pipeline[0], HybridPartitionChild)
            ):
                connection_type = connection_type.reconcile_connection_types(
                    existing_connection.connection_type
                )
                existing_connection.connection_type = connection_type
                if len(self.correlated_children) > len(original_correlated_children):
                    self._correlated_children = original_correlated_children
                return idx
        connection: HybridConnection = HybridConnection(
            self, child, connection_type, len(self.pipeline) - 1, {}
        )
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
        # Do the same for the general join condition, if one is present.
        if self.agg_keys is not None:
            successor_agg_keys: list[HybridExpr] = []
            for key in self.agg_keys:
                shifted_expr = key.shift_back(1)
                assert shifted_expr is not None
                successor_agg_keys.append(shifted_expr)
            successor.agg_keys = successor_agg_keys
        if self.join_keys is not None:
            successor_join_keys: list[tuple[HybridExpr, HybridExpr]] = []
            for lhs_key, rhs_key in self.join_keys:
                shifted_expr = rhs_key.shift_back(1)
                assert shifted_expr is not None
                successor_join_keys.append((lhs_key, shifted_expr))
            successor.join_keys = successor_join_keys
        if self.general_join_condition is not None:
            shifted_join_condition: HybridExpr | None = (
                self.general_join_condition.shift_back(1)
            )
            assert shifted_join_condition is not None
            successor.general_join_condition = shifted_join_condition


class HybridTranslator:
    """
    Class used to translate PyDough QDAG nodes into the HybridTree structure.
    """

    def __init__(self, configs: PyDoughConfigs, dialect: DatabaseDialect):
        self.configs = configs
        # An index used for creating fake column names for aliases
        self.alias_counter: int = 0
        # A stack where each element is a hybrid tree being derived
        # as as subtree of the previous element, and the current tree is
        # being derived as the subtree of the last element.
        self.stack: list[HybridTree] = []
        # If True, rewrites MEDIAN calls into an average of the 1-2 median rows
        # via window functions, otherwise leaves as-is.
        self.rewrite_median: bool = dialect not in {DatabaseDialect.ANSI}

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
                for window_arg in expr.args:
                    HybridTranslator.identify_connection_types(
                        window_arg, child_idx, reference_types, inside_aggregation
                    )
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
                            # If the argument is a reference to a child,
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

    def inject_expression(
        self, hybrid: HybridTree, expr: HybridExpr, create_new_calc: bool
    ) -> HybridExpr:
        """
        Injects a hybrid expression into the HybridTree's terms, returning
        the new name it was injected with.

        Args:
            `hybrid`: the base of the HybridTree to inject the expression into.
            `expr`: the expression to be injected.
            `create_new_calc`: if True, injects the expression into a new
            CALCULATE operation. If False, injects the expression into the
            last CALCULATE operation in the pipeline, if there is one at the
            end, otherwise creates a new one.

        Returns:
            The HybridExpr corresponding to the injected expression.
        """
        name: str = self.get_internal_name("expr", [hybrid.pipeline[-1].terms])
        if isinstance(hybrid.pipeline[-1], HybridCalculate) and not create_new_calc:
            hybrid.pipeline[-1].terms[name] = expr
            hybrid.pipeline[-1].new_expressions[name] = expr
        else:
            hybrid.pipeline.append(
                HybridCalculate(
                    hybrid.pipeline[-1],
                    {name: expr},
                    hybrid.pipeline[-1].orderings,
                )
            )
        return HybridRefExpr(name, expr.typ)

    def eject_aggregate_inputs(self, hybrid: HybridTree) -> None:
        """
        Ensures that any inputs to aggregation calls are only references to
        columns from the subtree of the hybrid connection being aggregated.

        Args:
            `hybrid`: the base of the HybridTree to eject the aggregate inputs
            from. The ancestors & children of `hybrid` must also be processed.
        """
        if hybrid.parent is not None:
            self.eject_aggregate_inputs(hybrid.parent)
        for child in hybrid.children:
            self.eject_aggregate_inputs(child.subtree)
            create_new_calc: bool = True
            for agg_name, agg_call in sorted(child.aggs.items()):
                rewritten: bool = False
                new_args: list[HybridExpr] = []
                for arg in agg_call.args:
                    if isinstance(arg, HybridRefExpr):
                        new_args.append(arg)
                    else:
                        rewritten = True
                        new_args.append(
                            self.inject_expression(child.subtree, arg, create_new_calc)
                        )
                        create_new_calc = False
                if rewritten:
                    child.aggs[agg_name] = HybridFunctionExpr(
                        agg_call.operator,
                        new_args,
                        agg_call.typ,
                    )

    def run_rewrites(self, hybrid: HybridTree):
        """
        Run any rewrite procedures that must occur after de-correlation, such
        as converting MEDIAN to an average of the 1-2 median rows.

        Args:
            `hybrid`: the bottom of the hybrid tree to rewrite.
        """
        # Recursively proceed on the ancestors & children
        if hybrid.parent is not None:
            self.run_rewrites(hybrid.parent)
        for child in hybrid.children:
            self.run_rewrites(child.subtree)

        create_new_calc: bool = True
        # Rewrite any MEDIAN calls
        if self.rewrite_median:
            for child in hybrid.children:
                for agg_name, agg_call in child.aggs.items():
                    if agg_call.operator == pydop.MEDIAN:
                        child.aggs[agg_name] = self.rewrite_median_call(
                            child, agg_call, create_new_calc
                        )
                        create_new_calc = False

    def populate_children(
        self,
        hybrid: HybridTree,
        child_operator: ChildOperator,
        child_idx_mapping: dict[int, int],
    ) -> None:
        """
        Helper utility that takes any children of a child operator (CALCULATE,
        WHERE, etc.) and builds the corresponding HybridTree subtree,
        where the parent of the subtree's root is absent instead of the
        current level, and inserts the corresponding HybridConnection node.

        Args:
            `hybrid`: the HybridTree having children added to it.
            `child_operator`: the collection QDAG node (CALCULATE, WHERE, etc.)
            containing the children.
            `child_idx_mapping`: a mapping of indices of children of the
            original `child_operator` to the indices of children of the hybrid
            tree level, since the hybrid tree contains the children of all
            pipeline operators of the current level and therefore the indices
            get changes. When the child is inserted, this mapping is mutated
            accordingly so expressions using the child indices know what hybrid
            connection index to use.
        """
        self.stack.append(hybrid)
        for child_idx, child in enumerate(child_operator.children):
            # Infer how the child is used by the current operation based on
            # the expressions that the operator uses.
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
                case Calculate():
                    for expr in child_operator.calc_term_values.values():
                        self.identify_connection_types(expr, child_idx, reference_types)
                case PartitionBy():
                    reference_types.add(ConnectionType.AGGREGATION)
            # Combine the various references to the child to identify the type
            # of connection and add the child. If it already exists, the index
            # of the existing child will be used instead, but the connection
            # type will be updated to reflect the new invocation of the child.
            if len(reference_types) == 0:
                raise ValueError(
                    f"Bad call to populate_children: child {child_idx} of {child_operator} is never used"
                )
            connection_type: ConnectionType = reference_types.pop()
            for con_typ in reference_types:
                connection_type = connection_type.reconcile_connection_types(con_typ)
            # Build the hybrid tree for the child. Before doing so, reset the
            # alias counter to 0 to ensure that identical subtrees are named
            # in the same manner. Afterwards, reset the alias counter to its
            # value within this context.
            snapshot: int = self.alias_counter
            self.alias_counter = 0
            original_correlated_children: set[int] = set(hybrid.correlated_children)
            subtree: HybridTree = self.make_hybrid_tree(
                child, hybrid, connection_type.is_aggregation
            )
            back_exprs: dict[str, HybridExpr] = {}
            for name in subtree.ancestral_mapping:
                # Skip adding backrefs for terms that remain part of the
                # ancestry through the PARTITION, since this creates an
                # unecessary correlation.
                if (
                    name in hybrid.ancestral_mapping
                    or name in hybrid.pipeline[-1].terms
                ):
                    continue
                hybrid_back_expr = self.make_hybrid_expr(
                    subtree,
                    child.get_expr(name),
                    {},
                    False,
                )
                back_exprs[name] = hybrid_back_expr
            if len(back_exprs):
                subtree.pipeline.append(
                    HybridCalculate(
                        subtree.pipeline[-1],
                        back_exprs,
                        subtree.pipeline[-1].orderings,
                    )
                )
            self.alias_counter = snapshot
            child_idx_mapping[child_idx] = hybrid.add_child(
                subtree, connection_type, original_correlated_children
            )
        self.stack.pop()

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
        # with `DEFAULT_TO(x, 0)`. Also, always does this step with
        # COUNT/NDISTINCT for left joins since the semantics of those functions
        # never allow returning NULL.
        if (
            (agg_call.operator == pydop.SUM and self.configs.sum_default_zero)
            or (agg_call.operator == pydop.AVG and self.configs.avg_default_zero)
            or (
                agg_call.operator in (pydop.COUNT, pydop.NDISTINCT)
                and joins_can_nullify
            )
        ):
            agg_ref = HybridFunctionExpr(
                pydop.DEFAULT_TO,
                [agg_ref, HybridLiteralExpr(Literal(0, NumericType()))],
                agg_call.typ,
            )
        return agg_ref

    def gen_agg_name(self, connection: "HybridConnection") -> str:
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
        self, prefix: str, reserved_names: list[Iterable[str]]
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
        assert expr.operator == pydop.COUNT, (
            f"Malformed call to handle_collection_count: {expr}"
        )
        assert len(expr.args) == 1, f"Malformed call to handle_collection_count: {expr}"
        collection_arg = expr.args[0]
        assert isinstance(collection_arg, ChildReferenceCollection), (
            f"Malformed call to handle_collection_count: {expr}"
        )
        count_call: HybridFunctionExpr = HybridFunctionExpr(
            pydop.COUNT, [], expr.pydough_type
        )
        child_idx: int = child_ref_mapping[collection_arg.child_idx]
        child_connection: HybridConnection = hybrid.children[child_idx]
        # Generate a unique name for the agg call to push into the child
        # connection. If the call already exists, reuse the existing name.
        agg_name: str
        if count_call in child_connection.aggs.values():
            agg_name = child_connection.fetch_agg_name(count_call)
        else:
            agg_name = self.gen_agg_name(child_connection)
            child_connection.aggs[agg_name] = count_call
        result_ref: HybridExpr = HybridChildRefExpr(
            agg_name, child_idx, expr.pydough_type
        )
        # The null-adding join is not done if this is the root level, since
        # that just means all the aggregations are no-groupby aggregations.
        joins_can_nullify: bool = not (
            isinstance(hybrid.pipeline[0], HybridRoot)
            or child_connection.connection_type.is_semi
        )
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
        assert isinstance(collection_arg, ChildReferenceCollection), (
            f"Malformed call to handle_has_hasnot: {expr}"
        )
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

    def convert_agg_arg(self, expr: HybridExpr, child_indices: set[int]) -> HybridExpr:
        """
        Translates a hybrid expression that is an argument to an aggregation
        (or a subexpression of such an argument) into a form that is expressed
        from the perspective of the child subtree that is being aggregated.

        Args:
            `expr`: the expression to be converted.
            `child_indices`: a set that is mutated to contain the indices of
            any children that are referenced by `expr`.

        Returns:
            The translated expression.

        Raises:
            NotImplementedError if `expr` is an expression that cannot be used
            inside of an aggregation call.
        """
        match expr:
            case HybridLiteralExpr():
                return expr
            case HybridChildRefExpr():
                # Child references become regular references because the
                # expression is phrased as if we were inside the child rather
                # than the parent.
                child_indices.add(expr.child_idx)
                return HybridRefExpr(expr.name, expr.typ)
            case HybridFunctionExpr():
                return HybridFunctionExpr(
                    expr.operator,
                    [self.convert_agg_arg(arg, child_indices) for arg in expr.args],
                    expr.typ,
                )
            case HybridBackRefExpr():
                raise NotImplementedError(
                    "PyDough does yet support aggregations whose arguments mix between subcollection data of the current context and fields of an ancestor of the current context"
                )
            case HybridRefExpr():
                raise NotImplementedError(
                    "PyDough does yet support aggregations whose arguments mix between subcollection data of the current context and fields of the context itself"
                )
            case HybridWindowExpr():
                raise NotImplementedError(
                    "PyDough does yet support aggregations whose arguments mix between subcollection data of the current context and window functions"
                )
            case _:
                raise NotImplementedError(
                    f"TODO: support converting {expr.__class__.__name__} in aggregations"
                )

    def make_agg_call(
        self,
        hybrid: HybridTree,
        expr: ExpressionFunctionCall,
        args: list[HybridExpr],
    ) -> HybridExpr:
        """
        For aggregate function calls, their arguments are translated in a
        manner that identifies what child subtree they correspond too, by
        index, and translates them relative to the subtree. Then, the
        aggregation calls are placed into the `aggs` mapping of the
        corresponding child connection, and the aggregation call becomes a
        child reference (referring to the aggs list), since after translation,
        an aggregated child subtree only has the grouping keys and the
        aggregation calls as opposed to its other terms.

        Args:
            `hybrid`: the hybrid tree that should be used to derive the
            translation of the aggregation call.
            `expr`: the aggregation function QDAG expression to be converted.
            `args`: the converted arguments to the aggregation call.
        """
        child_indices: set[int] = set()
        converted_args: list[HybridExpr] = [
            self.convert_agg_arg(arg, child_indices) for arg in args
        ]
        if len(child_indices) != 1:
            raise ValueError(
                f"Expected aggregation call to contain references to exactly one child collection, but found {len(child_indices)} in {expr}"
            )
        hybrid_call: HybridFunctionExpr = HybridFunctionExpr(
            expr.operator, converted_args, expr.pydough_type
        )
        # Identify the child connection that the aggregation call is pushed
        # into.
        child_idx: int = child_indices.pop()
        child_connection: HybridConnection = hybrid.children[child_idx]
        # If the aggregation already exists in the child, use a child reference
        # to it.
        agg_name: str
        if hybrid_call in child_connection.aggs.values():
            agg_name = child_connection.fetch_agg_name(hybrid_call)
        else:
            # Otherwise, Generate a unique name for the agg call to push into the
            # child connection.
            agg_name = self.gen_agg_name(child_connection)
            child_connection.aggs[agg_name] = hybrid_call
        result_ref: HybridExpr = HybridChildRefExpr(
            agg_name, child_idx, expr.pydough_type
        )
        joins_can_nullify: bool = not (
            isinstance(hybrid.pipeline[0], HybridRoot)
            or child_connection.connection_type.is_semi
        )
        return self.postprocess_agg_output(hybrid_call, result_ref, joins_can_nullify)

    def rewrite_median_call(
        self,
        child_connection: HybridConnection,
        expr: HybridFunctionExpr,
        create_new_calc: bool,
    ) -> HybridFunctionExpr:
        """
        Transforms a MEDIAN call into an AVG of the 1-2 median rows
        (obtained via window functions). This step must be done after
        de-correlation because it invokes the aggregation keys used for the
        child connection, which may change during de-correlation.

        Args:
            `child`: the child connection containing the aggregate call to
            MEDIAN as one of its aggs.
            `expr`: the aggregation function QDAG expression to be converted.
            `create_new_calc`: if True, creates a new CALCULATE when injecting
            the inputs to the AVG call into the child.
        """
        assert expr.operator == pydop.MEDIAN
        # Build an expression that makes all rows null except the 1-2 median
        # rows. The formula to find the kept rows is the following:
        #   ABS((r - 1) - (n - 1) / 2) < 1
        # Where `r` is the row number (sorted by the median column) and `n`
        # is the number of non-null rows of the median column. The window
        # functions are computed with the same partitioning keys that will be
        # used to aggregate the child connection.
        assert len(expr.args) == 1
        data_expr: HybridExpr = expr.args[0]
        one: HybridExpr = HybridLiteralExpr(Literal(1.0, NumericType()))
        two: HybridExpr = HybridLiteralExpr(Literal(2.0, NumericType()))
        assert child_connection.subtree.agg_keys is not None
        partition_args: list[HybridExpr] = child_connection.subtree.agg_keys
        order_args: list[HybridCollation] = [HybridCollation(data_expr, False, False)]
        rank: HybridExpr = HybridWindowExpr(
            pydop.RANKING, [], partition_args, order_args, NumericType(), {}
        )
        rows: HybridExpr = HybridWindowExpr(
            pydop.RELCOUNT, [data_expr], partition_args, [], NumericType(), {}
        )
        adjusted_rank: HybridExpr = HybridFunctionExpr(
            pydop.SUB, [rank, one], NumericType()
        )
        adjusted_rows: HybridExpr = HybridFunctionExpr(
            pydop.SUB, [rows, one], NumericType()
        )
        centerpoint: HybridExpr = HybridFunctionExpr(
            pydop.DIV, [adjusted_rows, two], NumericType()
        )
        distance_from_center = HybridFunctionExpr(
            pydop.ABS,
            [
                HybridFunctionExpr(
                    pydop.SUB, [adjusted_rank, centerpoint], NumericType()
                )
            ],
            NumericType(),
        )
        is_median_row: HybridExpr = HybridFunctionExpr(
            pydop.LET, [distance_from_center, one], BooleanType()
        )
        median_rows_arg: HybridExpr = HybridFunctionExpr(
            pydop.KEEP_IF, [data_expr, is_median_row], data_expr.typ
        )
        # Build a call to AVG on those 1-2 rows.
        median_rows_arg = self.inject_expression(
            child_connection.subtree, median_rows_arg, create_new_calc
        )
        avg_call: HybridFunctionExpr = HybridFunctionExpr(
            pydop.AVG, [median_rows_arg], expr.typ
        )

        return avg_call

    def make_hybrid_correl_expr(
        self,
        back_expr: BackReferenceExpression,
        collection: PyDoughCollectionQDAG,
        steps_taken_so_far: int,
    ) -> HybridCorrelExpr:
        """
        Converts a BACK reference into a correlated reference when the number
        of BACK levels exceeds the height of the current subtree.

        Args:
            `back_expr`: the original BACK reference to be converted.
            `collection`: the collection at the top of the current subtree,
            before we have run out of BACK levels to step up out of.
            `steps_taken_so_far`: the number of steps already taken to step
            up from the BACK node. This is needed so we know how many steps
            still need to be taken upward once we have stepped out of the child
            subtree back into the parent subtree.
        """
        if len(self.stack) == 0:
            raise ValueError("Back reference steps too far back")
        # Identify the parent subtree that the BACK reference is stepping back
        # into, out of the child.
        parent_tree: HybridTree = self.stack.pop()
        remaining_steps_back: int = back_expr.back_levels - steps_taken_so_far - 1
        parent_result: HybridExpr
        new_expr: PyDoughExpressionQDAG
        # Special case: stepping out of the data argument of PARTITION back
        # into its ancestor. For example:
        # TPCH.CALCULATE(x=...).PARTITION(data.WHERE(y > BACK(1).x), ...)
        partition_edge_case: bool = len(parent_tree.pipeline) == 1 and isinstance(
            parent_tree.pipeline[0], HybridPartition
        )
        if partition_edge_case:
            assert parent_tree.parent is not None
            # Treat the partition's parent as the context for the back
            # to step into, as opposed to the partition itself (so the back
            # levels are consistent)
            self.stack.append(parent_tree.parent)
            parent_result = self.make_hybrid_correl_expr(
                back_expr, collection, steps_taken_so_far
            ).expr
            self.stack.pop()
        elif remaining_steps_back == 0:
            # If there are no more steps back to be made, then the correlated
            # reference is to a reference from the current context.
            if back_expr.term_name in parent_tree.ancestral_mapping:
                new_expr = BackReferenceExpression(
                    collection,
                    back_expr.term_name,
                    parent_tree.ancestral_mapping[back_expr.term_name],
                )
                parent_result = self.make_hybrid_expr(parent_tree, new_expr, {}, False)
            elif back_expr.term_name in parent_tree.pipeline[-1].terms:
                parent_name: str = parent_tree.pipeline[-1].renamings.get(
                    back_expr.term_name, back_expr.term_name
                )
                parent_result = HybridRefExpr(parent_name, back_expr.pydough_type)
            else:
                raise ValueError(
                    f"Back reference to {back_expr.term_name} not found in parent"
                )
        else:
            # Otherwise, a back reference needs to be made from the current
            # collection a number of steps back based on how many steps still
            # need to be taken, and it must be recursively converted to a
            # hybrid expression that gets wrapped in a correlated reference.
            new_expr = BackReferenceExpression(
                collection, back_expr.term_name, remaining_steps_back
            )
            parent_result = self.make_hybrid_expr(parent_tree, new_expr, {}, False)
        if not isinstance(parent_result, HybridCorrelExpr):
            parent_tree.correlated_children.add(len(parent_tree.children))
        # Restore parent_tree back onto the stack, since evaluating `back_expr`
        # does not change the program's current placement in the subtrees.
        self.stack.append(parent_tree)
        # Create the correlated reference to the expression with regards to
        # the parent tree, which could also be a correlated expression.
        return HybridCorrelExpr(parent_tree, parent_result)

    def add_unique_terms(
        self,
        hybrid: HybridTree,
        levels_remaining: int,
        levels_so_far: int,
        partition_args: list[HybridExpr],
        child_idx: int | None,
    ) -> None:
        """
        Populates a list of partition keys with the unique terms of an ancestor
        level of the hybrid tree.

        Args:
            `hybrid`: the hybrid tree whose ancestor's unique terms are being
            added to the partition keys.
            `levels_remaining`: the number of levels left to step back before
            the unique terms are added to the partition keys.
            `levels_so_far`: the number of levels that have been stepped back
            so far.
            `partition_args`: the list of partition keys that is being
            populated with the unique terms of the ancestor level.
            `child_idx`: the index to use when identifying that a child node
            has become correlated. If not provided, uses the value from the
            top of the stack.
        """
        # When the number of levels remaining to step back is 0, we have
        # reached the targeted ancestor, so we add the unique terms.
        if levels_remaining == 0:
            for unique_term in sorted(hybrid.pipeline[-1].unique_exprs, key=str):
                shifted_arg: HybridExpr | None = unique_term.shift_back(levels_so_far)
                assert shifted_arg is not None
                partition_args.append(shifted_arg)
        elif hybrid.parent is None:
            # If we have not reached the target level yet, but we have reached
            # the top level of the tree, we need to step out of a child subtree
            # back into its parent and make a correlated reference.
            if len(self.stack) == 0:
                raise ValueError("Window function references too far back")
            prev_hybrid: HybridTree = self.stack.pop()
            correl_args: list[HybridExpr] = []
            self.add_unique_terms(
                prev_hybrid, levels_remaining - 1, 0, correl_args, child_idx
            )
            join_remapping: dict[HybridExpr, HybridExpr] = dict(
                hybrid.join_keys if hybrid.join_keys is not None else []
            )
            for arg in correl_args:
                if arg in join_remapping:
                    # Special case: if the uniqueness key is also a join key
                    # from the LHS, use the equivalent key from the RHS.
                    equivalent_key: HybridExpr | None = join_remapping[arg].shift_back(
                        levels_so_far
                    )
                    assert equivalent_key is not None
                    partition_args.append(equivalent_key)
                else:
                    # Otherwise, create a correlated reference to the term.
                    if not isinstance(arg, HybridCorrelExpr):
                        if child_idx is not None:
                            prev_hybrid.correlated_children.add(child_idx)
                        else:
                            prev_hybrid.correlated_children.add(
                                len(prev_hybrid.children)
                            )
                    partition_args.append(HybridCorrelExpr(prev_hybrid, arg))
            self.stack.append(prev_hybrid)
        else:
            # Otherwise, we have to step back further, so we recursively
            # repeat the procedure one level further up in the hybrid tree.
            self.add_unique_terms(
                hybrid.parent,
                levels_remaining - 1,
                levels_so_far + 1,
                partition_args,
                child_idx,
            )

    def make_hybrid_expr(
        self,
        hybrid: HybridTree,
        expr: PyDoughExpressionQDAG,
        child_ref_mapping: dict[int, int],
        inside_agg: bool,
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
            `inside_agg`: True if `expr` is being derived is inside of an
            aggregation call, False otherwise.

        Returns:
            The HybridExpr node corresponding to `expr`
        """
        expr_name: str
        child_connection: HybridConnection
        args: list[HybridExpr] = []
        hybrid_arg: HybridExpr
        ancestor_tree: HybridTree
        collection: PyDoughCollectionQDAG
        match expr:
            case PartitionKey():
                return self.make_hybrid_expr(
                    hybrid, expr.expr, child_ref_mapping, inside_agg
                )
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
            case SidedReference():
                if expr.is_parent:
                    return HybridSidedRefExpr(expr.term_name, expr.pydough_type)
                else:
                    return HybridRefExpr(expr.term_name, expr.pydough_type)
            case BackReferenceExpression():
                # A reference to an expression from an ancestor becomes a
                # reference to one of the terms of a parent level of the hybrid
                # tree. If the BACK goes far enough that it must step outside
                # a child subtree into the parent, a correlated reference is
                # created.
                ancestor_tree = hybrid
                back_idx: int = 0
                true_steps_back: int = 0
                # Keep stepping backward until `expr.back_levels` non-hidden
                # steps have been taken.
                collection = expr.collection
                while true_steps_back < expr.back_levels:
                    assert collection.ancestor_context is not None
                    collection = collection.ancestor_context
                    if ancestor_tree.parent is None:
                        return self.make_hybrid_correl_expr(
                            expr, collection, true_steps_back
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
                if hybrid.ancestral_mapping.get(expr.term_name, 0) > 0:
                    collection = expr.collection
                    while (
                        isinstance(collection, PartitionChild)
                        and expr.term_name in collection.child_access.ancestral_mapping
                    ):
                        collection = collection.child_access
                    return self.make_hybrid_expr(
                        hybrid,
                        BackReferenceExpression(
                            collection,
                            expr.term_name,
                            hybrid.ancestral_mapping[expr.term_name],
                        ),
                        child_ref_mapping,
                        inside_agg,
                    )
                expr_name = hybrid.pipeline[-1].renamings.get(
                    expr.term_name, expr.term_name
                )
                return HybridRefExpr(expr_name, expr.pydough_type)
            case ExpressionFunctionCall():
                if expr.operator.is_aggregation and inside_agg:
                    raise NotImplementedError(
                        "PyDough does not yet support calling aggregations inside of aggregations"
                    )
                # Do special casing for operators that an have collection
                # arguments.
                # TODO: (gh #148) handle collection-level NDISTINCT
                if (
                    expr.operator == pydop.COUNT
                    and len(expr.args) == 1
                    and isinstance(expr.args[0], PyDoughCollectionQDAG)
                ):
                    return self.handle_collection_count(hybrid, expr, child_ref_mapping)
                elif expr.operator in (pydop.HAS, pydop.HASNOT):
                    return self.handle_has_hasnot(hybrid, expr, child_ref_mapping)
                elif any(
                    not isinstance(arg, PyDoughExpressionQDAG) for arg in expr.args
                ):
                    raise NotImplementedError(
                        f"PyDough does not yet support non-expression arguments for aggregation function {expr.operator}"
                    )
                # For normal operators, translate their expression arguments
                # normally. If it is a non-aggregation, build the function
                # call. If it is an aggregation, transform accordingly.
                # such function that takes in a collection, as none currently
                # exist that are not aggregations.
                for arg in expr.args:
                    if not isinstance(arg, PyDoughExpressionQDAG):
                        raise NotImplementedError(
                            f"PyDough does not yet support non-expression arguments for function {expr.operator}"
                        )
                    args.append(
                        self.make_hybrid_expr(
                            hybrid,
                            arg,
                            child_ref_mapping,
                            inside_agg or expr.operator.is_aggregation,
                        )
                    )
                if expr.operator.is_aggregation:
                    return self.make_agg_call(hybrid, expr, args)
                else:
                    return HybridFunctionExpr(expr.operator, args, expr.pydough_type)
            case WindowCall():
                partition_args: list[HybridExpr] = []
                order_args: list[HybridCollation] = []
                # If the levels argument was provided, find the partition keys
                # for that ancestor level.
                if expr.levels is not None:
                    self.add_unique_terms(hybrid, expr.levels, 0, partition_args, None)
                # Convert all of the window function arguments to hybrid
                # expressions.
                for arg in expr.args:
                    args.append(
                        self.make_hybrid_expr(
                            hybrid, arg, child_ref_mapping, inside_agg
                        )
                    )
                # Convert all of the ordering terms to hybrid expressions.
                for col_arg in expr.collation_args:
                    hybrid_arg = self.make_hybrid_expr(
                        hybrid, col_arg.expr, child_ref_mapping, inside_agg
                    )
                    order_args.append(
                        HybridCollation(hybrid_arg, col_arg.asc, col_arg.na_last)
                    )
                # Build the new hybrid window function call with all the
                # converted terms.
                return HybridWindowExpr(
                    expr.window_operator,
                    args,
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
            a `CALCULATE` and a list of the new HybridCollation values.
        """
        new_expressions: dict[str, HybridExpr] = {}
        hybrid_orderings: list[HybridCollation] = []
        name: str
        expr: HybridExpr
        for collation in collations:
            if type(collation.expr) is Reference:
                name = collation.expr.term_name
            else:
                name = self.get_ordering_name(hybrid)
                expr = self.make_hybrid_expr(
                    hybrid, collation.expr, child_ref_mapping, False
                )
                new_expressions[name] = expr
            new_collation: HybridCollation = HybridCollation(
                HybridRefExpr(name, collation.expr.pydough_type),
                collation.asc,
                not collation.na_last,
            )
            hybrid_orderings.append(new_collation)
        return new_expressions, hybrid_orderings

    def make_hybrid_tree(
        self,
        node: PyDoughCollectionQDAG,
        parent: HybridTree | None,
        is_aggregate: bool = False,
    ) -> HybridTree:
        """
        Converts a collection QDAG into the HybridTree format.

        Args:
            `node`: the collection QDAG to be converted.
            `parent`: optional hybrid tree of the parent context that `node` is
            a child of.
            `is_aggregate`: True if the node is being aggregated with regards
            to `parent`, False otherwise.

        Returns:
            The HybridTree representation of `node`.
        """
        hybrid: HybridTree
        subtree: HybridTree
        successor_hybrid: HybridTree
        expr: HybridExpr
        child_ref_mapping: dict[int, int] = {}
        key_exprs: list[HybridExpr] = []
        join_key_exprs: list[tuple[HybridExpr, HybridExpr]] = []
        general_join_cond: HybridExpr | None = None
        collection_access: HybridCollectionAccess
        match node:
            case GlobalContext():
                return HybridTree(HybridRoot(), node.ancestral_mapping)
            case TableCollection() | SubCollection():
                collection_access = HybridCollectionAccess(node)
                successor_hybrid = HybridTree(collection_access, node.ancestral_mapping)
                # If accessing a sub-collection with a general join condition,
                # populate the general_condition field of the sub-collection
                # access with the general join condition converted from a QDAG
                # expression to a hybrid expression.
                if isinstance(node, SubCollection) and isinstance(
                    node.subcollection_property, GeneralJoinMetadata
                ):
                    assert node.general_condition is not None
                    collection_access.general_condition = self.make_hybrid_expr(
                        successor_hybrid,
                        node.general_condition,
                        {},
                        False,
                    )
                hybrid = self.make_hybrid_tree(
                    node.ancestor_context, parent, is_aggregate
                )
                hybrid.add_successor(successor_hybrid)
                return successor_hybrid
            case PartitionChild():
                hybrid = self.make_hybrid_tree(
                    node.ancestor_context, parent, is_aggregate
                )
                # Identify the original data being partitioned, which may
                # require stepping in multiple times if the partition is
                # nested inside another partition.
                src_tree: HybridTree = hybrid
                while isinstance(src_tree.pipeline[0], HybridPartitionChild):
                    src_tree = src_tree.pipeline[0].subtree
                subtree = src_tree.children[0].subtree
                successor_hybrid = HybridTree(
                    HybridPartitionChild(subtree),
                    node.ancestral_mapping,
                )
                hybrid.add_successor(successor_hybrid)
                return successor_hybrid
            case Calculate():
                hybrid = self.make_hybrid_tree(
                    node.preceding_context, parent, is_aggregate
                )
                self.populate_children(hybrid, node, child_ref_mapping)
                new_expressions: dict[str, HybridExpr] = {}
                for name in sorted(node.calc_terms):
                    expr = self.make_hybrid_expr(
                        hybrid, node.get_expr(name), child_ref_mapping, False
                    )
                    new_expressions[name] = expr
                hybrid.pipeline.append(
                    HybridCalculate(
                        hybrid.pipeline[-1],
                        new_expressions,
                        hybrid.pipeline[-1].orderings,
                    )
                )
                return hybrid
            case Singular():
                # a Singular node is just used to annotate the preceding context
                # with additional information with respect to parent context.
                # This information is no longer needed (as it has been used in
                # conversion from Unqualified to QDAG), so it can be discarded
                # and replaced with the preceding context.
                hybrid = self.make_hybrid_tree(
                    node.preceding_context, parent, is_aggregate
                )
                return hybrid
            case Where():
                hybrid = self.make_hybrid_tree(
                    node.preceding_context, parent, is_aggregate
                )
                self.populate_children(hybrid, node, child_ref_mapping)
                expr = self.make_hybrid_expr(
                    hybrid, node.condition, child_ref_mapping, False
                )
                hybrid.pipeline.append(HybridFilter(hybrid.pipeline[-1], expr))
                return hybrid
            case PartitionBy():
                hybrid = self.make_hybrid_tree(
                    node.ancestor_context, parent, is_aggregate
                )
                partition: HybridPartition = HybridPartition()
                successor_hybrid = HybridTree(partition, node.ancestral_mapping)
                hybrid.add_successor(successor_hybrid)
                self.populate_children(successor_hybrid, node, child_ref_mapping)
                partition_child_idx: int = child_ref_mapping[0]
                for key_name in sorted(node.calc_terms, key=str):
                    key = node.get_expr(key_name)
                    expr = self.make_hybrid_expr(
                        successor_hybrid, key, child_ref_mapping, False
                    )
                    partition.add_key(key_name, expr)
                    key_exprs.append(HybridRefExpr(key_name, expr.typ))
                successor_hybrid.children[
                    partition_child_idx
                ].subtree.agg_keys = key_exprs
                return successor_hybrid
            case OrderBy() | TopK():
                hybrid = self.make_hybrid_tree(
                    node.preceding_context, parent, is_aggregate
                )
                self.populate_children(hybrid, node, child_ref_mapping)
                new_nodes: dict[str, HybridExpr]
                hybrid_orderings: list[HybridCollation]
                new_nodes, hybrid_orderings = self.process_hybrid_collations(
                    hybrid, node.collation, child_ref_mapping
                )
                hybrid.pipeline.append(
                    HybridCalculate(hybrid.pipeline[-1], new_nodes, hybrid_orderings)
                )
                if isinstance(node, TopK):
                    hybrid.pipeline.append(
                        HybridLimit(hybrid.pipeline[-1], node.records_to_keep)
                    )
                return hybrid
            case ChildOperatorChildAccess():
                assert parent is not None
                match node.child_access:
                    case TableCollection() | SubCollection():
                        collection_access = HybridCollectionAccess(node.child_access)
                        successor_hybrid = HybridTree(
                            collection_access,
                            node.ancestral_mapping,
                        )
                        if isinstance(node.child_access, SubCollection):
                            sub_property: SubcollectionRelationshipMetadata = (
                                node.child_access.subcollection_property
                            )
                            if isinstance(sub_property, SimpleJoinMetadata):
                                # For a simple join access, populate the join
                                # keys of the tree which will be bubbled
                                # down throughout the entire child tree.
                                join_key_exprs = HybridTranslator.get_join_keys(
                                    parent,
                                    node.child_access.subcollection_property,
                                    successor_hybrid.pipeline[-1],
                                )
                            elif isinstance(sub_property, GeneralJoinMetadata):
                                # For general join, do the same except with the
                                # general condition instead of equi-join keys.
                                assert node.child_access.general_condition is not None
                                collection_access.general_condition = (
                                    general_join_cond
                                ) = self.make_hybrid_expr(
                                    successor_hybrid,
                                    node.child_access.general_condition,
                                    {},
                                    False,
                                )
                            elif isinstance(sub_property, CartesianProductMetadata):
                                pass
                            else:
                                raise NotImplementedError(
                                    f"Unsupported metadata type for subcollection access: {sub_property.__class__.__name__}"
                                )
                    case PartitionChild():
                        source: HybridTree = parent
                        if isinstance(source.pipeline[0], HybridPartitionChild):
                            source = source.pipeline[0].subtree
                        successor_hybrid = HybridTree(
                            HybridPartitionChild(source.children[0].subtree),
                            node.ancestral_mapping,
                        )
                        partition_by = (
                            node.child_access.ancestor_context.starting_predecessor
                        )
                        assert isinstance(partition_by, PartitionBy)
                        for key in partition_by.keys:
                            rhs_expr: HybridExpr = self.make_hybrid_expr(
                                successor_hybrid,
                                Reference(node.child_access, key.expr.term_name),
                                child_ref_mapping,
                                False,
                            )
                            assert isinstance(
                                rhs_expr, (HybridRefExpr, HybridBackRefExpr)
                            )
                            lhs_expr: HybridExpr = HybridChildRefExpr(
                                rhs_expr.name, 0, rhs_expr.typ
                            )
                            join_key_exprs.append((lhs_expr, rhs_expr))

                    case PartitionBy():
                        partition = HybridPartition()
                        successor_hybrid = HybridTree(partition, node.ancestral_mapping)
                        self.populate_children(
                            successor_hybrid, node.child_access, child_ref_mapping
                        )
                        partition_child_idx = child_ref_mapping[0]
                        for key_name in node.calc_terms:
                            key = node.get_expr(key_name)
                            expr = self.make_hybrid_expr(
                                successor_hybrid, key, child_ref_mapping, False
                            )
                            partition.add_key(key_name, expr)
                            key_exprs.append(HybridRefExpr(key_name, expr.typ))
                        successor_hybrid.children[
                            partition_child_idx
                        ].subtree.agg_keys = key_exprs
                    case _:
                        raise NotImplementedError(
                            f"{node.__class__.__name__} (child is {node.child_access.__class__.__name__})"
                        )
                if general_join_cond is None:
                    # For a simple join, add the join keys to the child
                    # and make the RHS of those keys the agg keys.
                    successor_hybrid.agg_keys = [
                        rhs_key for _, rhs_key in join_key_exprs
                    ]
                    successor_hybrid.join_keys = join_key_exprs
                else:
                    # For a general join, instead use the general join
                    # condition. However, if an aggregate is being performed
                    # on the child, the aggregation keys are the uniqueness
                    # keys from the lhs of the join condition, which means
                    # a calculate must be added to the child that accesses
                    # those keys as correlated references, then uses those
                    # terms from the calculate in the agg keys.
                    if is_aggregate:
                        lhs_unique_keys: dict[str, HybridExpr] = {}
                        key_exprs = []
                        back_levels: int = 0
                        current_level: HybridTree | None = parent
                        # First, find the uniqueness keys from every level of
                        # the parent and add them to lhs_unique_keys as a
                        # correlated reference.
                        while current_level is not None:
                            for expr in current_level.pipeline[-1].unique_exprs:
                                key_name = f"key_{len(lhs_unique_keys)}"
                                shifted_expr: HybridExpr | None = expr.shift_back(
                                    back_levels
                                )
                                assert shifted_expr is not None
                                expr = HybridCorrelExpr(parent, shifted_expr)
                                lhs_unique_keys[key_name] = expr
                            back_levels += 1
                            current_level = current_level.parent
                        # Insert the calculate to access these correlated
                        # keys, then add references to the new terms to
                        # the agg keys.
                        successor_hybrid.pipeline.append(
                            HybridCalculate(
                                successor_hybrid.pipeline[0], lhs_unique_keys, []
                            )
                        )
                        for key_name, expr in lhs_unique_keys.items():
                            key_exprs.append(HybridRefExpr(key_name, expr.typ))
                        # Mark the parent's connection to this child as
                        # correlated, and set the agg keys.
                        parent.correlated_children.add(len(parent.children))
                        successor_hybrid.agg_keys = key_exprs
                    successor_hybrid.general_join_condition = general_join_cond
                return successor_hybrid
            case _:
                raise NotImplementedError(f"{node.__class__.__name__}")
