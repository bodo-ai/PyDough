"""
Utilities used by PyDough test files, such as the TestInfo classes used to
build QDAG nodes for unit tests.
"""

from types import NoneType

from dateutil import parser  # type: ignore[import-untyped]

__all__ = [
    "AstNodeTestInfo",
    "BackReferenceExpressionInfo",
    "CalculateInfo",
    "ChildReferenceExpressionInfo",
    "ColumnInfo",
    "FunctionInfo",
    "LiteralInfo",
    "OrderInfo",
    "PartitionInfo",
    "ReferenceInfo",
    "SingularInfo",
    "SubCollectionInfo",
    "TableCollectionInfo",
    "TopKInfo",
    "WhereInfo",
    "graph_fetcher",
    "map_over_dict_values",
]

import datetime
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pandas as pd
import pytest

import pydough
import pydough.pydough_operators as pydop
from pydough import init_pydough_context, to_df, to_sql
from pydough.configs import PyDoughConfigs
from pydough.conversion import convert_ast_to_relational
from pydough.database_connectors import DatabaseContext
from pydough.errors import PyDoughTestingException
from pydough.evaluation.evaluate_unqualified import _load_column_selection
from pydough.metadata import GraphMetadata
from pydough.pydough_operators import get_operator_by_name
from pydough.qdag import (
    AstNodeBuilder,
    ChildOperatorChildAccess,
    ChildReferenceExpression,
    CollationExpression,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    PyDoughQDAG,
    Singular,
)
from pydough.relational import (
    ColumnReference,
    ExpressionSortInfo,
    LiteralExpression,
    RelationalExpression,
    RelationalRoot,
    Scan,
)
from pydough.types import PyDoughType, UnknownType
from pydough.unqualified import (
    UnqualifiedNode,
    qualify_node,
)

# Type alias for a function that takes in a string and generates metadata
# for a graph based on it.
graph_fetcher = Callable[[str], GraphMetadata]


def map_over_dict_values(
    dictionary: dict[Any, Any], func: Callable[[Any], Any]
) -> dict[Any, Any]:
    """
    Applies a lambda function to the values of a dictionary, returning a
    new dictionary with the transformation applied.

    Args:
        `dictionary`: The input dictionary whose values are to be transformed.
        `func`: The lambda to call that transforms each value in `dictionary`.

    Returns:
        The transformed dictionary, with the same keys as `dictionary`.
    """
    return {key: func(val) for key, val in dictionary.items()}


class AstNodeTestInfo(ABC):
    """
    Base class used in tests to specify information about a QDAG node
    before it can be created, e.g. describing column properties or
    function calls by name before a builder can be used to create them.
    """

    @abstractmethod
    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        """
        Uses a passed-in QDAG node builder to construct the node.

        Args:
            `builder`: the builder that should be used to create the QDAG
            objects.
            `context`: an optional collection QDAG used as the context within
            which the QDAG is created.
            `children_contexts`: an optional list of collection QDAGs of
            child nodes of a CALCULATE that are accessible for
            ChildReferenceExpression usage.

        Returns:
            The new instance of the QDAG object.
        """

    def __repr__(self):
        return self.to_string()

    @abstractmethod
    def to_string(self) -> str:
        """
        String representation of the TestInfo before it is built into a QDAG node.
        """


class LiteralInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a PyDough literal. Contains the
    following fields:
    - `value`: the object stored in the literal
    - `data_type`: the PyDough type of the literal
    """

    def __init__(self, value: object, data_type: PyDoughType):
        self.value: object = value
        self.data_type: PyDoughType = data_type

    def to_string(self) -> str:
        return f"Literal[{self.value!r}]"

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        return builder.build_literal(self.value, self.data_type)


class ColumnInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a table column. Contains the
    following fields:
    - `collection_name`: the name of the collection for the table.
    - `property_name`: the name of the collection property for the column.
    """

    def __init__(self, collection_name: str, property_name: str):
        self.collection_name: str = collection_name
        self.property_name: str = property_name

    def to_string(self) -> str:
        return f"Column[{self.collection_name}.{self.property_name}]"

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        return builder.build_column(self.collection_name, self.property_name)


class FunctionInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a function call. Contains the
    following fields:
    - `function_name`: the name of PyDough operator to be invoked.
    - `args_info`: a list of TestInfo objects used to build the arguments.
    """

    def __init__(self, function_name: str, args_info: list[AstNodeTestInfo]):
        self.function_name: str = function_name
        self.args_info: list[AstNodeTestInfo] = args_info

    def to_string(self) -> str:
        arg_strings: list[str] = [arg.to_string() for arg in self.args_info]
        return f"Call[{self.function_name} on ({', '.join(arg_strings)})]"

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        args: list[PyDoughQDAG] = [
            info.build(builder, context, children_contexts) for info in self.args_info
        ]
        operator = get_operator_by_name(self.function_name)
        return builder.build_expression_function_call(operator, args)


class WindowInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a window call. Contains the
    following fields:
    - `function_name`: the name of the window function to be invoked.
    - `collation`: a list of TestInfo objects used to build the collation
        arguments (passed in via *args).
    - `levels`: the number of levels upward to reference (passed in via
        kwargs).
    - `kwargs`: any additional keyword arguments to the window function call.
    """

    def __init__(self, name: str, *args, **kwargs):
        self.name: str = name
        self.collation: tuple[tuple[AstNodeTestInfo, bool, bool]] = args
        self.levels: int | None = None
        if "levels" in kwargs:
            self.levels = kwargs.pop("levels")
        self.kwargs: dict[str, object] = kwargs

    def to_string(self) -> str:
        collation_strings: list[str] = []
        for info, asc, na_last in self.collation:
            suffix = "ASC" if asc else "DESC"
            kwarg = "'last'" if na_last else "'first'"
            collation_strings.append(f"({info.to_string()}).{suffix}(na_pos={kwarg})")
        kwargs_str: str = ""
        for kwarg, val in self.kwargs.items():
            kwargs_str += f", {kwarg}={val!r}"
        match self.name:
            case "RANKING":
                return f"{self.name}(by=({', '.join(collation_strings)}), levels={self.levels}{kwargs_str})"
            case _:
                raise PyDoughTestingException(
                    f"Unsupported window function {self.name}"
                )

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        assert context is not None, (
            "Cannot call .build() on RankingInfo without providing a context"
        )
        collation_args: list[CollationExpression] = []
        for arg in self.collation:
            expr = arg[0].build(builder, context, children_contexts)
            assert isinstance(expr, PyDoughExpressionQDAG)
            collation_args.append(CollationExpression(expr, arg[1], arg[2]))
        match self.name:
            case "RANKING":
                return builder.build_window_call(
                    pydop.RANKING,
                    [],
                    collation_args,
                    self.levels,
                    self.kwargs,
                )
            case _:
                raise PyDoughTestingException(
                    f"Unsupported window function {self.name}"
                )


class ReferenceInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a reference. Contains the
    following fields:
    - `name`: the name of the term being referenced from the preceding context.
    """

    def __init__(self, name: str):
        self.name: str = name

    def to_string(self) -> str:
        return f"Reference[{self.name}]"

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        assert context is not None, (
            "Cannot call .build() on ReferenceInfo without providing a context"
        )
        typ: PyDoughType = context.get_expr(self.name).pydough_type
        return builder.build_reference(context, self.name, typ)


class BackReferenceExpressionInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a back reference expression.
    Contains the following fields:
    - `name`: the name of the term being referenced.
    - `levels`: the number of levels upward to reference.
    """

    def __init__(self, name: str, levels: int):
        self.name: str = name
        self.levels: int = levels

    def to_string(self) -> str:
        return f"BackReferenceExpression[{self.levels}:{self.name}]"

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        assert context is not None, (
            "Cannot call .build() on BackReferenceExpressionInfo without providing a context"
        )
        return builder.build_back_reference_expression(context, self.name, self.levels)


class ChildReferenceExpressionInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a child reference expression.
    Contains the following fields:
    - `name`: the name of the term being referenced.
    - `child_idx`: the index of the child being referenced.
    """

    def __init__(self, name: str, child_idx: int):
        self.name: str = name
        self.child_idx: int = child_idx

    def to_string(self) -> str:
        return f"${self.child_idx}.{self.name}"

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        assert children_contexts is not None, (
            "Cannot call .build() on ChildReferenceExpressionInfo without providing a list of child contexts"
        )
        return builder.build_child_reference_expression(
            children_contexts, self.child_idx, self.name
        )


class CollectionTestInfo(AstNodeTestInfo):
    """
    Abstract base class for TestInfo implementations that build collections.
    Each implementation the following additional features:
    - `successor`: the TestInfo for a collection node that is to be built using
      the collection node built by this builder as its parent/preceding context.
    - Can use the `**` operator to pipeline collection infos into one another.
    - `build` is already implemented and automatically pipelines into the next
      collection test info. Instead, each implementation class must implement
      a `local_build` method that works like the regular `build`.
    - `to_string` is already implemented and automatically concatenated with
      the next collection test info. Instead, each implementation class must
      implement a `local_string` method that works like the regular `to_string`.
    """

    def __init__(self):
        self.successor: CollectionTestInfo | None = None

    def __pow__(self, other):
        """
        Specifies that `other` is the successor of `self`, meaning that when
        `self.succeed` is called, it uses `self` as the predecessor/parent
        when building `other`.
        """
        assert isinstance(other, CollectionTestInfo), (
            f"can only use ** for pipelining collection info when the right hand side is a collection info, not {other.__class__.__name__}"
        )
        self.successor = other
        return self

    @abstractmethod
    def local_string(self) -> str:
        """
        String representation of the TestInfo before it is built into a QDAG node.
        """

    def to_string(self) -> str:
        local_result: str = self.local_string()
        if self.successor is None:
            return local_result
        return f"{local_result} -> {self.successor.to_string()}"

    @abstractmethod
    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        """
        Uses a passed-in QDAG node builder to construct the collection node.

        Args:
            `builder`: the builder that should be used to create the QDAG
            objects.
            `context`: an optional collection QDAG used as the context within
            which the QDAG is created.
            `children_contexts`: an optional list of collection QDAG of child
            nodes of a CALCULATE that are accessible for
            ChildReferenceExpression usage.

        Returns:
            The new instance of the collection QDAG object.
        """

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        local_result: PyDoughCollectionQDAG = self.local_build(
            builder, context, children_contexts
        )
        if self.successor is None:
            return local_result
        return self.successor.build(builder, local_result, children_contexts)


class TableCollectionInfo(CollectionTestInfo):
    """
    CollectionTestInfo implementation class to build a table collection.
    Contains the following fields:
    - `name`: the name of the table collection within the graph.
    """

    def __init__(self, name: str):
        super().__init__()
        self.name: str = name

    def local_string(self) -> str:
        return f"Table[{self.name}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            context = builder.build_global_context()
        return builder.build_child_access(self.name, context)


class SubCollectionInfo(TableCollectionInfo):
    """
    CollectionTestInfo implementation class to create a subcollection access,
    either as a direct subcollection or via a compound relationship. Contains
    the following fields:
    - `name`: the name of the subcollection property within the collection.

    NOTE: must provide a `context` when building.
    """

    def local_string(self) -> str:
        return f"SubCollection[{self.name}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        assert context is not None, (
            "Cannot call .build() on ReferenceInfo without providing a context"
        )
        return builder.build_child_access(self.name, context)


class ChildOperatorChildAccessInfo(CollectionTestInfo):
    """
    CollectionTestInfo implementation class that wraps around a subcollection
    info within a CALCULATE context. Contains the following fields:
    - `child_info`: the collection info for the child subcollection.

    NOTE: must provide a `context` when building.
    """

    def __init__(self, child_info: CollectionTestInfo):
        self.child_info: CollectionTestInfo = child_info
        self.successor = child_info.successor

    def local_string(self) -> str:
        return f"ChildSubCollection[{self.child_info!r}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        assert context is not None, (
            "Cannot call .build() on ReferenceInfo without providing a context"
        )
        access: PyDoughCollectionQDAG = self.child_info.local_build(
            builder, context, children_contexts
        )
        return ChildOperatorChildAccess(access)


class ChildReferenceCollectionInfo(CollectionTestInfo):
    """
    CollectionTestInfo implementation class to build a reference to a
    child collection. Contains the following fields:
    - `idx`: the index of the child collection being referenced.

    NOTE: must provide a `context` when building.
    """

    def __init__(self, idx: int):
        super().__init__()
        self.idx: int = idx

    def local_string(self) -> str:
        return f"ChildReferenceCollection[{self.idx}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        assert context is not None, (
            "Cannot call .build() on ChildReferenceCollection without providing a context"
        )
        assert children_contexts is not None, (
            "Cannot call .build() on ChildReferenceCollection without providing a list of child contexts"
        )
        return builder.build_child_reference_collection(
            context, children_contexts, self.idx
        )


class ChildOperatorInfo(CollectionTestInfo):
    """
    Base class for types of CollectionTestInfo that have child nodes, such as
    CALCULATE or WHERE.  Contains the following fields:
    - `children_info`: a list of CollectionTestInfo objects that will be used
       to build the child contexts.
    """

    def __init__(self, children: list[CollectionTestInfo]):
        super().__init__()
        self.children_info: list[CollectionTestInfo] = children

    def child_strings(self) -> str:
        """
        Returns the string representations of all the children in a way that
        can be easily used in each `local_string` implementation.
        """
        child_strings: list[str] = []
        for idx, child in enumerate(self.children_info):
            child_strings.append(f"${idx}: {child.to_string()}")
        if len(self.children_info) == 0:
            return ""
        return "\n" + "\n".join(child_strings) + "\n"

    def build_children(
        self, builder: AstNodeBuilder, context: PyDoughCollectionQDAG | None = None
    ) -> list[PyDoughCollectionQDAG]:
        """
        Builds all of the child infos into the children of the operator.

        Args:
            `builder`: the builder that should be used to create the QDAG
            objects.
            `context`: an optional collection QDAG used as the context within
            which the QDAG is created.

        Returns:
            The list of built child collections.
        """
        children: list[PyDoughCollectionQDAG] = []
        for idx, child_info in enumerate(self.children_info):
            child = ChildOperatorChildAccessInfo(child_info).build(builder, context)
            assert isinstance(child, PyDoughCollectionQDAG)
            children.append(child)
        return children


class CalculateInfo(ChildOperatorInfo):
    """
    CollectionTestInfo implementation class to build a CALCULATE node.
    Contains the following fields:
    - `children_info`: a list of CollectionTestInfo objects that will be used
       to build the child contexts.
    - `args`: a list tuples containing a field name and a test info to derive
       an expression in the CALCULATE. Passed in via keyword arguments to the
       constructor, where the argument names are the field names and the
       argument values are the expression infos.
    """

    def __init__(self, children: list[CollectionTestInfo], **kwargs):
        super().__init__(children)
        self.args: list[tuple[str, AstNodeTestInfo]] = list(kwargs.items())

    def local_string(self) -> str:
        args_strings: list[str] = [
            f"{name}={arg.to_string()}" for name, arg in self.args
        ]
        return f"Calculate[{self.child_strings()}{', '.join(args_strings)}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            context = builder.build_global_context()
        children: list[PyDoughCollectionQDAG] = self.build_children(
            builder,
            context,
        )
        args: list[tuple[str, PyDoughExpressionQDAG]] = []
        for name, info in self.args:
            expr = info.build(builder, context, children)
            assert isinstance(expr, PyDoughExpressionQDAG)
            args.append((name, expr))
        return builder.build_calculate(context, children, args)


class WhereInfo(ChildOperatorInfo):
    """
    CollectionTestInfo implementation class to build a WHERE clause.
    Contains the following fields:
    - `condition`: a test info describing the predicate for the WHERE clause.

    NOTE: must provide a `context` when building.
    """

    def __init__(self, children: list[CollectionTestInfo], condition: AstNodeTestInfo):
        super().__init__(children)
        self.condition: AstNodeTestInfo = condition

    def local_string(self) -> str:
        return f"WHERE[{self.child_strings()}{self.condition.to_string()}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            raise PyDoughTestingException(
                "Must provide a context when building a WHERE clause."
            )
        children: list[PyDoughCollectionQDAG] = self.build_children(builder, context)
        cond = self.condition.build(builder, context, children)
        assert isinstance(cond, PyDoughExpressionQDAG)
        return builder.build_where(context, children, cond)


class SingularInfo(ChildOperatorInfo):
    """
    CollectionTestInfo implementation class to build a SINGULAR clause.
    Contains the following fields:
    - `condition`: a test info describing the predicate for the WHERE clause.

    NOTE: must provide a `context` when building.
    """

    def __init__(
        self,
    ):
        super().__init__([])

    def local_string(self) -> str:
        return f"SINGULAR[{self.child_strings()}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            raise PyDoughTestingException(
                "Must provide a context when building a Singular clause."
            )
        raw_singular: Singular = builder.build_singular(context)
        return raw_singular


class OrderInfo(ChildOperatorInfo):
    """
    CollectionTestInfo implementation class to build a ORDERBEY clause.
    Contains the following fields:
    - `collations`: a list of tuples in the form `(test_info, asc, na_last)`
      ordering keys for the ORDER BY clause. Passed in via variadic arguments.

    NOTE: must provide a `context` when building.
    """

    def __init__(
        self,
        children: list[CollectionTestInfo],
        *args,
    ):
        super().__init__(children)
        self.collation: tuple[tuple[AstNodeTestInfo, bool, bool]] = args

    def local_string(self) -> str:
        collation_strings: list[str] = []
        for info, asc, na_last in self.collation:
            suffix = "ASC" if asc else "DESC"
            kwarg = "'last'" if na_last else "'first'"
            collation_strings.append(f"({info.to_string()}).{suffix}(na_pos={kwarg})")
        return f"OrderBy[{self.child_strings()}{', '.join(collation_strings)}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            raise PyDoughTestingException(
                "Must provide context and children_contexts when building an ORDER BY clause."
            )
        children: list[PyDoughCollectionQDAG] = self.build_children(builder, context)
        collation: list[CollationExpression] = []
        for info, asc, na_last in self.collation:
            expr = info.build(builder, context, children)
            assert isinstance(expr, PyDoughExpressionQDAG)
            collation.append(CollationExpression(expr, asc, na_last))
        return builder.build_order(context, children, collation)


class TopKInfo(ChildOperatorInfo):
    """
    CollectionTestInfo implementation class to build a TOP K clause.
    Contains the following fields:
    - `records_to_keep`: the `K` value in TOP K.
    - `collations`: a list of tuples in the form `(test_info, asc, na_last)`
      ordering keys for the ORDER BY clause. Passed in via variadic arguments.

    NOTE: must provide a `context` when building.
    """

    def __init__(
        self,
        children: list[CollectionTestInfo],
        records_to_keep: int,
        *args,
    ):
        super().__init__(children)
        self.records_to_keep: int = records_to_keep
        self.collation: tuple[tuple[AstNodeTestInfo, bool, bool]] = args

    def local_string(self) -> str:
        collation_strings: list[str] = []
        for info, asc, na_last in self.collation:
            suffix = "ASC" if asc else "DESC"
            kwarg = "'last'" if na_last else "'first'"
            collation_strings.append(f"({info.to_string()}).{suffix}(na_pos={kwarg})")
        return f"TopK[{self.child_strings()}{self.records_to_keep}, {', '.join(collation_strings)}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            raise PyDoughTestingException(
                "Must provide context and children_contexts when building a TOPK clause."
            )
        children: list[PyDoughCollectionQDAG] = self.build_children(builder, context)
        collation: list[CollationExpression] = []
        for info, asc, na_last in self.collation:
            expr = info.build(builder, context, children)
            assert isinstance(expr, PyDoughExpressionQDAG)
            collation.append(CollationExpression(expr, asc, na_last))
        return builder.build_top_k(context, children, self.records_to_keep, collation)


class PartitionInfo(ChildOperatorInfo):
    """
    CollectionTestInfo implementation class to build a PARTITION BY clause.
    Contains the following fields:
    - `child_name`: the name used to access the child.
    - `keys`: a list of test info for the keys to partition on.
    """

    def __init__(
        self,
        child: CollectionTestInfo,
        name: str,
        keys: list[AstNodeTestInfo],
    ):
        super().__init__([child])
        self.name: str = name
        self.keys: list[AstNodeTestInfo] = keys

    def local_string(self) -> str:
        key_strings_tup: tuple = tuple([key.to_string() for key in self.keys])
        return f"PartitionBy[{self.child_strings()}name={self.name!r}, by={key_strings_tup}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: list[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            context = builder.build_global_context()
        children: list[PyDoughCollectionQDAG] = self.build_children(builder, context)
        assert len(children) == 1
        keys: list[ChildReferenceExpression] = []
        for info in self.keys:
            expr = info.build(builder, context, children)
            assert isinstance(expr, ChildReferenceExpression)
            keys.append(expr)
        return builder.build_partition(context, children[0], self.name, keys)


def make_relational_column_reference(
    name: str, typ: PyDoughType | None = None, input_name: str | None = None
) -> ColumnReference:
    """
    Make a column reference given name and type. This is used
    for generating various relational nodes.

    Args:
        `name`: The name of the column in the input.
        `typ`: The PyDoughType of the column. Defaults to
            None.
        `input_name`: The name of the input node. This is
            used by Join to differentiate between the left and right.
            Defaults to None.

    Returns:
        The output column.
    """
    pydough_type = typ if typ is not None else UnknownType()
    return ColumnReference(name, pydough_type, input_name)


def make_relational_literal(value: Any, typ: PyDoughType | None = None):
    """
    Make a literal given value and type. This is used for
    generating various relational nodes.

    Args:
        `value`: The value of the literal.

    Returns:
        The output literal.
    """
    pydough_type = typ if typ is not None else UnknownType()
    return LiteralExpression(value, pydough_type)


def build_simple_scan() -> Scan:
    """
    Build a simple scan node for reuse in tests.

    Returns:
        The Scan node.
    """
    return Scan(
        "table",
        {
            "a": make_relational_column_reference("a"),
            "b": make_relational_column_reference("b"),
        },
    )


def make_relational_ordering(
    expr: RelationalExpression, ascending: bool = True, nulls_first: bool = True
):
    """
    Create an ordering as a function of a Relational column reference
    with the given ascending and nulls_first parameters.

    Args:
        `expr`: The expression used as a sorting key.
        `ascending`: Whether the ordering is ascending or descending.
        `nulls_first`: Whether the ordering places nulls first or last.

    Returns:
        The column ordering information.
    """
    return ExpressionSortInfo(expr, ascending, nulls_first)


def transform_and_exec_pydough(
    pydough_impl: Callable[..., UnqualifiedNode] | str,
    graph: GraphMetadata,
    kwargs: dict | None,
) -> UnqualifiedNode:
    """
    Obtains the unqualified node from a PyDough function by invoking the
    decorator to transform it (or evaluating the string if provided), then
    calling the transformed function.

    Args:
        `pydough_impl`: The PyDough function to be transformed and executed,
        or the string containing the PyDough code to be executed.
        `graph`: The metadata being used.
        `kwargs`: The keyword arguments to pass to the PyDough function, if
        any.

    Returns:
        The unqualified node created by running the transformed version of
        `pydough_impl`.
    """
    kwargs = kwargs if kwargs is not None else {}
    if isinstance(pydough_impl, str):
        # If the pydough_impl is a string, parse it with pydough.from_string.
        return pydough.from_string(pydough_impl, metadata=graph, environment=kwargs)
    else:
        # OTherwise, transform the function with the decorator and call it.
        return init_pydough_context(graph)(pydough_impl)(**kwargs)


@dataclass
class PyDoughSQLComparisonTest:
    """
    The data packet encapsulating the information to run a PyDough e2e test
    that compares the result against a reference answer derived by executing a
    SQL query.
    """

    pydough_function: Callable[..., UnqualifiedNode]
    """
    Function that returns the PyDough code evaluated by the unit test.
    """

    graph_name: str
    """
    The graph that the PyDough code will use.
    """

    sql_function: Callable[[], str]
    """
    Function that returns the SQL code that should be executed on the database
    to derive the reference solution.
    """

    test_name: str
    """
    The name of the unit test
    """

    columns: dict[str, str] | list[str] | None = None
    """
    If provided, passed in as the columns argument to the `to_sql` or `to_df`
    function.
    """

    order_sensitive: bool = False
    """
    If False, the resulting data frames will be sorted so the order
    of the results is not taken into account
    """

    fix_column_names: bool = True
    """
    If True, ignore whatever column names are in the output and just use the
    same column names as in the reference solution.
    """

    def run_e2e_test(
        self,
        fetcher: graph_fetcher,
        database: DatabaseContext,
        config: PyDoughConfigs | None = None,
        reference_database: DatabaseContext | None = None,
        coerce_types: bool = False,
    ):
        """
        Runs an end-to-end test using the data in the SQL comparison test,
        comparing the result of the PyDough code against the reference solution
        derived by executing the SQL query.

        Args:
            `fetcher`: The function that takes in the name of the graph used
            by the test and fetches the graph metadata.
            `database`: The database context to use for executing SQL.
            `config`: The PyDough configuration to use for the test, if any.
            `reference_database`: The database context to use for executing
                                the reference SQL.
            `coerce_types`: If True, coerces the types of the result and reference
            solution DataFrames to ensure compatibility.
        """
        # Obtain the graph and the unqualified node
        graph: GraphMetadata = fetcher(self.graph_name)
        root: UnqualifiedNode = transform_and_exec_pydough(
            self.pydough_function, graph, None
        )

        # Obtain the DataFrame result from the PyDough code
        call_kwargs: dict = {"metadata": graph, "database": database}
        if config is not None:
            call_kwargs["config"] = config
        if self.columns is not None:
            call_kwargs["columns"] = self.columns
        result: pd.DataFrame = to_df(root, **call_kwargs)

        # Obtain the reference solution by executing the refsol SQL query
        sql_text: str = self.sql_function()
        refsol: pd.DataFrame
        if reference_database is not None:
            refsol = reference_database.connection.execute_query_df(sql_text)
        else:
            refsol = database.connection.execute_query_df(sql_text)

        # If the query does not care about column names, update the answer to use
        # the column names in the refsol.
        if self.fix_column_names:
            assert len(result.columns) == len(refsol.columns)
            result.columns = refsol.columns

        # If the query is not order-sensitive, sort the DataFrames before comparison
        if not self.order_sensitive:
            result = result.sort_values(by=list(result.columns)).reset_index(drop=True)
            refsol = refsol.sort_values(by=list(refsol.columns)).reset_index(drop=True)

        # Harmonize types between result and reference solution
        if coerce_types:
            for col_name in result.columns:
                result[col_name], refsol[col_name] = harmonize_types(
                    result[col_name], refsol[col_name]
                )
        # Perform the comparison between the result and the reference solution
        pd.testing.assert_frame_equal(result, refsol, rtol=1.0e-5, atol=1.0e-5)


@dataclass
class PyDoughPandasTest:
    """
    The data packet encapsulating the information to run a PyDough e2e test
    that compares the result against a reference answer derived by running
    a function that returns a Pandas DataFrame. The dataclass contains the
    following fields:
    - `pydough_function`: the function that returns the PyDough code evaluated
      by the unit test, or a string representing the PyDough code.
    - `graph_name`: the name of the graph that the PyDough code will use.
    - `pd_function`: the function that returns the Pandas DataFrame that should
      be used as the reference solution.
    - `test_name`: the name of the unit test.
    - `columns` (optional): if provided, passed in as the columns argument to
      the `to_sql` or `to_df` function.
    - `order_sensitive` (optional): if False, the resulting data frames will be
      sorted so the order of the results is not taken into account.
    - `fix_column_names` (optional): if True, ignore whatever column names are
      in the output and just use the same column names as in the reference
      solution.
    - `args` (optional): additional arguments to pass to the PyDough function.
    - `skip_relational`: (optional): if True, does not run the test as part of
       relational plan testing. Default is False.
    - `skip_sql`: (optional): if True, does not run the test as part of SQL
       testing. Default is False.
    - `fix_output_dialect`: (optional): update refsol to match Dialect behavior
    """

    pydough_function: Callable[..., UnqualifiedNode] | str
    """
    Function that returns the PyDough code evaluated by the unit test, or a
    string representing the PyDough code.
    """

    graph_name: str
    """
    The graph that the PyDough code will use.
    """

    pd_function: Callable[[], pd.DataFrame]
    """
    Function that returns the SQL code that should be executed on the database
    to derive the reference solution.
    """

    test_name: str
    """
    The name of the unit test
    """

    columns: dict[str, str] | list[str] | None = None
    """
    If provided, passed in as the columns argument to the `to_sql` or `to_df`
    function.
    """

    order_sensitive: bool = False
    """
    If False, the resulting data frames will be sorted so the order
    of the results is not taken into account
    """

    fix_column_names: bool = True
    """
    If True, ignore whatever column names are in the output and just use the
    same column names as in the reference solution.
    """

    kwargs: dict | None = None
    """
    Any additional keyword arguments to pass to the PyDough function when
    executing it. If None, no additional keyword arguments are passed.
    """

    skip_relational: bool = False
    """
    If True, does not run the test as part of relational plan testing.
    """

    skip_sql: bool = False
    """
    If True, does not run the test as part of SQL testing.
    """

    fix_output_dialect: str = "sqlite"
    """
    Dialect name to update output
    """

    def run_relational_test(
        self,
        fetcher: graph_fetcher,
        file_path: str,
        update: bool,
        config: PyDoughConfigs | None = None,
    ) -> None:
        """
        Runs a test on the relational plan code generated by the PyDough code,
        comparing the generated relational plan against the expected SQL stored
        in the reference file.

        Args:
            `fetcher`: The function that takes in the name of the graph used
            by the test and fetches the graph metadata.
            `file_path`: The path to the file containing the expected SQL text.
            `update`: If True, updates the file with the generated relational
            plan text, otherwise compares the generated relational plan text
            against the expected relational plan text in the file.
            `config`: The PyDough configuration to use for the test, if any.
        """
        # Skip if indicated.
        if self.skip_relational:
            pytest.skip(f"Skipping relational plan test for {self.test_name}")

        # Obtain the graph and the unqualified node
        graph: GraphMetadata = fetcher(self.graph_name)
        root: UnqualifiedNode = transform_and_exec_pydough(
            self.pydough_function, graph, self.kwargs
        )

        # Run the PyDough code through the pipeline up until it is converted to
        # a relational plan.
        if config is None:
            config = pydough.active_session.config
        qualified: PyDoughQDAG = qualify_node(root, graph, config)
        assert isinstance(qualified, PyDoughCollectionQDAG), (
            "Expected qualified answer to be a collection, not an expression"
        )
        relational: RelationalRoot = convert_ast_to_relational(
            qualified, _load_column_selection({"columns": self.columns}), config
        )

        # Either update the reference solution, or compare the generated
        # relational plan text against it.
        if update:
            with open(file_path, "w") as f:
                f.write(relational.to_tree_string() + "\n")
        else:
            with open(file_path) as f:
                expected_relational_string: str = f.read()
            assert relational.to_tree_string() == expected_relational_string.strip(), (
                "Mismatch between tree string representation of relational node and expected Relational tree string"
            )

    def run_sql_test(
        self,
        fetcher: graph_fetcher,
        file_path: str,
        update: bool,
        database: DatabaseContext,
        config: PyDoughConfigs | None = None,
    ) -> None:
        """
        Runs a test on the SQL code generated by the PyDough code,
        comparing the generated SQL against the expected SQL stored in
        the reference file.

        Args:
            `fetcher`: The function that takes in the name of the graph used
            by the test and fetches the graph metadata.
            `file_path`: The path to the file containing the expected SQL text.
            `update`: If True, updates the file with the generated SQL text,
            otherwise compares the generated SQL text against the expected SQL
            text in the file.
            `database`: The database context to determine what dialect of SQL
            to use when generating the SQL test.
            `config`: The PyDough configuration to use for the test, if any.
        """
        # Skip if indicated.
        if self.skip_sql:
            pytest.skip(f"Skipping SQL text test for {self.test_name}")

        # Obtain the graph and the unqualified node
        graph: GraphMetadata = fetcher(self.graph_name)
        root: UnqualifiedNode = transform_and_exec_pydough(
            self.pydough_function, graph, self.kwargs
        )

        # Convert the PyDough code to SQL text
        call_kwargs: dict = {"metadata": graph, "database": database}
        if config is not None:
            call_kwargs["config"] = config
        if self.columns is not None:
            call_kwargs["columns"] = self.columns
        sql_text: str = to_sql(root, **call_kwargs)

        # Either update the reference solution, or compare the generated sql
        # text against it.
        if update:
            with open(file_path, "w") as f:
                f.write(sql_text + "\n")
        else:
            with open(file_path) as f:
                expected_sql_text: str = f.read()
            assert sql_text == expected_sql_text.strip(), (
                "Mismatch between SQL text produced expected SQL text"
            )

    def run_e2e_test(
        self,
        fetcher: graph_fetcher,
        database: DatabaseContext,
        config: PyDoughConfigs | None = None,
        display_sql: bool = False,
        coerce_types: bool = False,
    ):
        """
        Runs an end-to-end test using the data in the SQL comparison test,
        comparing the result of the PyDough code against the reference solution
        stored in pd_function.

        Args:
            `fetcher`: The function that takes in the name of the graph used
            by the test and fetches the graph metadata.
            `database`: The database context to use for executing SQL.
            `config`: The PyDough configuration to use for the test, if any.
            `display_sql`: If True, displays the SQL generated by PyDough.
            `coerce_types`: If True, coerces the types of the result and reference
            solution DataFrames to ensure compatibility.
        """
        # Obtain the graph and the unqualified node
        graph: GraphMetadata = fetcher(self.graph_name)
        root: UnqualifiedNode = transform_and_exec_pydough(
            self.pydough_function, graph, self.kwargs
        )
        # Obtain the DataFrame result from the PyDough code
        call_kwargs: dict = {
            "metadata": graph,
            "database": database,
            "display_sql": display_sql,
        }
        if config is not None:
            call_kwargs["config"] = config
        if self.columns is not None:
            call_kwargs["columns"] = self.columns
        result: pd.DataFrame = to_df(root, **call_kwargs)
        # Extract the reference solution from the function
        refsol: pd.DataFrame = self.pd_function()

        # If the query does not care about column names, update the answer to use
        # the column names in the refsol.
        if self.fix_column_names:
            assert len(result.columns) == len(refsol.columns)
            result.columns = refsol.columns

        # FIXME:
        if self.fix_output_dialect == "snowflake":
            # Update column "q"
            # Start of Week in Snowflake is Monday
            if self.test_name == "smoke_b":
                refsol["q"] = [
                    "1994-06-06",
                    "1994-05-23",
                    "1998-02-16",
                    "1993-06-07",
                    "1992-10-19",
                ]

        # If the query is not order-sensitive, sort the DataFrames before comparison
        if not self.order_sensitive:
            result = result.sort_values(by=list(result.columns)).reset_index(drop=True)
            refsol = refsol.sort_values(by=list(refsol.columns)).reset_index(drop=True)

        if coerce_types:
            for col_name in result.columns:
                result[col_name], refsol[col_name] = harmonize_types(
                    result[col_name], refsol[col_name]
                )
        # Perform the comparison between the result and the reference solution
        pd.testing.assert_frame_equal(
            result, refsol, check_dtype=(not coerce_types), check_exact=False, atol=1e-8
        )


def harmonize_types(column_a, column_b):
    """
    Harmonizes data types between two Pandas columns to ensure compatibility
    for comparison equality check operations.

    The function performs type conversions based on common mismatches, including:
    - None to ' ' for string and NoneType columns
    - Decimal to integer conversion
    - Decimal to float conversion
    - String to datetime or date conversion
    - Date to string or datetime conversion

    If no known mismatch pattern is found, the original columns are returned unchanged.

    Parameters:
        `column_a`: The first column to harmonize.
        `column_b`: The second column to harmonize.

    Returns:
        A tuple of the two harmonized columns.
    """
    # Different integer types
    if pd.api.types.is_integer_dtype(column_a) and pd.api.types.is_integer_dtype(
        column_b
    ):
        # cast both to the largest integer type among the two
        max_bits = max(column_a.dtype.itemsize, column_b.dtype.itemsize) * 8
        target_type = getattr(pd, f"Int{max_bits}") if max_bits != 64 else "int64"
        return column_a.astype(target_type), column_b.astype(target_type)

    # bool vs int, convert bool to int.
    if any(isinstance(elem, bool) for elem in column_a) and any(
        isinstance(elem, int) for elem in column_b
    ):
        return column_a.astype(int), column_b
    if any(isinstance(elem, int) for elem in column_a) and any(
        isinstance(elem, bool) for elem in column_b
    ):
        return column_a, column_b.astype(int)

    # int vs float
    if any(isinstance(elem, int) for elem in column_a) and any(
        isinstance(elem, float) for elem in column_b
    ):
        return column_a.astype(float), column_b

    # float vs int
    if any(isinstance(elem, float) for elem in column_a) and any(
        isinstance(elem, int) for elem in column_b
    ):
        return column_a, column_b.astype(float)

    # Decimal vs float
    if any(isinstance(elem, Decimal) for elem in column_a) and any(
        isinstance(elem, float) for elem in column_b
    ):
        return column_a.apply(lambda x: pd.NA if pd.isna(x) else float(x)), column_b

    # float vs Decimal
    if any(isinstance(elem, float) for elem in column_a) and any(
        isinstance(elem, Decimal) for elem in column_b
    ):
        return column_a, column_b.apply(lambda x: pd.NA if pd.isna(x) else float(x))

    if any(isinstance(elem, (str, NoneType)) for elem in column_a) and any(
        isinstance(elem, (str, NoneType)) for elem in column_b
    ):
        return column_a.apply(lambda x: "" if pd.isna(x) else str(x)), column_b.apply(
            lambda x: "" if pd.isna(x) else str(x)
        )
    # float vs None. Convert to nullable floats
    if any(isinstance(elem, (float, NoneType)) for elem in column_a) and any(
        isinstance(elem, (float, NoneType)) for elem in column_b
    ):
        return column_a.astype("Float64"), column_b.astype("Float64")

    if any(isinstance(elem, Decimal) for elem in column_a) and any(
        isinstance(elem, int) for elem in column_b
    ):
        return column_a.apply(lambda x: pd.NA if pd.isna(x) else int(x)), column_b
    if any(isinstance(elem, int) for elem in column_a) and any(
        isinstance(elem, Decimal) for elem in column_b
    ):
        return column_a, column_b.apply(lambda x: pd.NA if pd.isna(x) else int(x))
    if any(isinstance(elem, Decimal) for elem in column_a) and any(
        isinstance(elem, float) for elem in column_b
    ):
        return column_a.apply(lambda x: pd.NA if pd.isna(x) else float(x)), column_b
    if any(isinstance(elem, float) for elem in column_a) and any(
        isinstance(elem, Decimal) for elem in column_b
    ):
        return column_a, column_b.apply(lambda x: pd.NA if pd.isna(x) else float(x))
    if any(isinstance(elem, pd.Timestamp) for elem in column_a) and any(
        isinstance(elem, str) for elem in column_b
    ):
        return column_a, column_b.apply(
            lambda x: pd.NA if pd.isna(x) else pd.Timestamp(x)
        )
    if any(isinstance(elem, str) for elem in column_a) and any(
        isinstance(elem, datetime.date) for elem in column_b
    ):
        return column_a.apply(
            lambda x: pd.NA if pd.isna(x) else pd.Timestamp(x)
        ), column_b
    if any(isinstance(elem, datetime.date) for elem in column_a) and any(
        isinstance(elem, str) for elem in column_b
    ):
        return column_a, column_b.apply(
            lambda x: parser.parse(x).date() if isinstance(x, str) else x
        )
    if any(isinstance(elem, str) for elem in column_a) and any(
        isinstance(elem, datetime.date) for elem in column_b
    ):
        return column_a.apply(
            lambda x: parser.parse(x).date() if isinstance(x, str) else x
        ), column_b
    return column_a, column_b


def run_e2e_error_test(
    pydough_impl: Callable[[], UnqualifiedNode] | str,
    error_message: str,
    graph: GraphMetadata,
    columns: dict[str, str] | list[str] | None = None,
    database: DatabaseContext | None = None,
    config: PyDoughConfigs | None = None,
) -> None:
    """
    Runs an end-to-end test that expects an error to be raised when
    executing the PyDough code. The error message is checked against the
    provided `error_message`.

    Args:
        `pydough_impl`: The PyDough function to be tested, or the string that
        should be evaluated to obtain the PyDough code.
        `error_message`: The error message that is expected to be raised.
        `graph`: The metadata graph to use for the test.
        `columns`: The columns argument to use for the test, if any.
        `database`: The database context to use for the test, if any.
        `config`: The PyDough configuration to use for the test, if any.
    """
    with pytest.raises(Exception, match=error_message):
        root: UnqualifiedNode = transform_and_exec_pydough(pydough_impl, graph, None)
        call_kwargs: dict = {}
        if graph is not None:
            call_kwargs["metadata"] = graph
        if config is not None:
            call_kwargs["config"] = config
        if database is not None:
            call_kwargs["database"] = database
        if columns is not None:
            call_kwargs["columns"] = columns
        to_df(root, **call_kwargs)
