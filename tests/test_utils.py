"""
Utilities used by PyDough test files, such as the TestInfo classes used to
build QDAG nodes for unit tests.
"""

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
    "SubCollectionInfo",
    "TableCollectionInfo",
    "TopKInfo",
    "WhereInfo",
    "graph_fetcher",
    "map_over_dict_values",
    "noun_fetcher",
]

from abc import ABC, abstractmethod
from collections.abc import Callable, MutableMapping, MutableSequence
from typing import Any

import pydough.pydough_operators as pydop
from pydough.metadata import GraphMetadata
from pydough.qdag import (
    AstNodeBuilder,
    Calculate,
    ChildOperatorChildAccess,
    ChildReferenceExpression,
    CollationExpression,
    OrderBy,
    PartitionBy,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    PyDoughQDAG,
    TopK,
    Where,
)
from pydough.relational import (
    ColumnReference,
    ExpressionSortInfo,
    LiteralExpression,
    RelationalExpression,
    Scan,
)
from pydough.types import PyDoughType, UnknownType

# Type alias for a function that takes in a string and generates metadata
# for a graph based on it.
graph_fetcher = Callable[[str], GraphMetadata]

# Type alias for a function that takes in a string and generates the
# representation of all the nouns in a metadata graphs based on it.
noun_fetcher = Callable[[str], MutableMapping[str, set[str]]]


def map_over_dict_values(
    dictionary: MutableMapping[Any, Any], func: Callable[[Any], Any]
) -> MutableMapping[Any, Any]:
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        return builder.build_column(self.collection_name, self.property_name)


class FunctionInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a function call. Contains the
    following fields:
    - `function_name`: the name of PyDough operator to be invoked.
    - `args_info`: a list of TestInfo objects used to build the arguments.
    """

    def __init__(self, function_name: str, args_info: MutableSequence[AstNodeTestInfo]):
        self.function_name: str = function_name
        self.args_info: MutableSequence[AstNodeTestInfo] = args_info

    def to_string(self) -> str:
        arg_strings: MutableSequence[str] = [arg.to_string() for arg in self.args_info]
        return f"Call[{self.function_name} on ({', '.join(arg_strings)})]"

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        args: list[PyDoughQDAG] = [
            info.build(builder, context, children_contexts) for info in self.args_info
        ]
        return builder.build_expression_function_call(self.function_name, args)


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
                raise Exception(f"Unsupported window function {self.name}")

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        assert (
            context is not None
        ), "Cannot call .build() on RankingInfo without providing a context"
        collation_args: list[CollationExpression] = []
        for arg in self.collation:
            expr = arg[0].build(builder, context, children_contexts)
            assert isinstance(expr, PyDoughExpressionQDAG)
            collation_args.append(CollationExpression(expr, arg[1], arg[2]))
        match self.name:
            case "RANKING":
                return builder.build_window_call(
                    pydop.RANKING,
                    collation_args,
                    self.levels,
                    self.kwargs,
                )
            case _:
                raise Exception(f"Unsupported window function {self.name}")


class ReferenceInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a reference. Contains the
    following fields:
    - `name`: the name of the calc term being referenced.
    """

    def __init__(self, name: str):
        self.name: str = name

    def to_string(self) -> str:
        return f"Reference[{self.name}]"

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        assert (
            context is not None
        ), "Cannot call .build() on ReferenceInfo without providing a context"
        return builder.build_reference(context, self.name)


class BackReferenceExpressionInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a back reference expression.
    Contains the following fields:
    - `name`: the name of the calc term being referenced.
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        assert (
            context is not None
        ), "Cannot call .build() on BackReferenceExpressionInfo without providing a context"
        return builder.build_back_reference_expression(context, self.name, self.levels)


class ChildReferenceExpressionInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a child reference expression.
    Contains the following fields:
    - `name`: the name of the calc term being referenced.
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughQDAG:
        assert (
            children_contexts is not None
        ), "Cannot call .build() on ChildReferenceExpressionInfo without providing a list of child contexts"
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
        assert isinstance(
            other, CollectionTestInfo
        ), f"can only use ** for pipelining collection info when the right hand side is a collection info, not {other.__class__.__name__}"
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        assert (
            context is not None
        ), "Cannot call .build() on ReferenceInfo without providing a context"
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        assert (
            context is not None
        ), "Cannot call .build() on ReferenceInfo without providing a context"
        access: PyDoughCollectionQDAG = self.child_info.local_build(
            builder, context, children_contexts
        )
        return ChildOperatorChildAccess(access)


class ChildReferenceCollectionInfo(CollectionTestInfo):
    """
    CollectionTestInfo implementation class to build a reference to a
    child collection. Contains the following fields:
    - `idx`: the index of the calc term being referenced.

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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        assert (
            context is not None
        ), "Cannot call .build() on ChildReferenceCollection without providing a context"
        assert (
            children_contexts is not None
        ), "Cannot call .build() on ChildReferenceCollection without providing a list of child contexts"
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

    def __init__(self, children: MutableSequence[CollectionTestInfo]):
        super().__init__()
        self.children_info: MutableSequence[CollectionTestInfo] = children

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
    ) -> MutableSequence[PyDoughCollectionQDAG]:
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
        children: MutableSequence[PyDoughCollectionQDAG] = []
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

    def __init__(self, children: MutableSequence[CollectionTestInfo], **kwargs):
        super().__init__(children)
        self.args: MutableSequence[tuple[str, AstNodeTestInfo]] = list(kwargs.items())

    def local_string(self) -> str:
        args_strings: MutableSequence[str] = [
            f"{name}={arg.to_string()}" for name, arg in self.args
        ]
        return f"Calculate[{self.child_strings()}{', '.join(args_strings)}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            context = builder.build_global_context()
        children: MutableSequence[PyDoughCollectionQDAG] = self.build_children(
            builder,
            context,
        )
        raw_calc = builder.build_calculate(context, children)
        assert isinstance(raw_calc, Calculate)
        args: MutableSequence[tuple[str, PyDoughExpressionQDAG]] = []
        for name, info in self.args:
            expr = info.build(builder, context, children)
            assert isinstance(expr, PyDoughExpressionQDAG)
            args.append((name, expr))
        return raw_calc.with_terms(args)


class WhereInfo(ChildOperatorInfo):
    """
    CollectionTestInfo implementation class to build a WHERE clause.
    Contains the following fields:
    - `condition`: a test info describing the predicate for the WHERE clause.

    NOTE: must provide a `context` when building.
    """

    def __init__(
        self, children: MutableSequence[CollectionTestInfo], condition: AstNodeTestInfo
    ):
        super().__init__(children)
        self.condition: AstNodeTestInfo = condition

    def local_string(self) -> str:
        return f"WHERE[{self.child_strings()}{self.condition.to_string()}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            raise Exception("Must provide a context when building a WHERE clause.")
        children: MutableSequence[PyDoughCollectionQDAG] = self.build_children(
            builder, context
        )
        raw_where = builder.build_where(context, children)
        assert isinstance(raw_where, Where)
        cond = self.condition.build(builder, context, children)
        assert isinstance(cond, PyDoughExpressionQDAG)
        return raw_where.with_condition(cond)


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
        children: MutableSequence[CollectionTestInfo],
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            raise Exception(
                "Must provide context and children_contexts when building an ORDER BY clause."
            )
        children: MutableSequence[PyDoughCollectionQDAG] = self.build_children(
            builder, context
        )
        raw_order = builder.build_order(context, children)
        assert isinstance(raw_order, OrderBy)
        collation: list[CollationExpression] = []
        for info, asc, na_last in self.collation:
            expr = info.build(builder, context, children)
            assert isinstance(expr, PyDoughExpressionQDAG)
            collation.append(CollationExpression(expr, asc, na_last))
        return raw_order.with_collation(collation)


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
        children: MutableSequence[CollectionTestInfo],
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
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            raise Exception(
                "Must provide context and children_contexts when building a TOPK clause."
            )
        children: MutableSequence[PyDoughCollectionQDAG] = self.build_children(
            builder, context
        )
        raw_top_k = builder.build_top_k(context, children, self.records_to_keep)
        assert isinstance(raw_top_k, TopK)
        collation: list[CollationExpression] = []
        for info, asc, na_last in self.collation:
            expr = info.build(builder, context, children)
            assert isinstance(expr, PyDoughExpressionQDAG)
            collation.append(CollationExpression(expr, asc, na_last))
        return raw_top_k.with_collation(collation)


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
        child_name: str,
        keys: list[AstNodeTestInfo],
    ):
        super().__init__([child])
        self.child_name: str = child_name
        self.keys: list[AstNodeTestInfo] = keys

    def local_string(self) -> str:
        key_strings_tup: tuple = tuple([key.to_string() for key in self.keys])
        return f"PartitionBy[{self.child_strings()}name={self.child_name!r}, by={key_strings_tup}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionQDAG | None = None,
        children_contexts: MutableSequence[PyDoughCollectionQDAG] | None = None,
    ) -> PyDoughCollectionQDAG:
        if context is None:
            context = builder.build_global_context()
        children: MutableSequence[PyDoughCollectionQDAG] = self.build_children(
            builder, context
        )
        assert len(children) == 1
        raw_partition = builder.build_partition(context, children[0], self.child_name)
        assert isinstance(raw_partition, PartitionBy)
        keys: list[ChildReferenceExpression] = []
        for info in self.keys:
            expr = info.build(builder, context, children)
            assert isinstance(expr, ChildReferenceExpression)
            keys.append(expr)
        return raw_partition.with_keys(keys)


def make_relational_column_reference(
    name: str, typ: PyDoughType | None = None, input_name: str | None = None
) -> ColumnReference:
    """
    Make a column reference given name and type. This is used
    for generating various relational nodes.

    Args:
        name (str): The name of the column in the input.
        typ (PyDoughType | None): The PyDoughType of the column. Defaults to
            None.
        input_name (str | None): The name of the input node. This is
            used by Join to differentiate between the left and right.
            Defaults to None.

    Returns:
        Column: The output column.
    """
    pydough_type = typ if typ is not None else UnknownType()
    return ColumnReference(name, pydough_type, input_name)


def make_relational_literal(value: Any, typ: PyDoughType | None = None):
    """
    Make a literal given value and type. This is used for
    generating various relational nodes.

    Args:
        value (Any): The value of the literal.

    Returns:
        Literal: The output literal.
    """
    pydough_type = typ if typ is not None else UnknownType()
    return LiteralExpression(value, pydough_type)


def build_simple_scan() -> Scan:
    """
    Build a simple scan node for reuse in tests.

    Returns:
        Scan: The Scan node.
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
    Create am ordering as a function of a Relational column reference
    with the given ascending and nulls_first parameters.

    Args:
        name (str): _description_
        typ (PyDoughType | None, optional): _description_. Defaults to None.
        ascending (bool, optional): _description_. Defaults to True.
        nulls_first (bool, optional): _description_. Defaults to True.

    Returns:
        ExpressionSortInfo: The column ordering information.
    """
    return ExpressionSortInfo(expr, ascending, nulls_first)
