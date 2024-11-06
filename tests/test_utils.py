"""
TODO: add file-level docstring
"""

__all__ = [
    "graph_fetcher",
    "noun_fetcher",
    "map_over_dict_values",
    "AstNodeTestInfo",
    "LiteralInfo",
    "ColumnInfo",
    "FunctionInfo",
    "ReferenceInfo",
    "TableCollectionInfo",
    "SubCollectionInfo",
    "CalcInfo",
    "BackReferenceExpressionInfo",
    "ChildReferenceInfo",
    "BackReferenceCollectionInfo",
    "WhereInfo",
    "OrderInfo",
]

from abc import ABC, abstractmethod
from collections.abc import Callable, MutableMapping, MutableSequence
from typing import Any

from pydough.metadata import GraphMetadata
from pydough.pydough_ast import (
    AstNodeBuilder,
    Calc,
    CalcChildCollection,
    CollationExpression,
    OrderBy,
    PyDoughAST,
    PyDoughCollectionAST,
    PyDoughExpressionAST,
    Where,
)
from pydough.pydough_ast.collections import CollectionAccess
from pydough.types import PyDoughType

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
    Base class used in tests to specify information about an AST node
    before it can be created, e.g. describing column properties or
    function calls by name before a builder can be used to create them.
    """

    @abstractmethod
    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughAST:
        """
        Uses a passed-in AST node builder to construct the node.

        Args:
            `builder`: the builder that should be used to create the AST
            objects.
            `context`: an optional collection AST used as the context within
            which the AST is created.
            `children_contexts`: an optional list of collection ASTs of
            child nodes of a CALC that are accessible for ChildReference usage.

        Returns:
            The new instance of the AST object.
        """

    def __repr__(self):
        return self.to_string()

    @abstractmethod
    def to_string(self) -> str:
        """
        String representation of the TestInfo before it is built into an AST node.
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
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughAST:
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
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughAST:
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
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughAST:
        args: MutableSequence[PyDoughAST] = [
            info.build(builder, context, children_contexts) for info in self.args_info
        ]
        return builder.build_expression_function_call(self.function_name, args)


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
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughAST:
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
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughAST:
        assert (
            context is not None
        ), "Cannot call .build() on BackReferenceExpressionInfo without providing a context"
        return builder.build_back_reference_expression(context, self.name, self.levels)


class ChildReferenceInfo(AstNodeTestInfo):
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
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughAST:
        assert (
            children_contexts is not None
        ), "Cannot call .build() on ChildReferenceInfo without providing a list of child contexts"
        return builder.build_child_reference(
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
        String representation of the TestInfo before it is built into an AST node.
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
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        """
        Uses a passed-in AST node builder to construct the collection node.

        Args:
            `builder`: the builder that should be used to create the AST
            objects.
            `context`: an optional collection AST used as the context within
            which the AST is created.
            `children_contexts`: an optional list of collection ASTs of child
            nodes of a CALC that are accessible for ChildReference usage.

        Returns:
            The new instance of the collection AST object.
        """

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        local_result: PyDoughCollectionAST = self.local_build(
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
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        if context is None:
            context = builder.build_global_context()
        return builder.build_collection_access(self.name, context)


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
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        assert (
            context is not None
        ), "Cannot call .build() on ReferenceInfo without providing a context"
        return builder.build_collection_access(self.name, context)


class CalcChildCollectionInfo(CollectionTestInfo):
    """
    CollectionTestInfo implementation class that wraps around a subcollection
    info within a Calc context. Contains the following fields:
    - `child_info`: the collection info for the child subcollection.
    - `is_last`: the this the last child of the parent CALC.

    NOTE: must provide a `context` when building.
    """

    def __init__(self, child_info: CollectionTestInfo, is_last: bool):
        self.child_info: CollectionTestInfo = child_info
        self.is_last: bool = is_last
        self.successor = child_info.successor

    def local_string(self) -> str:
        return f"ChildSubCollection[{self.child_info!r}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        assert (
            context is not None
        ), "Cannot call .build() on ReferenceInfo without providing a context"
        access = self.child_info.local_build(builder, context, children_contexts)
        assert isinstance(access, CollectionAccess)
        return CalcChildCollection(
            access,
            self.is_last,
        )


class BackReferenceCollectionInfo(CollectionTestInfo):
    """
    CollectionTestInfo implementation class to build a reference to an
    ancestor collection. Contains the following fields:
    - `name`: the name of the calc term being referenced.
    - `levels`: the number of levels upward to reference.

    NOTE: must provide a `context` when building.
    """

    def __init__(self, name: str, levels: int):
        super().__init__()
        self.name: str = name
        self.levels: int = levels

    def local_string(self) -> str:
        return f"BackReferenceCollection[{self.levels}:{self.name}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        assert (
            context is not None
        ), "Cannot call .build() on BackReferenceCollectionInfo without providing a context"
        return builder.build_back_reference_collection(context, self.name, self.levels)


class ChildOperatorInfo(CollectionTestInfo):
    """
    Base class for types of CollectionTestInfo that have child nodes, such as
    CALC or WHERE.  Contains the following fields:
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
            child_strings.append(f"${idx}: {child.to_string}")
        if len(self.children_info) == 0:
            return ""
        return "\n" + "\n".join(child_strings) + "\n"

    def build_children(
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
    ) -> MutableSequence[PyDoughCollectionAST]:
        """
        Builds all of the child infos into the children of the operator.

        Args:
            `builder`: the builder that should be used to create the AST
            objects.
            `context`: an optional collection AST used as the context within
            which the AST is created.

        Returns:
            The list of built child collections.
        """
        children: MutableSequence[PyDoughCollectionAST] = []
        for idx, child_info in enumerate(self.children_info):
            child = CalcChildCollectionInfo(
                child_info, idx == len(self.children_info) - 1
            ).build(builder, context)
            assert isinstance(child, PyDoughCollectionAST)
            children.append(child)
        return children


class CalcInfo(ChildOperatorInfo):
    """
    CollectionTestInfo implementation class to build a CALC node.
    Contains the following fields:
    - `children_info`: a list of CollectionTestInfo objects that will be used
       to build the child contexts.
    - `args`: a list tuples containing a field name and a test info to derive
       an expression in the CALC. Passed in via keyword arguments to the
       constructor, where the argument names are the field names and the
       argument values are the expression infos.

    NOTE: must provide a `context` when building.
    """

    def __init__(self, children: MutableSequence[CollectionTestInfo], **kwargs):
        super().__init__(children)
        self.args: MutableSequence[tuple[str, AstNodeTestInfo]] = list(kwargs.items())

    def local_string(self) -> str:
        args_strings: MutableSequence[str] = [
            f"{name}={arg.to_string()}" for name, arg in self.args
        ]
        return f"Calc[{self.child_strings()}{', '.join(args_strings)}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        if context is None:
            context = builder.build_global_context()
        children: MutableSequence[PyDoughCollectionAST] = self.build_children(
            builder,
            context,
        )
        raw_calc = builder.build_calc(context, children)
        assert isinstance(raw_calc, Calc)
        args: MutableSequence[tuple[str, PyDoughExpressionAST]] = []
        for name, info in self.args:
            expr = info.build(builder, context, children)
            assert isinstance(expr, PyDoughExpressionAST)
            args.append((name, expr))
        return raw_calc.with_terms(args)


class WhereInfo(ChildOperatorInfo):
    """
    CollectionTestInfo implementation class to build a WHERE clause.
    Contains the following fields:
    - `condition`: a test info describing the predicate for the WHERe clause.

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
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        if context is None:
            context = builder.build_global_context()
        children: MutableSequence[PyDoughCollectionAST] = self.build_children(
            builder, context
        )
        raw_where = builder.build_where(context, children)
        assert isinstance(raw_where, Where)
        cond = self.condition.build(builder, context, children)
        assert isinstance(cond, PyDoughExpressionAST)
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
        context: PyDoughCollectionAST | None = None,
        children_contexts: MutableSequence[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        if context is None:
            context = builder.build_global_context()
        children: MutableSequence[PyDoughCollectionAST] = self.build_children(
            builder, context
        )
        raw_order = builder.build_order(context, children)
        assert isinstance(raw_order, OrderBy)
        collation: list[CollationExpression] = []
        for info, asc, na_last in self.collation:
            expr = info.build(builder, context, children)
            assert isinstance(expr, PyDoughExpressionAST)
            collation.append(CollationExpression(expr, asc, na_last))
        return raw_order.with_collation(collation)
