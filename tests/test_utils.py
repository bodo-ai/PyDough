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
    "GlobalCalcInfo",
]

from pydough.metadata import GraphMetadata
from pydough.pydough_ast import (
    AstNodeBuilder,
    PyDoughAST,
    PyDoughCollectionAST,
    PyDoughExpressionAST,
    Calc,
    CalcSubCollection,
    GlobalCalc,
    GlobalCalcTableCollection,
)
from typing import Dict, Set, Callable, Any, List, Tuple
from pydough.types import PyDoughType
from abc import ABC, abstractmethod

# Type alias for a function that takes in a string and generates metadata
# for a graph based on it.
graph_fetcher = Callable[[str], GraphMetadata]

# Type alias for a function that takes in a string and generates the
# representation of all the nouns in a metadata graphs based on it.
noun_fetcher = Callable[[str], Dict[str, Set[str]]]


def map_over_dict_values(dictionary: dict, func: Callable[[Any], Any]) -> dict:
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
        children_contexts: List[PyDoughCollectionAST] | None = None,
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
        children_contexts: List[PyDoughCollectionAST] | None = None,
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
        self.property_name: property_name = property_name

    def to_string(self) -> str:
        return f"Column[{self.collection_name}.{self.property_name}]"

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: List[PyDoughCollectionAST] | None = None,
    ) -> PyDoughAST:
        return builder.build_column(self.collection_name, self.property_name)


class FunctionInfo(AstNodeTestInfo):
    """
    TestInfo implementation class to build a function call. Contains the
    following fields:
    - `function_name`: the name of PyDough operator to be invoked.
    - `args_info`: a list of TestInfo objects used to build the arguments.
    """

    def __init__(self, function_name: str, args_info: List[AstNodeTestInfo]):
        self.function_name: str = function_name
        self.args_info: List[AstNodeTestInfo] = args_info

    def to_string(self) -> str:
        arg_strings: List[str] = [arg.to_string() for arg in self.args_info]
        return f"Call[{self.function_name} on ({', '.join(arg_strings)})]"

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: List[PyDoughCollectionAST] | None = None,
    ) -> PyDoughAST:
        args: List[PyDoughAST] = [
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
        children_contexts: List[PyDoughCollectionAST] | None = None,
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
        children_contexts: List[PyDoughCollectionAST] | None = None,
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
        children_contexts: List[PyDoughCollectionAST] | None = None,
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
    def local_build(
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
    ) -> PyDoughCollectionAST:
        """
        Uses a passed-in AST node builder to construct the collection node.

        Args:
            `builder`: the builder that should be used to create the AST
            objects.
            `context`: an optional collection AST used as the context within
            which the AST is created.

        Returns:
            The new instance of the collection AST object.
        """

    def build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: List[PyDoughCollectionAST] | None = None,
    ) -> PyDoughAST:
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

    def to_string(self) -> str:
        return f"Table[{self.name}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: List[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        return builder.build_table_collection(self.name)


class SubCollectionInfo(CollectionTestInfo):
    """
    CollectionTestInfo implementation class to create a subcollection access,
    either as a direct subcollection or via a compound relationship. Contains
    the following fields:
    - `name`: the name of the subcollection property within the collection.

    NOTE: must provide a `context` when building.
    """

    def __init__(self, name: str):
        super().__init__()
        self.name: str = name

    def to_string(self) -> str:
        return f"SubCollection[{self.name}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: List[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        assert (
            context is not None
        ), "Cannot call .build() on ReferenceInfo without providing a context"
        return builder.build_sub_collection(context, self.name)


class CalcSubCollectionInfo(CollectionTestInfo):
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

    def to_string(self) -> str:
        return f"ChildSubCollection[{self.child_info!r}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: List[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        assert (
            context is not None
        ), "Cannot call .build() on ReferenceInfo without providing a context"
        return CalcSubCollection(
            self.child_info.local_build(builder, context, children_contexts),
            self.is_last,
        )


class CalcInfo(CollectionTestInfo):
    """
    CollectionTestInfo implementation class to build a CALC term.
    Contains the following fields:
    - `args`: a list tuples containing a field name and a test info
      to derive an expression in the CALC.

    NOTE: must provide a `context` when building.
    """

    def __init__(self, children, **kwargs):
        super().__init__()
        self.children_info: List[CollectionTestInfo] = children
        self.args: List[Tuple[str, AstNodeTestInfo]] = list(kwargs.items())

    def to_string(self) -> str:
        args_strings: List[str] = [
            f"{name}={arg.to_string()}" for name, arg in self.args
        ]
        return f"Calc[{', '.join(args_strings)}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: List[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        assert (
            context is not None
        ), "Cannot call .build() on CalcInfo without providing a context"
        children: List[PyDoughCollectionAST] = [
            CalcSubCollectionInfo(child, idx == len(self.children_info) - 1).build(
                builder, context
            )
            for idx, child in enumerate(self.children_info)
        ]
        raw_calc: Calc = builder.build_calc(context, children)
        args: List[Tuple[str, PyDoughExpressionAST]] = [
            (name, info.build(builder, context, children)) for name, info in self.args
        ]
        return raw_calc.with_terms(args)


class GlobalCalcTableCollectionInfo(CollectionTestInfo):
    """
    CollectionTestInfo implementation class that wraps around a table context
    info within a Calc context. Contains the following fields:
    - `child_info`: the collection info for the child table collection.
    - `is_last`: the this the last child of the parent CALC.

    NOTE: must provide a `context` when building.
    """

    def __init__(self, child_info: CollectionTestInfo, is_last: bool):
        self.child_info: CollectionTestInfo = child_info
        self.is_last: bool = is_last
        self.successor = child_info.successor

    def to_string(self) -> str:
        return f"ChildTableCollection[{self.child_info!r}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: List[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        assert (
            context is not None
        ), "Cannot call .build() on ReferenceInfo without providing a context"
        return GlobalCalcTableCollection(
            self.child_info.local_build(builder, context, children_contexts),
            self.is_last,
        )


class GlobalCalcInfo(CalcInfo):
    """
    CollectionTestInfo implementation class to build a global CALC.
    """

    def to_string(self) -> str:
        return f"Global{super().to_string()}"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: List[PyDoughCollectionAST] | None = None,
    ) -> PyDoughCollectionAST:
        children: List[PyDoughCollectionAST] = [
            GlobalCalcTableCollectionInfo(
                child, idx == len(self.children_info) - 1
            ).build(builder, context)
            for idx, child in enumerate(self.children_info)
        ]
        raw_calc: GlobalCalc = builder.build_global_calc(builder.graph, children)
        args: List[Tuple[str, PyDoughExpressionAST]] = [
            (name, info.build(builder, context, children)) for name, info in self.args
        ]
        return raw_calc.with_terms(args)


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

    def to_string(self) -> str:
        return f"BackReferenceCollection[{self.levels}:{self.name}]"

    def local_build(
        self,
        builder: AstNodeBuilder,
        context: PyDoughCollectionAST | None = None,
        children_contexts: List[PyDoughCollectionAST] | None = None,
    ) -> PyDoughAST:
        assert (
            context is not None
        ), "Cannot call .build() on BackReferenceCollectionInfo without providing a context"
        return builder.build_back_reference_collection(context, self.name, self.levels)
