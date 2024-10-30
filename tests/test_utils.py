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
    "CalcInfo",
]

from pydough.metadata import GraphMetadata
from pydough.pydough_ast import (
    AstNodeBuilder,
    PyDoughAST,
    PyDoughCollectionAST,
    PyDoughExpressionAST,
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
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
    ) -> PyDoughAST:
        """
        Uses a passed-in AST node builder to construct the node.

        Args:
            `builder`: the builder that should be used to create the AST
            objects.
            `context`: an optional collection AST used as the context within
            which the AST is created.

        Returns:
            The new instance of the AST object.
        """

    def __repr__(self):
        return self.to_string

    @property
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
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
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
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
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
        arg_strings: List[str] = [arg.to_string for arg in self.args_info]
        return f"Call[{self.function_name} on ({', '.join(arg_strings)})]"

    def build(
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
    ) -> PyDoughAST:
        args: List[PyDoughAST] = [
            info.build(builder, context) for info in self.args_info
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
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
    ) -> PyDoughAST:
        assert (
            context is not None
        ), "Cannot call .build() on ReferenceInfo without providing a context"
        return builder.build_reference(context, self.name)


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

    def __repr__(self):
        as_str: str = self.to_string
        if self.successor is not None:
            as_str = f"{as_str}.{self.successor!r}"
        return as_str

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
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
    ) -> PyDoughAST:
        local_result: PyDoughCollectionAST = self.local_build(builder, context)
        if self.successor is None:
            return local_result
        return self.successor.build(builder, local_result)


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
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
    ) -> PyDoughCollectionAST:
        return builder.build_table_collection(self.name)


class CalcInfo(CollectionTestInfo):
    """
    CollectionTestInfo implementation class to build a CALC term.
    Contains the following fields:
    - `args`: a list tuples containing a field name and a test info
      to derive an expression in the CALC.

    NOTE: must provide a `context` when building.
    """

    def __init__(self, **kwargs):
        super().__init__()
        self.args: List[Tuple[str, AstNodeTestInfo]] = list(kwargs.items())

    def to_string(self) -> str:
        args_strings: List[str] = [f"{name}={arg.to_string}" for name, arg in self.args]
        return f"Calc[{', '.join(args_strings)}]"

    def local_build(
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
    ) -> PyDoughCollectionAST:
        assert (
            context is not None
        ), "Cannot call .build() on CalcInfo without providing a context"
        args: List[Tuple[str, PyDoughExpressionAST]] = [
            (name, info.build(builder, context)) for name, info in self.args
        ]
        return builder.build_calc(context, args)
