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
        return self.to_string()

    @abstractmethod
    def to_string(self) -> str:
        """
        TODO: add function docstring
        """


class LiteralInfo(AstNodeTestInfo):
    """
    TODO: add class docstring
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
    TODO: add class docstring
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
    TODO: add class docstring
    """

    def __init__(self, function_name: str, args_info: List[AstNodeTestInfo]):
        self.function_name: str = function_name
        self.args_info: List[AstNodeTestInfo] = args_info

    def to_string(self) -> str:
        arg_strings: List[str] = [arg.to_string() for arg in self.args_info]
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
    TODO: add class docstring
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
    TODO: add class docstring
    """

    def __init__(self):
        self.successor: CollectionTestInfo | None = None

    def __repr__(self):
        as_str: str = self.to_string()
        if self.successor is not None:
            as_str = f"{as_str}.{self.successor!r}"
        return as_str

    def __pow__(self, other):
        """
        TODO: add function docstring
        """
        assert isinstance(
            other, CollectionTestInfo
        ), f"can only use @ for pipelining collection info when the right hand side is a collection info, not {other.__class__.__name__}"
        self.successor = other
        return self

    def succeed(
        self, builder: AstNodeBuilder, collection: PyDoughCollectionAST
    ) -> PyDoughAST:
        """
        TODO: add function docstring
        """
        if self.successor is None:
            return collection
        return self.successor.build(builder, collection)


class TableCollectionInfo(CollectionTestInfo):
    """
    TODO: add class docstring
    """

    def __init__(self, name: str):
        super().__init__()
        self.name: str = name

    def to_string(self) -> str:
        return f"Table[{self.name}]"

    def build(
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
    ) -> PyDoughAST:
        local_result: PyDoughCollectionAST = builder.build_table_collection(self.name)
        return self.succeed(builder, local_result)


class SubCollectionInfo(CollectionTestInfo):
    def __init__(self, name: str):
        super().__init__()
        self.name: str = name

    def to_string(self) -> str:
        return f"SubCollection[{self.name}]"

    def build(
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
    ) -> PyDoughAST:
        assert (
            context is not None
        ), "Cannot call .build() on ReferenceInfo without providing a context"
        local_result: PyDoughCollectionAST = builder.build_sub_collection(
            context, self.name
        )
        return self.succeed(builder, local_result)


class CalcInfo(CollectionTestInfo):
    """
    TODO: add class docstring
    """

    def __init__(self, **kwargs):
        super().__init__()
        self.args: List[Tuple[str, AstNodeTestInfo]] = list(kwargs.items())

    def to_string(self) -> str:
        args_strings: List[str] = [
            f"{name}={arg.to_string()}" for name, arg in self.args
        ]
        return f"Calc[{', '.join(args_strings)}]"

    def build(
        self, builder: AstNodeBuilder, context: PyDoughCollectionAST | None = None
    ) -> PyDoughAST:
        assert (
            context is not None
        ), "Cannot call .build() on CalcInfo without providing a context"
        args: List[Tuple[str, PyDoughExpressionAST]] = [
            (name, info.build(builder, context)) for name, info in self.args
        ]
        local_result: PyDoughCollectionAST = builder.build_calc(context, args)
        return self.succeed(builder, local_result)
