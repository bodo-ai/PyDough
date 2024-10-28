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
]

from pydough.metadata import GraphMetadata
from pydough.pydough_ast import AstNodeBuilder, PyDoughAST
from typing import Dict, Set, Callable, Any, List
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
    def build(self, builder: AstNodeBuilder) -> PyDoughAST:
        """
        Uses a passed-in AST node builder to construct the node.
        """


class LiteralInfo(AstNodeTestInfo):
    def __init__(self, value: object, data_type: PyDoughType):
        self.value: object = value
        self.data_type: PyDoughType = data_type

    def build(self, builder: AstNodeBuilder) -> PyDoughAST:
        return builder.build_literal(self.value, self.data_type)


class ColumnInfo(AstNodeTestInfo):
    def __init__(self, collection_name: str, property_name: str):
        self.collection_name: str = collection_name
        self.property_name: property_name = property_name

    def build(self, builder: AstNodeBuilder) -> PyDoughAST:
        return builder.build_column(self.collection_name, self.property_name)


class FunctionInfo(AstNodeTestInfo):
    def __init__(self, function_name: str, args_info: List[AstNodeTestInfo]):
        self.function_name: str = function_name
        self.args_info: List[AstNodeTestInfo] = args_info

    def build(self, builder: AstNodeBuilder) -> PyDoughAST:
        args: List[PyDoughAST] = [info.build(builder) for info in self.args_info]
        return builder.build_expression_function_call(self.function_name, args)
