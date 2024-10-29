"""
TODO: add file-level docstring
"""

__all__ = ["AstNodeBuilder"]

from pydough.metadata import (
    GraphMetadata,
    CollectionMetadata,
    PropertyMetadata,
    TableColumnMetadata,
    PyDoughMetadataException,
)
from typing import Dict, List
from pydough.types import PyDoughType
from .abstract_pydough_ast import PyDoughAST
from .expressions import (
    Literal,
    ExpressionFunctionCall,
    ColumnProperty,
)
from .pydough_operators import PyDoughOperatorAST, builtin_registered_operators
from .errors import PyDoughASTException


class AstNodeBuilder(object):
    """
    Class used in testing to build AST nodes
    """

    def __init__(self, graph: GraphMetadata):
        self._graph: GraphMetadata = graph
        self._operators: Dict[str, PyDoughOperatorAST] = builtin_registered_operators()

    @property
    def graph(self) -> GraphMetadata:
        """
        The graph used by the node builder.
        """
        return self._graph

    @property
    def operators(self) -> Dict[str, PyDoughOperatorAST]:
        """
        The operators that the builder has access to.
        """
        return self._operators

    def build_literal(self, value: object, data_type: PyDoughType) -> Literal:
        """
        Creates a new literal of the specified PyDough type using a passed-in
        literal value.

        Args:
            `value`: the literal value to be stored.
            `data_type`: the PyDough type of the literal.

        Returns:
            The newly created PyDough literal.

        Raises:
            `PyDoughASTException`: if the literal cannot be created.
        """
        return Literal(value, data_type)

    def build_column(
        self, collection_name: str, property_name: str
    ) -> TableColumnMetadata:
        """
        Creates a new column property node by accessing a specific property of
        a collection in the graph by name.

        Args:
            `value`: the literal value to be stored.
            `data_type`: the PyDough type of the literal.

        Returns:
            The newly created PyDough literal.

        Raises:
            `PyDoughMetadataException`: if the property does not exist or is
            not a table column.
        """
        collection: CollectionMetadata = self.graph.get_collection(collection_name)
        property: PropertyMetadata = collection.get_property(property_name)
        if not isinstance(property, TableColumnMetadata):
            raise PyDoughMetadataException(
                f"Expected {property.error_name} to be a table column property"
            )
        return ColumnProperty(property)

    def build_expression_function_call(
        self, function_name: str, args: List[PyDoughAST]
    ) -> ExpressionFunctionCall:
        """
        Creates a new expression function call by accessing a builtin
        expression function operator by name and calling it on the passed-in
        arguments.

        Args:
            `value`: the literal value to be stored.
            `data_type`: the PyDough type of the literal.

        Returns:
            The newly created PyDough literal.

        Raises:
            `PyDoughASTException`: if the operator does not exist or cannot be
            called on the arguments.
        """
        if function_name not in self.operators:
            raise PyDoughASTException(f"Unrecognized operator name {function_name!r}")
        operator: PyDoughOperatorAST = self.operators[function_name]
        return ExpressionFunctionCall(operator, args)
