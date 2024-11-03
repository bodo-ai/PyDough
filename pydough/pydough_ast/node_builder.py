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
    Reference,
    BackReferenceExpression,
)
from .pydough_operators import PyDoughOperatorAST, builtin_registered_operators
from .errors import PyDoughASTException
from .collections import PyDoughCollectionAST, Calc, GlobalContext, CollectionAccess


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

    def build_reference(self, collection: PyDoughCollectionAST, name: str) -> Reference:
        """
        Creates a new reference to an expression from a preceding collection.

        Args:
            `collection`: the collection that the reference comes from.
            `name`: the name of the expression being referenced.

        Returns:
            The newly created PyDough Reference.

        Raises:
            `PyDoughASTException`: if `name` does not refer to an expression in
            the collection.
        """
        return Reference(collection, name)

    def build_back_reference_expression(
        self, collection: PyDoughCollectionAST, name: str, levels: int
    ) -> Reference:
        """
        Creates a new reference to an expression from an ancestor collection.

        Args:
            `collection`: the collection that the back reference comes from.
            `name`: the name of the expression being referenced.
            `levels`: the number of levels back from `collection` the reference
            refers to.

        Returns:
            The newly created PyDough Back Reference.

        Raises:
            `PyDoughASTException`: if `name` does not refer to an expression in
            the ancestor collection, or the collection does not have `levels`
            many ancestors.
        """
        return BackReferenceExpression(collection, name, levels)

    def build_global_context(self) -> GlobalContext:
        """
        Creates a new global context for the graph.

        Returns:
            The newly created PyDough GlobalContext.
        """
        return GlobalContext(self.graph)

    def build_collection_access(
        self, name: str, preceding_context: PyDoughCollectionAST
    ) -> CollectionAccess:
        """
        Creates a new collection access AST node.

        Args:
            `name`: the name of the collection being referenced.
            `preceding_context`: the collection node from which the
            collection access is being fetched.

        Returns:
            The newly created PyDough CollectionAccess.

        Raises:
            `PyDoughMetadataException`: if `name` does not refer to a
            collection that `preceding_context` has access to.
        """
        return preceding_context.get_term(name)

    def build_calc(
        self,
        preceding_context: PyDoughCollectionAST,
    ) -> Calc:
        """
        Creates a CALC term, but `with_terms` still needs to be called on the
        output.

        Args:
            `preceding_context`: the preceding collection.

        Returns:
            The newly created PyDough CALC term.

        Raises:
            `PyDoughASTException`: if the terms are invalid for the CALC term.
        """
        return Calc(preceding_context)
