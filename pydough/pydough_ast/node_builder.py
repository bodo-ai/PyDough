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
    ChildReference,
)
from .pydough_operators import PyDoughOperatorAST, builtin_registered_operators
from .errors import PyDoughASTException
from .collections import (
    PyDoughCollectionAST,
    TableCollection,
    SubCollection,
    Calc,
    CalcSubCollection,
    BackReferenceCollection,
    GlobalCalc,
)


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

    def build_child_reference(
        self, children: List[PyDoughCollectionAST], child_idx: int, name: str
    ) -> Reference:
        """
        Creates a new reference to an expression from a child collection of a
        CALC.

        Args:
            `children`: the child collections that the reference accesses.
            `child_idx`: the index of the child collection being referenced.
            `name`: the name of the expression being referenced.

        Returns:
            The newly created PyDough Child Reference.

        Raises:
            `PyDoughASTException`: if `name` does not refer to an expression in
            the collection, or `child_idx` is not a valid index for `children`.
        """
        if child_idx not in range(len(children)):
            raise PyDoughASTException(
                f"Invalid child reference index {child_idx} with {len(children)} children"
            )
        return ChildReference(children[child_idx], child_idx, name)

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

    def build_global_calc(
        self, graph: GraphMetadata, children: List[CalcSubCollection]
    ) -> GlobalCalc:
        """
        Creates a new global CALC instance, but `with_terms` still needs to be
        called on the output.

        Args:
            `children`: the child subcollections accessed by the CALC term.

        Returns:
            The newly created GlobalCalc.
        """
        return GlobalCalc(graph, children)

    def build_table_collection(self, name: str) -> TableCollection:
        """
        Creates a new table collection invocation.

        Args:
            `name`: the name of the collection being referenced.

        Returns:
            The newly created PyDough TableCollection.

        Raises:
            `PyDoughMetadataException`: if `name` does not refer to a
            collection in the graph.
        """
        return TableCollection(self.graph.get_collection(name))

    def build_sub_collection(
        self, collection: PyDoughCollectionAST, name: str
    ) -> SubCollection:
        """
        Creates a new sub collection invocation.

        Args:
            `collection`: the parent collection.
            `name`: the name of the subcollection being referenced.

        Returns:
            The newly created PyDough SubCollection.

        Raises:
            `PyDoughMetadataException`: if `name` does not refer to a
            property of the collection.
        """
        term: PyDoughAST = collection.get_term(name)
        if not isinstance(term, SubCollection):
            raise PyDoughMetadataException(
                f"Expected {term!r} to refer to a subcollection"
            )
        return term

    def build_calc(
        self,
        collection: PyDoughCollectionAST,
        children: List[CalcSubCollection],
    ) -> Calc:
        """
        Creates a CALC instance, but `with_terms` still needs to be called on
        the output.

        Args:
            `collection`: the preceding collection.
            `children`: the child subcollections accessed by the CALC term.
            `terms`: the named expressions in the CALC term.

        Returns:
            The newly created PyDough CALC term.
        """
        return Calc(collection, children)

    def build_back_reference_collection(
        self,
        collection: PyDoughCollectionAST,
        term_name: str,
        back_levels: int,
    ) -> BackReferenceCollection:
        """
        Creates a reference to a a subcollection of an ancestor.

        Args:
            `collection`: the preceding collection.
            `term_name`: the name of the subcollection being accessed.
            `back_levels`: the number of levels up in the ancestry tree to go.

        Returns:
            The newly created PyDough CALC term.

        Raises:
            `PyDoughASTException`: if the terms are invalid for the CALC term.
        """
        return BackReferenceCollection(collection, term_name, back_levels)
