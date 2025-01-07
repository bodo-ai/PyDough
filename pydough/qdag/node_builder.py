"""
Definitions of utilities used to build PyDough QDAG nodes.
"""

__all__ = ["AstNodeBuilder"]

from collections.abc import MutableMapping, MutableSequence

from pydough.metadata import (
    CollectionMetadata,
    GraphMetadata,
    PropertyMetadata,
    PyDoughMetadataException,
    TableColumnMetadata,
)
from pydough.pydough_operators import (
    ExpressionWindowOperator,
    PyDoughExpressionOperator,
    PyDoughOperator,
    builtin_registered_operators,
)
from pydough.types import PyDoughType

from .abstract_pydough_qdag import PyDoughQDAG
from .collections import (
    BackReferenceCollection,
    Calc,
    ChildAccess,
    ChildReferenceCollection,
    GlobalContext,
    OrderBy,
    PartitionBy,
    PyDoughCollectionQDAG,
    TopK,
    Where,
)
from .errors import PyDoughQDAGException
from .expressions import (
    BackReferenceExpression,
    ChildReferenceExpression,
    CollationExpression,
    ColumnProperty,
    ExpressionFunctionCall,
    Literal,
    Reference,
    WindowCall,
)


class AstNodeBuilder:
    """
    Class used in testing to build QDAG nodes
    """

    def __init__(self, graph: GraphMetadata):
        self._graph: GraphMetadata = graph
        self._operators: MutableMapping[str, PyDoughOperator] = (
            builtin_registered_operators()
        )

    @property
    def graph(self) -> GraphMetadata:
        """
        The graph used by the node builder.
        """
        return self._graph

    @property
    def operators(self) -> MutableMapping[str, PyDoughOperator]:
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
            `PyDoughQDAGException`: if the literal cannot be created.
        """
        return Literal(value, data_type)

    def build_column(self, collection_name: str, property_name: str) -> ColumnProperty:
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
        collection = self.graph.get_collection(collection_name)
        assert isinstance(collection, CollectionMetadata)
        property = collection.get_property(property_name)
        assert isinstance(property, PropertyMetadata)
        if not isinstance(property, TableColumnMetadata):
            raise PyDoughMetadataException(
                f"Expected {property.error_name} to be a table column property"
            )
        return ColumnProperty(property)

    def build_expression_function_call(
        self, function_name: str, args: list[PyDoughQDAG]
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
            `PyDoughQDAGException`: if the operator does not exist or cannot be
            called on the arguments.
        """
        if function_name not in self.operators:
            raise PyDoughQDAGException(f"Unrecognized operator name {function_name!r}")
        operator = self.operators[function_name]
        assert isinstance(operator, PyDoughExpressionOperator)
        return ExpressionFunctionCall(operator, args)

    def build_window_call(
        self,
        window_operator: ExpressionWindowOperator,
        collation_args: list[CollationExpression],
        levels: int | None,
        kwargs: dict[str, object],
    ) -> WindowCall:
        """
        Creates a new window function call that returns an expression.

        Args:
            `window_operator`: the operator for the window function called.
            `collation_args`: the orderings used by the window function.
            `levels`: which ancestor the window function partitions relative to
            (None is the same thing as the furthest ancestor).
            `kwargs`: any additional arguments to the function, such as whether
            ranking allows ties or is dense.
            are ties.

        Returns:
            The window function call as a QDAG expression node.
        """
        return WindowCall(window_operator, collation_args, levels, kwargs)

    def build_reference(
        self, collection: PyDoughCollectionQDAG, name: str
    ) -> Reference:
        """
        Creates a new reference to an expression from a preceding collection.

        Args:
            `collection`: the collection that the reference comes from.
            `name`: the name of the expression being referenced.

        Returns:
            The newly created PyDough Reference.

        Raises:
            `PyDoughQDAGException`: if `name` does not refer to an expression in
            the collection.
        """
        return Reference(collection, name)

    def build_child_reference_expression(
        self,
        children: MutableSequence[PyDoughCollectionQDAG],
        child_idx: int,
        name: str,
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
            `PyDoughQDAGException`: if `name` does not refer to an expression in
            the collection, or `child_idx` is not a valid index for `children`.
        """
        if child_idx not in range(len(children)):
            raise PyDoughQDAGException(
                f"Invalid child reference index {child_idx} with {len(children)} children"
            )
        return ChildReferenceExpression(children[child_idx], child_idx, name)

    def build_back_reference_expression(
        self, collection: PyDoughCollectionQDAG, name: str, levels: int
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
            `PyDoughQDAGException`: if `name` does not refer to an expression in
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

    def build_child_access(
        self, name: str, preceding_context: PyDoughCollectionQDAG
    ) -> ChildAccess:
        """
        Creates a new child access QDAG node.

        Args:
            `name`: the name of the collection being referenced.
            `preceding_context`: the collection node from which the
            child access is being fetched.

        Returns:
            The newly created PyDough ChildAccess.

        Raises:
            `PyDoughMetadataException`: if `name` does not refer to a
            collection that `preceding_context` has access to.
        """
        term = preceding_context.get_collection(name)
        assert isinstance(term, ChildAccess)
        return term

    def build_calc(
        self,
        preceding_context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
    ) -> Calc:
        """
        Creates a CALC instance, but `with_terms` still needs to be called on
        the output.

        Args:
            `preceding_context`: the preceding collection.
            `children`: the child collections accessed by the CALC term.

        Returns:
            The newly created PyDough CALC term.
        """
        return Calc(preceding_context, children)

    def build_where(
        self,
        preceding_context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
    ) -> Where:
        """
        Creates a WHERE instance, but `with_condition` still needs to be called on
        the output.

        Args:
            `preceding_context`: the preceding collection.
            `children`: the child collections accessed by the WHERE term.

        Returns:
            The newly created PyDough WHERE instance.
        """
        return Where(preceding_context, children)

    def build_order(
        self,
        preceding_context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
    ) -> OrderBy:
        """
        Creates a ORDERBY instance, but `with_collation` still needs to be called on
        the output.

        Args:
            `preceding_context`: the preceding collection.
            `children`: the child collections accessed by the ORDERBY term.

        Returns:
            The newly created PyDough ORDERBY instance.
        """
        return OrderBy(preceding_context, children)

    def build_top_k(
        self,
        preceding_context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
        records_to_keep: int,
    ) -> TopK:
        """
        Creates a TOP K instance, but `with_collation` still needs to be called on
        the output.

        Args:
            `preceding_context`: the preceding collection.
            `children`: the child collections accessed by the ORDERBY term.
            `records_to_keep`: the `K` value in the TOP K.

        Returns:
            The newly created PyDough TOP K instance.
        """
        return TopK(preceding_context, children, records_to_keep)

    def build_partition(
        self,
        preceding_context: PyDoughCollectionQDAG,
        child: PyDoughCollectionQDAG,
        child_name: str,
    ) -> PartitionBy:
        """
        Creates a PARTITION BY instance, but `with_keys` still needs to be called on
        the output.

        Args:
            `preceding_context`: the preceding collection.
            `child`: the child that is the input to the PARTITION BY term.
            `child_name`: the name that is used to access `child`.

        Returns:
            The newly created PyDough PARTITION BY instance.
        """
        return PartitionBy(preceding_context, child, child_name)

    def build_back_reference_collection(
        self,
        collection: PyDoughCollectionQDAG,
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
            `PyDoughQDAGException`: if the terms are invalid for the CALC term.
        """
        return BackReferenceCollection(collection, term_name, back_levels)

    def build_child_reference_collection(
        self,
        preceding_context: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
        child_idx: int,
    ) -> ChildReferenceCollection:
        """
        Creates a new reference to a collection from a child collection of a
        CALC or other child operator.

        Args:
            `preceding_context`: the preceding collection.
            `children`: the child collections that the reference accesses.
            `child_idx`: the index of the child collection being referenced.

        Returns:
            The newly created PyDough Child Reference.

        Raises:
            `PyDoughQDAGException`: if `child_idx` is not a valid index for `children`.
        """
        if child_idx not in range(len(children)):
            raise PyDoughQDAGException(
                f"Invalid child reference index {child_idx} with {len(children)} children"
            )
        return ChildReferenceCollection(
            preceding_context, children[child_idx], child_idx
        )
