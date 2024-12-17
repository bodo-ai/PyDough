"""
APIs used for user explanation & exploration of PyDough metadata and code.
"""

__all__ = ["explain", "explain_structure", "explain_graph"]


import pydough.pydough_operators as pydop

from .metadata.abstract_metadata import AbstractMetadata
from .metadata.collections import CollectionMetadata, SimpleTableMetadata
from .metadata.graphs import GraphMetadata
from .metadata.properties import (
    CartesianProductMetadata,
    CompoundRelationshipMetadata,
    PropertyMetadata,
    ReversiblePropertyMetadata,
    ScalarAttributeMetadata,
    SimpleJoinMetadata,
    SubcollectionRelationshipMetadata,
    TableColumnMetadata,
)
from .qdag import (
    BackReferenceCollection,
    BackReferenceExpression,
    Calc,
    ChildOperator,
    ChildReferenceExpression,
    ColumnProperty,
    ExpressionFunctionCall,
    GlobalContext,
    OrderBy,
    PartitionBy,
    PartitionChild,
    PyDoughAST,
    PyDoughASTException,
    PyDoughCollectionAST,
    PyDoughExpressionAST,
    Reference,
    SubCollection,
    TableCollection,
    TopK,
    Where,
)
from .unqualified import (
    UnqualifiedAccess,
    UnqualifiedCalc,
    UnqualifiedNode,
    UnqualifiedOrderBy,
    UnqualifiedPartition,
    UnqualifiedRoot,
    UnqualifiedTopK,
    UnqualifiedWhere,
    display_raw,
    qualify_node,
    qualify_term,
)


def property_cardinality_string(property: ReversiblePropertyMetadata) -> str:
    """
    Converts a reversible subcollection property into a string representation
    of its cardinality.

    Args:
        `property`: the metadata property whose cardinality is being requested.

    Returns:
        A string representation of the relationship between the two collections
        that are connected by the relationship.
    """
    match (property.is_plural, property.reverse_property.is_plural):
        case (False, False):
            return "One -> One"
        case (False, True):
            return "Many -> One"
        case (True, False):
            return "One -> Many"
        case (True, True):
            return "Many -> Many"
        case _:
            raise ValueError("Malformed case")


def explain_property(property: PropertyMetadata, verbose: bool) -> str:
    """
    Displays information about a PyDough metadata property, including:
    - The name of the property
    - For scalar properties, its type & the data it corresponds to
    - For subcollection properties, the collection it connncts to and any
      additional information about how they are connected.

    Args:
        `property`: the metadata property being examined.
        `verbose`: if true, displays more detailed information about
        `property` in a less compact format.

    Returns:
        A string explanation of `property`.
    """
    lines: list[str] = []
    collection_name: str = property.collection.name
    property_name: str = property.name
    lines.append(f"PyDough property: {collection_name}.{property_name}")
    match property:
        case TableColumnMetadata():
            assert isinstance(property.collection, SimpleTableMetadata)
            lines.append(
                f"Column name: {property.collection.table_path}.{property.column_name}"
            )
            lines.append(f"Data type: {property.data_type.json_string}")

        case ReversiblePropertyMetadata():
            child_name: str = property.other_collection.name
            lines.append(
                f"This property connects collection {collection_name} to {child_name}."
            )
            if verbose:
                lines.append(
                    f"Cardinality of connection: {property_cardinality_string(property)}"
                )
                lines.append("Is reversible: yes")
                lines.append(f"Reverse property: {child_name}.{property.reverse_name}")
                match property:
                    case CartesianProductMetadata():
                        lines.append(
                            f"Note: this is a cartesian-product relationship, meaning that every record of {collection_name} matches onto every record of {child_name}."
                        )
                    case SimpleJoinMetadata():
                        conditions: list[str] = []
                        for lhs_key_name, rhs_key_names in property.keys.items():
                            for rhs_key_name in rhs_key_names:
                                conditions.append(
                                    f"  {collection_name}.{lhs_key_name} == {child_name}.{rhs_key_name}"
                                )
                        conditions.sort()
                        assert len(conditions) > 0
                        lines.append(
                            "The subcollection relationship is defined by the following join conditions:"
                        )
                        for cond_str in conditions:
                            lines.append(f"  {cond_str}")
                    case CompoundRelationshipMetadata():
                        primary_name: str = (
                            f"{collection_name}.{property.primary_property.name}"
                        )
                        lines.append(
                            f"Note: this property is a compound property; it is an alias for {primary_name}.{property.secondary_property.name}."
                        )
                        if len(property.inherited_properties) > 0:
                            lines.append(
                                f"The following properties are inherited from {primary_name}:"
                            )
                            for alias, inherited_property in sorted(
                                property.inherited_properties.items()
                            ):
                                lines.append(
                                    f"  {alias} is an alias for {primary_name}.{inherited_property.name}"
                                )
                        else:
                            lines.append(
                                f"There are no properties inherited from {primary_name}"
                            )
                    case _:
                        raise ValueError(
                            f"Unrecognized type of property: {property.__class__.__name__}"
                        )
            else:
                lines.append(
                    f"Use pydough.explain(graph['{collection_name}']['{property_name}'], verbose=True) to learn more details."
                )
        case _:
            raise ValueError(
                f"Unrecognized type of property: {property.__class__.__name__}"
            )
    return "\n".join(lines)


def explain_collection(collection: CollectionMetadata, verbose: bool) -> str:
    """
    Displays information about a PyDough metadata collection, including:
    - The name of the collection
    - The data that the collection corresponds to
    - The names of unique properties of the collection
    - The names of scalar & subcollection properties of the collection

    Args:
        `collection`: the metadata collection being examined.
        `verbose`: if true, displays more detailed information about
        `collection` in a less compact format.

    Returns:
        A string explanation of `collection`.
    """
    lines: list[str] = []
    lines.append(f"PyDough collection: {collection.name}")
    property_names: list[str] = sorted(collection.get_property_names())
    scalar_properties: list[str] = []
    subcollection_properties: list[str] = []
    for property_name in property_names:
        property = collection.get_property(property_name)
        assert isinstance(property, PropertyMetadata)
        if property.is_subcollection:
            assert isinstance(property, SubcollectionRelationshipMetadata)
            subcollection_properties.append(property.name)
        else:
            assert isinstance(property, ScalarAttributeMetadata)
            scalar_properties.append(property.name)
    scalar_properties.sort()
    subcollection_properties.sort()
    if isinstance(collection, SimpleTableMetadata):
        if verbose:
            lines.append(f"Table path: {collection.table_path}")
            lines.append(
                f"Unique properties of collection: {collection.unique_properties}"
            )
    else:
        raise ValueError(
            f"Unrecognized type of collection: {collection.__class__.__name__}"
        )
    if len(scalar_properties) == 0:
        lines.append("Scalar properties: collection has no scalar properties")
    else:
        if verbose:
            lines.append("Scalar properties:")
            for scalar_property in scalar_properties:
                lines.append(f"  {scalar_property}")
        else:
            lines.append(
                f"Scalar properties: {', '.join(property for property in scalar_properties)}"
            )
    if len(subcollection_properties) == 0:
        lines.append(
            "Subcollection properties: collection has no subcollection properties"
        )
    else:
        if verbose:
            lines.append("Subcollection properties:")
            for subcollection_property in subcollection_properties:
                lines.append(f"  {subcollection_property}")
        else:
            lines.append(
                f"Subcollection properties: {', '.join(property for property in subcollection_properties)}"
            )
    if len(scalar_properties) > 0 or len(subcollection_properties) > 0:
        lines.append(
            f"Call pydough.explain(graph['{collection.name}'][property_name]) to learn more about any of these properties."
        )
    return "\n".join(lines)


def explain_graph(graph: GraphMetadata, verbose: bool) -> str:
    """
    Displays information about a PyDough metadata graph, namely its name and
    the names of the collections it contains.

    Args:
        `graph`: the metadata graph being examined.
        `verbose`: if true, displays more detailed information about `graph` in
        in a less compact format.

    Returns:
        A string explanation of `graph`.
    """
    lines: list[str] = []
    lines.append(f"PyDough graph: {graph.name}")
    collection_names: list[str] = sorted(graph.get_collection_names())
    if len(collection_names) == 0:
        lines.append("Collections: graph contains no collections")
    else:
        if verbose:
            lines.append("Collections:")
            for collection_name in collection_names:
                lines.append(f"  {collection_name}")
        else:
            lines.append(f"Collections: {', '.join(collection_names)}")
        lines.append(
            "Call pydough.explain(graph[collection_name]) to learn more about any of these collections.\n"
            "Call pydough.explain_structure(graph) to see how all of the collections in the graph are connected."
        )
    return "\n".join(lines)


def find_unqualified_root(node: UnqualifiedNode) -> UnqualifiedRoot | None:
    """
    Recursively searches for the ancestor unqualified root of an unqualified
    node.

    Args:
        `node`: the node being searched for its underlying root node.

    Returns:
        The underlying root node if one can be found, otherwise None.
    """
    match node:
        case UnqualifiedRoot():
            return node
        case (
            UnqualifiedAccess()
            | UnqualifiedCalc()
            | UnqualifiedWhere()
            | UnqualifiedOrderBy()
            | UnqualifiedTopK()
            | UnqualifiedPartition()
        ):
            predecessor: UnqualifiedNode = node._parcel[0]
            return find_unqualified_root(predecessor)
        case _:
            return None


def explain_unqualified(node: UnqualifiedNode, verbose: bool) -> str:
    """
    Displays information about an unqualified node, if it is possible to
    qualify the node as a collection. If not, then `explain_term` may need to
    be called. The information displayed may include:
    - The structure of the collection, once qualified.
    - What operation the most recent operation of the collection is doing.
    - Any child collections that are derived by the collection.
    - How many BACK levels can be accessed.
    - The subcollections & expressions that are accessible from the collection.
    - The expressions that would be included if the collection was executed.

    Args:
        `node`: the unqualified node object being examined.
        `verbose`: if true, displays more detailed information about `node` and
        in a less compact format.

    Returns:
        An explanation of `node`.
    """
    lines: list[str] = []
    qualified_node: PyDoughAST | None = None

    # Attempt to qualify the node, dumping an appropriate message if it could
    # not be qualified
    try:
        root: UnqualifiedRoot | None = find_unqualified_root(node)
        if root is not None:
            qualified_node = qualify_node(node, root._parcel[0])
        else:
            # If the root is None, it means that the node was an expression
            # without information about its context.
            lines.append(
                f"Cannot call pydough.explain on {display_raw(node)}.\n"
                "Did you mean to use pydough.explain_term?"
            )
    except PyDoughASTException as e:
        # If the qualification failed, dump an appropriate message indicating
        # why pydough_explain did not work on it.
        if "Unrecognized term" in str(e):
            lines.append(
                f"{str(e)}\n"
                "This could mean you accessed a property using a name that does not exist, or\n"
                "that you need to place your PyDough code into a context for it to make sense.\n"
                "Did you mean to use pydough.explain_term?"
            )
        else:
            raise e

    # If the qualification succeeded, dump info about the qualified node.
    if isinstance(qualified_node, PyDoughExpressionAST):
        lines.append(
            "If pydough.explain is called on an unqualified PyDough code, it is expected to\n"
            "be a collection, but instead received the following expression:\n"
            f" {qualified_node.to_string()}\n"
            "Did you mean to use pydough.explain_term?"
        )
    elif isinstance(qualified_node, PyDoughCollectionAST):
        if verbose:
            # Dump the structure of the collection
            lines.append("PyDough collection representing the following logic:")
            if verbose:
                for line in qualified_node.to_tree_string().splitlines():
                    lines.append(f"  {line}")
            else:
                lines.append(f"  {qualified_node.to_string()}")
            lines.append("")

        # Explain what the specific node does
        collection_name: str
        property_name: str
        tree_string: str
        regular_string: str
        expr_string: str
        match qualified_node:
            case GlobalContext():
                lines.append(
                    "This node is a reference to the global context for the entire graph. An operation must be done onto this node (e.g. a CALC or accessing a collection) before it can be executed."
                )
            case TableCollection():
                collection_name = qualified_node.collection.name
                lines.append(
                    f"This node, specifically, accesses the collection {collection_name}.\n"
                    f"Call pydough.explain(graph['{collection_name}']) to learn more about this collection."
                )
            case SubCollection():
                collection_name = qualified_node.subcollection_property.collection.name
                property_name = qualified_node.subcollection_property.name
                lines.append(
                    f"This node, specifically, accesses the subcollection {collection_name}.{property_name}. Call pydough.explain(graph['{collection_name}']['{property_name}']) to learn more about this subcollection property."
                )
            case PartitionChild():
                lines.append(
                    f"This node, specifically, accesses the unpartitioned data of a partitioning (child name: {qualified_node.partition_child_name})."
                )
                lines.append("Using BACK(1) will access the partitioned data.")
            case ChildOperator():
                if len(qualified_node.children):
                    lines.append(
                        "This node first derives the following children before doing its main task:"
                    )
                    for idx, child in enumerate(qualified_node.children):
                        if verbose:
                            lines.append(f"  child ${idx + 1}:")
                            for line in child.to_tree_string().splitlines()[1:]:
                                lines.append(f"  {line}")
                        else:
                            lines.append(f"  child ${idx + 1}: {child.to_string()}")
                    lines.append("")
                match qualified_node:
                    case Calc():
                        lines.append(
                            "The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:"
                        )
                        for name in sorted(qualified_node.calc_terms):
                            suffix: str = ""
                            expr: PyDoughExpressionAST = qualified_node.get_expr(name)
                            tree_string = expr.to_string(True)
                            regular_string = expr.to_string(False)
                            if tree_string != regular_string:
                                suffix += f", aka {regular_string}"
                            if name in qualified_node.preceding_context.all_terms:
                                if (
                                    isinstance(expr, Reference)
                                    and expr.term_name == name
                                ):
                                    suffix += " (propagated from previous collection)"
                                else:
                                    suffix += f" (overwrites existing value of {name})"
                            lines.append(f"  {name} <- {tree_string}{suffix}")
                    case Where():
                        lines.append(
                            "The main task of this node is to filter on the following conditions:"
                        )
                        conditions: list[PyDoughExpressionAST] = []
                        if (
                            isinstance(qualified_node.condition, ExpressionFunctionCall)
                            and qualified_node.condition.operator == pydop.BAN
                        ):
                            for arg in qualified_node.condition.args:
                                assert isinstance(arg, PyDoughExpressionAST)
                                conditions.append(arg)
                        else:
                            conditions.append(qualified_node.condition)
                        for condition in conditions:
                            tree_string = condition.to_string(True)
                            regular_string = condition.to_string(False)
                            expr_string = tree_string
                            if tree_string != regular_string:
                                expr_string += f", aka {regular_string}"
                            lines.append(f"  {expr_string}")
                    case OrderBy():
                        if isinstance(qualified_node, TopK):
                            lines.append(
                                f"The main task of this node is to sort the collection on the following and keep the first {qualified_node.records_to_keep} records:"
                            )
                        else:
                            lines.append(
                                "The main task of this node is to sort the collection on the following:"
                            )
                        for idx, order_term in enumerate(qualified_node.collation):
                            expr_string = "  "
                            if idx > 0:
                                expr_string += "with ties broken by: "
                            tree_string = order_term.expr.to_string(True)
                            regular_string = order_term.expr.to_string(False)
                            expr_string += tree_string
                            if tree_string != regular_string:
                                expr_string += f", aka {regular_string}"
                            expr_string += ", in "
                            expr_string += (
                                "ascending" if order_term.asc else "descending"
                            )
                            expr_string += " order with nulls at the "
                            expr_string += "end" if order_term.na_last else "start"
                            lines.append(expr_string)
                    case PartitionBy():
                        lines.append(
                            "The main task of this node is to partition the child data on the following keys:"
                        )
                        for key in qualified_node.keys:
                            lines.append(f"  {key.expr.to_string(True)}")
                        lines.append(
                            f"Note: the subcollection of this collection containing records from the unpartitioned data is called '{qualified_node.child_name}'."
                        )
                    case _:
                        raise NotImplementedError(qualified_node.__class__.__name__)
            case _:
                raise NotImplementedError(qualified_node.__class__.__name__)

        if verbose:
            # Dump the calc terms of the collection
            if len(qualified_node.calc_terms) > 0:
                lines.append(
                    "\nThe following terms will be included in the result if this collection is executed:\n"
                    f"  {', '.join(sorted(qualified_node.calc_terms))}"
                )
            else:
                lines.append(
                    "\nThe collection does not have any terms that can be included in a result if it is executed."
                )

            # Identify the number of BACK levels that are accessible
            back_counter: int = 0
            copy_node: PyDoughCollectionAST = qualified_node
            while copy_node.ancestor_context is not None:
                back_counter += 1
                copy_node = copy_node.ancestor_context
            if back_counter == 0:
                lines.append("\nIt is not possible to use BACK from this collection.")
            elif back_counter == 1:
                lines.append(
                    "\nIt is possible to use BACK to go up to 1 level above this collection."
                )
            else:
                lines.append(
                    f"\nIt is possible to use BACK to go up to {back_counter} levels above this collection."
                )

        # Dump the collection & expression terms of the collection
        expr_names: list[str] = []
        collection_names: list[str] = []
        for name in qualified_node.all_terms:
            term: PyDoughAST = qualified_node.get_term(name)
            if isinstance(term, PyDoughExpressionAST):
                expr_names.append(name)
            else:
                collection_names.append(name)
        expr_names.sort()
        collection_names.sort()

        if len(expr_names) > 0:
            lines.append(
                "\n"
                "The collection has access to the following expressions:\n"
                f"  {', '.join(expr_names)}"
            )

        if len(collection_names) > 0:
            lines.append(
                "\n"
                "The collection has access to the following collections:\n"
                f"  {', '.join(collection_names)}"
            )

        if len(expr_names) > 0 or len(collection_names) > 0:
            lines.append(
                "\n"
                "Call pydough.explain_term(collection, term) to learn more about any of these\n"
                "expressions or collections that the collection has access to."
            )

        if not verbose:
            lines.append(
                "\n" "Call pydough.explain(collection, verbose=True) for more details."
            )

    return "\n".join(lines)


def explain(data: AbstractMetadata | UnqualifiedNode, verbose: bool = False) -> str:
    """
    Displays information about a PyDough metadata object or unqualified node.
    The metadata could be for a graph, collection, or property. An unqualified
    node can only be passed in if it is possible to qualify it as a PyDough
    collection. If not, then `pydough.explain_term` may need to be used.

    Args:
        `data`: the metadata or unqualified node object being examined.
        `verbose`: if true, displays more detailed information about `data` and
        in a less compact format.

    Returns:
        An explanation of `data`.
    """
    match data:
        case GraphMetadata():
            return explain_graph(data, verbose)
        case CollectionMetadata():
            return explain_collection(data, verbose)
        case PropertyMetadata():
            return explain_property(data, verbose)
        case UnqualifiedNode():
            return explain_unqualified(data, verbose)
        case _:
            raise ValueError(
                f"Cannot call pydough.explain on argument of type {data.__class__.__name__}"
            )


def explain_structure(graph: GraphMetadata) -> str:
    """
    Displays information about a PyDough metadata graph, including the
    following:
    - The names of each collection in the graph.
    - For each collection, the names of all of it scalar and subcollection
      properties.
    - For each of those subcollection properties:
        - The collection it maps to.
        - The cardinality of the connection.
        - The name of the reverse relationship.

    Args:
        `graph`: the metadata graph being examined.

    Returns:
        The string representation of the graph's structure.
    """
    assert isinstance(graph, GraphMetadata)
    lines: list[str] = []
    lines.append(f"Structure of PyDough graph: {graph.name}")
    collection_names: list[str] = sorted(graph.get_collection_names())
    if len(collection_names) == 0:
        lines.append("  Graph contains no collections")
    else:
        for collection_name in collection_names:
            lines.append(f"\n  {collection_name}")
            collection = graph.get_collection(collection_name)
            assert isinstance(collection, CollectionMetadata)
            scalar_properties: list[str] = []
            subcollection_properties: list[str] = []
            for property_name in collection.get_property_names():
                property = collection.get_property(property_name)
                assert isinstance(property, PropertyMetadata)
                if property.is_subcollection:
                    assert isinstance(property, SubcollectionRelationshipMetadata)
                    assert isinstance(property, ReversiblePropertyMetadata)
                    card = "multiple" if property.is_plural else "one member of"
                    subcollection_properties.append(
                        f"{property.name} [{card} {property.other_collection.name}] (reverse of {property.other_collection.name}.{property.reverse_name})"
                    )
                else:
                    assert isinstance(property, ScalarAttributeMetadata)
                    scalar_properties.append(property.name)
            scalar_properties.sort()
            subcollection_properties.sort()
            combined_lies = scalar_properties + subcollection_properties
            for idx, line in enumerate(combined_lies):
                prefix = "  └── " if idx == (len(combined_lies) - 1) else "  ├── "
                lines.append(f"{prefix}{line}")
    return "\n".join(lines)


def collection_in_context_string(
    context: PyDoughCollectionAST, collection: PyDoughCollectionAST
) -> str:
    """
    Converts a collection in the context of another collection into a single
    string in a way that elides back collection references. For example,
    if the context is A.B.C.D, and the collection is BACK(2).E.F, the result
    would be "A.B.E.F".

    Args:
        `context`: the collection representing the context that `collection`
        exists within.
        `collection`: the collection that exists within `context`.

    Returns:
        The desired string representation of context and collection combined.
    """
    if isinstance(collection, BackReferenceCollection):
        ancestor: PyDoughCollectionAST = context
        for _ in range(collection.back_levels):
            assert ancestor.ancestor_context is not None
            ancestor = ancestor.ancestor_context
        return f"{ancestor.to_string()}.{collection.term_name}"
    elif (
        collection.preceding_context is not None
        and collection.preceding_context is not context
    ):
        return f"{collection_in_context_string(context, collection.preceding_context)}.{collection.standalone_string}"
    elif collection.ancestor_context == context:
        return f"{context.to_string()}.{collection.standalone_string}"
    else:
        assert collection.ancestor_context is not None
        return f"{collection_in_context_string(context, collection.ancestor_context)}.{collection.standalone_string}"


def explain_term(
    node: UnqualifiedNode, term: UnqualifiedNode, verbose: bool = False
) -> str:
    """
    Displays information about an unqualified node as it exists within
    the context of an unqualified node. For example, if
    `explain_terms(Nations, name)` is called, it will display information about
    the `name` property of `Nations`. This information can include:
    - The structure of the qualified `collection` and `term`
    - Any additional children of the collection that must be derived in order
      to derive `term`.
    - The meaning of `term` within `collection`.
    - The cardinality of `term` within `collection`.
    - Examples of how to use `term` within `collection`.
    - How to learn more about `term`.

    Args:
        `node`: the unqualified node that, when qualified, becomes a collection
        that is used as the context through which `term` is derived.
        `term`: the unqualified node that information is being sought about.
        This term will only make sense if it is qualified within the context of
        `node`. This term could be an expression or a collection.
        `verbose`: if true, displays more detailed information about `node` and
        `term` in a less compact format.

    Returns:
        An explanation of `term` as it exists within the context of `node`.
    """

    lines: list[str] = []
    root: UnqualifiedRoot | None = find_unqualified_root(node)
    qualified_node: PyDoughAST | None = None

    try:
        if root is None:
            lines.append(
                f"Invalid first argument to pydough.explain_term: {display_raw(node)}"
            )
        else:
            qualified_node = qualify_node(node, root._parcel[0])
    except PyDoughASTException as e:
        if "Unrecognized term" in str(e):
            lines.append(
                f"Invalid first argument to pydough.explain_term: {display_raw(node)}"
                f"  {str(e)}"
                "This could mean you accessed a property using a name that does not exist, or\n"
                "that you need to place your PyDough code into a context for it to make sense."
            )
        else:
            raise e

    if isinstance(qualified_node, PyDoughExpressionAST):
        lines.append(
            "The first argument of pydough.explain_term is expected to be a collection, but"
        )
        lines.append("instead received the following expression:")
        lines.append(f" {qualified_node.to_string()}")
    elif qualified_node is not None and root is not None:
        assert isinstance(qualified_node, PyDoughCollectionAST)
        new_children, qualified_term = qualify_term(
            qualified_node, term, root._parcel[0]
        )
        if verbose:
            lines.append("Collection:")
            for line in qualified_node.to_tree_string().splitlines():
                lines.append(f"  {line}")
        else:
            lines.append(f"Collection: {qualified_node.to_string()}")
        lines.append("")
        if len(new_children) > 0:
            lines.append(
                "The evaluation of this term first derives the following additional children to the collection before doing its main task:"
            )
            for idx, child in enumerate(new_children):
                if verbose:
                    lines.append(f"  child ${idx + 1}:")
                    for line in child.to_tree_string().splitlines()[1:]:
                        lines.append(f"  {line}")
                else:
                    lines.append(f"  child ${idx + 1}: {child.to_string()}")
            lines.append("")
        # If the qualification succeeded, dump info about the qualified node,
        # depending on what its nature is:
        if isinstance(qualified_term, PyDoughExpressionAST):
            lines.append(
                f"The term is the following expression: {qualified_term.to_string(True)}"
            )
            lines.append("")
            collection: PyDoughCollectionAST = qualified_node
            expr: PyDoughExpressionAST = qualified_term
            while True:
                match expr:
                    case ChildReferenceExpression():
                        lines.append(
                            f"This is a reference to expression '{expr.term_name}' of child ${expr.child_idx + 1}"
                        )
                        break
                    case BackReferenceExpression():
                        back_idx_str: str
                        match expr.back_levels % 10:
                            case 1:
                                back_idx_str = f"{expr.back_levels}st"
                            case 2:
                                back_idx_str = f"{expr.back_levels}2nd"
                            case 3:
                                back_idx_str = f"{expr.back_levels}3rd"
                            case _:
                                back_idx_str = f"{expr.back_levels}th"
                        lines.append(
                            f"This is a reference to expression '{expr.term_name}' of the {back_idx_str} ancestor of the collection, which is the following:"
                        )
                        if verbose:
                            for line in expr.ancestor.to_tree_string().splitlines():
                                lines.append(f"  {line}")
                        else:
                            lines.append(f"  {expr.ancestor.to_string()}")
                        break
                    case Reference():
                        expr = collection.get_expr(expr.term_name)
                        if (
                            isinstance(expr, Reference)
                            and collection.preceding_context is not None
                        ):
                            collection = collection.preceding_context
                    case ColumnProperty():
                        lines.append(
                            f"This is column '{expr.column_property.name}' of collection '{expr.column_property.collection.name}'"
                        )
                        break
                    case ExpressionFunctionCall():
                        if isinstance(expr.operator, pydop.BinaryOperator):
                            lines.append(
                                f"This expression combines the following arguments with the '{expr.operator.function_name}' operator:"
                            )
                        elif (
                            expr.operator in (pydop.COUNT, pydop.NDISTINCT)
                            and len(expr.args) == 1
                            and isinstance(expr.args[0], PyDoughCollectionAST)
                        ):
                            metric: str = (
                                "records"
                                if expr.operator == pydop.COUNT
                                else "distinct records"
                            )
                            lines.append(
                                f"This expression counts how many {metric} of the following subcollection exist for each record of the collection:"
                            )
                        elif (
                            expr.operator in (pydop.HAS, pydop.HASNOT)
                            and len(expr.args) == 1
                            and isinstance(expr.args[0], PyDoughCollectionAST)
                        ):
                            predicate: str = (
                                "has" if expr.operator == pydop.HAS else "does not have"
                            )
                            lines.append(
                                f"This expression returns whether the collection {predicate} any records of the following subcollection:"
                            )
                        else:
                            suffix = (
                                ", aggregating them into a single value for each record of the collection"
                                if expr.operator.is_aggregation
                                else ""
                            )
                            lines.append(
                                f"This expression calls the function '{expr.operator.function_name}' on the following arguments{suffix}:"
                            )
                        for arg in expr.args:
                            assert isinstance(
                                arg, (PyDoughCollectionAST, PyDoughExpressionAST)
                            )
                            lines.append(f"  {arg.to_string()}")
                        lines.append("")
                        lines.append(
                            "Call pydough.explain_term with this collection and any of the arguments to learn more about them."
                        )
                        break
                    case _:
                        raise NotImplementedError(expr.__class__.__name__)
            if verbose:
                lines.append("")
                if qualified_term.is_singular(qualified_node.starting_predecessor):
                    lines.append(
                        "This term is singular with regards to the collection, meaning it can be placed in a CALC of a collection."
                    )
                    lines.append("For example, the following is valid:")
                    lines.append(
                        f"  {qualified_node.to_string()}({qualified_term.to_string()})"
                    )
                else:
                    lines.append(
                        "This expression is plural with regards to the collection, meaning it can be placed in a CALC of a collection if it is aggregated."
                    )
                    lines.append("For example, the following is valid:")
                    lines.append(
                        f"  {qualified_node.to_string()}(COUNT({qualified_term.to_string()}))"
                    )
        else:
            assert isinstance(qualified_term, PyDoughCollectionAST)
            lines.append("The term is the following child of the collection:")
            if verbose:
                for line in qualified_term.to_tree_string().splitlines():
                    lines.append(f"  {line}")
            else:
                lines.append(f"  {qualified_term.to_string()}")
            if verbose:
                lines.append("")
                assert (
                    len(qualified_term.calc_terms) > 0
                ), "Child collection has no expression terms"
                chosen_term_name: str = min(qualified_term.calc_terms)
                if qualified_term.starting_predecessor.is_singular(
                    qualified_node.starting_predecessor
                ):
                    lines.append(
                        "This child is singular with regards to the collection, meaning its scalar terms can be accessed by the collection as if they were scalar terms of the expression."
                    )
                    lines.append("For example, the following is valid:")
                    lines.append(
                        f"  {qualified_node.to_string()}({qualified_term.to_string()}.{chosen_term_name})"
                    )
                else:
                    lines.append(
                        "This child is plural with regards to the collection, meaning its scalar terms can only be accessed by the collection if they are aggregated."
                    )
                    lines.append("For example, the following are valid:")
                    lines.append(
                        f"  {qualified_node.to_string()}(COUNT({qualified_term.to_string()}.{chosen_term_name}))"
                    )
                    lines.append(
                        f"  {qualified_node.to_string()}.WHERE(HAS({qualified_term.to_string()}))"
                    )
                    lines.append(
                        f"  {qualified_node.to_string()}.ORDER_BY(COUNT({qualified_term.to_string()}).DESC())"
                    )
                lines.append("")
                lines.append(
                    "To learn more about this child, you can try calling pydough.explain on the following:"
                )
                lines.append(
                    f"  {collection_in_context_string(qualified_node, qualified_term)}"
                )

    return "\n".join(lines)
