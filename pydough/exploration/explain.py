"""
Implementation of the `pydough.explain` function, which provides detailed
explanations of PyDough metadata objects and unqualified nodes.
"""

__all__ = ["explain"]

import pydough
import pydough.pydough_operators as pydop
from pydough.configs import PyDoughConfigs
from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.collections import CollectionMetadata, SimpleTableMetadata
from pydough.metadata.graphs import GraphMetadata
from pydough.metadata.properties import (
    CartesianProductMetadata,
    CompoundRelationshipMetadata,
    GeneralJoinMetadata,
    PropertyMetadata,
    ReversiblePropertyMetadata,
    ScalarAttributeMetadata,
    SimpleJoinMetadata,
    SubcollectionRelationshipMetadata,
    TableColumnMetadata,
)
from pydough.qdag import (
    BackReferenceExpression,
    Calculate,
    ChildOperator,
    ExpressionFunctionCall,
    GlobalContext,
    OrderBy,
    PartitionBy,
    PartitionChild,
    PyDoughCollectionQDAG,
    PyDoughExpressionQDAG,
    PyDoughQDAG,
    PyDoughQDAGException,
    Reference,
    SubCollection,
    TableCollection,
    TopK,
    Where,
)
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    display_raw,
    qualify_node,
)

from .term import find_unqualified_root


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
    - For subcollection properties, the collection it connects to and any
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
                    case GeneralJoinMetadata():
                        lines.append(
                            "The subcollection relationship is defined by a following general join condition."
                        )
                        lines.append(f"The condition is: {property.condition!r}")
                        lines.append(
                            f"The parent & child collections are referred to as {property.self_name!r} and {property.other_name!r}, respectively"
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


def explain_unqualified(node: UnqualifiedNode, verbose: bool) -> str:
    """
    Displays information about an unqualified node, if it is possible to
    qualify the node as a collection. If not, then `explain_term` may need to
    be called. The information displayed may include:
    - The structure of the collection, once qualified.
    - What operation the most recent operation of the collection is doing.
    - Any child collections that are derived by the collection.
    - The sub-collections & expressions that are accessible from the collection.
    - The expressions that would be included if the collection was executed.

    Args:
        `node`: the unqualified node object being examined.
        `verbose`: if true, displays more detailed information about `node` and
        in a less compact format.

    Returns:
        An explanation of `node`.
    """
    lines: list[str] = []
    qualified_node: PyDoughQDAG | None = None
    config: PyDoughConfigs = pydough.active_session.config
    # Attempt to qualify the node, dumping an appropriate message if it could
    # not be qualified
    try:
        root: UnqualifiedRoot | None = find_unqualified_root(node)
        if root is not None:
            qualified_node = qualify_node(node, root._parcel[0], config)
        else:
            # If the root is None, it means that the node was an expression
            # without information about its context.
            lines.append(
                f"Cannot call pydough.explain on {display_raw(node)}.\n"
                "Did you mean to use pydough.explain_term?"
            )
    except PyDoughQDAGException as e:
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
    if isinstance(qualified_node, PyDoughExpressionQDAG):
        lines.append(
            "If pydough.explain is called on an unqualified PyDough code, it is expected to\n"
            "be a collection, but instead received the following expression:\n"
            f" {qualified_node.to_string()}\n"
            "Did you mean to use pydough.explain_term?"
        )
    elif isinstance(qualified_node, PyDoughCollectionQDAG):
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
                    "This node is a reference to the global context for the entire graph. An operation must be done onto this node (e.g. a CALCULATE or accessing a collection) before it can be executed."
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
                    case Calculate():
                        lines.append(
                            "The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:"
                        )
                        for name in sorted(qualified_node.calc_terms):
                            suffix: str = ""
                            expr: PyDoughExpressionQDAG = qualified_node.get_expr(name)
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
                            elif isinstance(expr, BackReferenceExpression):
                                suffix = (
                                    " (referencing an alias defined in an ancestor)"
                                )
                            lines.append(f"  {name} <- {tree_string}{suffix}")
                    case Where():
                        lines.append(
                            "The main task of this node is to filter on the following conditions:"
                        )
                        conditions: list[PyDoughExpressionQDAG] = []
                        if (
                            isinstance(qualified_node.condition, ExpressionFunctionCall)
                            and qualified_node.condition.operator == pydop.BAN
                        ):
                            for arg in qualified_node.condition.args:
                                assert isinstance(arg, PyDoughExpressionQDAG)
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
                            f"Note: the subcollection of this collection containing records from the unpartitioned data is called '{qualified_node.child.name}'."
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

        # Dump the collection & expression terms of the collection
        expr_names: list[str] = []
        collection_names: list[str] = []
        for name in qualified_node.all_terms:
            term: PyDoughQDAG = qualified_node.get_term(name)
            if isinstance(term, PyDoughExpressionQDAG):
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
                "\nCall pydough.explain(collection, verbose=True) for more details."
            )

    return "\n".join(lines)


def explain(
    data: AbstractMetadata | UnqualifiedNode,
    verbose: bool = False,
    config: PyDoughConfigs | None = None,
) -> str:
    """
    Displays information about a PyDough metadata object or unqualified node.
    The metadata could be for a graph, collection, or property. An unqualified
    node can only be passed in if it is possible to qualify it as a PyDough
    collection. If not, then `pydough.explain_term` may need to be used.

    Args:
        `data`: the metadata or unqualified node object being examined.
        `verbose`: if true, displays more detailed information about `data` and
        in a less compact format.
        `config`: the configuration to use for the explanation. If not provided,
        the active session's configuration will be used.

    Returns:
        An explanation of `data`.
    """
    if config is None:
        config = pydough.active_session.config
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
