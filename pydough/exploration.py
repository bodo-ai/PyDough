"""
TODO: add file-level docstring
"""

__all__ = ["explain"]


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
from .pydough_ast import (
    Calc,
    ChildOperator,
    GlobalContext,
    PyDoughAST,
    PyDoughCollectionAST,
    PyDoughExpressionAST,
    Reference,
    SubCollection,
    TableCollection,
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
    qualify_node,
)


def property_cardinality_string(property: ReversiblePropertyMetadata) -> str:
    """
    TODO
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
    TODO
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
    TODO
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
    TODO
    """
    lines: list[str] = []
    lines.append(f"PyDough graph: {graph.name}")
    collection_names: list[str] = sorted(graph.get_collection_names())
    if len(collection_names) == 0:
        lines.append("Collections: graph contains no collections")
    else:
        lines.append("Collections:")
        for collection_name in collection_names:
            lines.append(f"  {collection_name}")
        lines.append(
            "Call pydough.explain(graph[collection_name]) to learn more about any of these collections."
        )
        lines.append(
            "Call pydough.explain_structure(graph) to see how all of the collections in the graph are connected."
        )
    return "\n".join(lines)


def find_unqualified_root(node: UnqualifiedNode) -> UnqualifiedRoot:
    """
    TODO
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
            raise ValueError(
                f"Cannot call pydough.explain on argument of type {node.__class__.__name__}"
            )


def explain_unqualified(node: UnqualifiedNode, verbose: bool) -> str:
    """
    TODO
    """
    lines: list[str] = []
    root: UnqualifiedRoot = find_unqualified_root(node)
    qualified_node: PyDoughCollectionAST = qualify_node(node, root._parcel[0])

    if verbose:
        # Dump the structure of the collection
        lines.append("PyDough collection representing the following logic:")
        for line in qualified_node.to_tree_string().splitlines():
            lines.append(f"  {line}")

    # Explain what the specific node does
    collection_name: str
    property_name: str
    lines.append("")
    match qualified_node:
        case GlobalContext():
            lines.append(
                "This node is a reference to the global context for the entire graph. An operation must be done onto this node (e.g. a CALC or accessing a collection) before it can be executed."
            )
        case TableCollection():
            collection_name = qualified_node.collection.name
            lines.append(
                f"This node, specifically, accesses the collection {collection_name}. Call pydough.explain(graph['{collection_name}']) to learn more about this collection."
            )
        case SubCollection():
            collection_name = qualified_node.subcollection_property.collection.name
            property_name = qualified_node.subcollection_property.name
            lines.append(
                f"This node, specifically, accesses the subcollection {collection_name}.{property_name}. Call pydough.explain(graph['{collection_name}']['{property_name}']) to learn more about this subcollection property."
            )
        case ChildOperator():
            if len(qualified_node.children):
                lines.append(
                    "This node first derives the following children before doing its main task:"
                )
                for idx, child in enumerate(qualified_node.children):
                    lines.append(f"  child ${idx + 1}:")
                    for line in child.to_tree_string().splitlines()[1:]:
                        lines.append(f"  {line}")
                lines.append("")
            match qualified_node:
                case Calc():
                    lines.append(
                        "The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:"
                    )
                    for name in sorted(qualified_node.calc_terms):
                        suffix: str = ""
                        expr: PyDoughExpressionAST = qualified_node.get_expr(name)
                        tree_string: str = expr.to_string(True)
                        regular_string: str = expr.to_string(False)
                        if tree_string != regular_string:
                            suffix += f", aka {regular_string}"
                        if name in qualified_node.preceding_context.all_terms:
                            if isinstance(expr, Reference) and expr.term_name == name:
                                suffix += " (propagated from previous collection)"
                            else:
                                suffix += f" (overwrites existing value of {name})"
                        lines.append(f"  {name} <- {tree_string}{suffix}")
                case _:
                    raise NotImplementedError
        case _:
            raise NotImplementedError

    if verbose:
        # Dump the calc terms of the collection
        lines.append("")
        if len(qualified_node.calc_terms) > 0:
            lines.append(
                "The following terms will be included in the result if this collection is executed:"
            )
            lines.append(f"  {', '.join(sorted(qualified_node.calc_terms))}")
        else:
            lines.append(
                "The collection does not have any terms that can be included in a result if it is executed."
            )

        # Identify the number of BACK levels that are accessible
        back_counter: int = 0
        copy_node: PyDoughCollectionAST = qualified_node
        lines.append("")
        while copy_node.ancestor_context is not None:
            back_counter += 1
            copy_node = copy_node.ancestor_context
        if back_counter == 0:
            lines.append("It is not possible to use BACK from this collection.")
        elif back_counter == 1:
            lines.append(
                "It is possible to use BACK to go up to 1 level above this collection."
            )
        else:
            lines.append(
                f"It is possible to use BACK to go up to {back_counter} levels above this collection."
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
        lines.append("")
        lines.append("The collection has access to the following expressions:")
        lines.append(f"  {', '.join(expr_names)}")

    if len(collection_names) > 0:
        lines.append("")
        lines.append("The collection has access to the following collections:")
        lines.append(f"  {', '.join(collection_names)}")

    if len(expr_names) > 0 or len(collection_names) > 0:
        lines.append("")
        lines.append(
            "Call pydough.explain_term(collection, term_name) to learn more about any of these expressions or collections that the collection has access to."
        )

    return "\n".join(lines)


def explain(data: AbstractMetadata | UnqualifiedNode, verbose: bool = False) -> str:
    """
    TODO
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
