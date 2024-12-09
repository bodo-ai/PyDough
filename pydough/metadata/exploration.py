"""
TODO: add file-level docstring
"""

__all__ = ["explain_meta"]


from .abstract_metadata import AbstractMetadata
from .collections import CollectionMetadata, SimpleTableMetadata
from .graphs import GraphMetadata
from .properties import (
    CartesianProductMetadata,
    CompoundRelationshipMetadata,
    PropertyMetadata,
    ReversiblePropertyMetadata,
    ScalarAttributeMetadata,
    SimpleJoinMetadata,
    SubcollectionRelationshipMetadata,
    TableColumnMetadata,
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


def explain_property(property: PropertyMetadata) -> str:
    """
    TODO
    """
    lines: list[str] = []
    lines.append(f"PyDough property: {property.error_name}")
    match property:
        case TableColumnMetadata():
            assert isinstance(property.collection, SimpleTableMetadata)
            lines.append(
                f"Column name: {property.collection.table_path}.{property.column_name}"
            )
            lines.append(f"Data type: {property.data_type.json_string}")
        case ReversiblePropertyMetadata():
            parent_name: str = property.collection.name
            child_name: str = property.other_collection.name
            lines.append(f"Connects collection {parent_name} to {child_name}")
            lines.append(
                f"Cardinality of connection: {property_cardinality_string(property)}"
            )
            lines.append("Is reversible: yes")
            lines.append(f"Reverse property: {child_name}.{property.reverse_name}")
            match property:
                case CartesianProductMetadata():
                    lines.append(
                        f"Note: this is a cartesian-product relationship, meaning that every record of {parent_name} matches onto every record of {child_name}."
                    )
                case SimpleJoinMetadata():
                    conditions: list[str] = []
                    for lhs_key_name, rhs_key_names in property.keys.items():
                        for rhs_key_name in rhs_key_names:
                            conditions.append(
                                f"  {parent_name}.{lhs_key_name} == {child_name}.{rhs_key_name}"
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
                        f"{parent_name}.{property.primary_property.name}"
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
        case _:
            raise ValueError(
                f"Unrecognized type of property: {property.__class__.__name__}"
            )
    return "\n".join(lines)


def explain_collection(collection: CollectionMetadata) -> str:
    """
    TODO
    """
    lines: list[str] = []
    lines.append(f"PyDough collection: {collection.error_name}")
    property_names: list[str] = sorted(collection.get_property_names())
    scalar_properties: list[ScalarAttributeMetadata] = []
    subcollection_properties: list[SubcollectionRelationshipMetadata] = []
    for property_name in property_names:
        property = collection.get_property(property_name)
        assert isinstance(property, PropertyMetadata)
        if property.is_subcollection:
            assert isinstance(property, SubcollectionRelationshipMetadata)
            subcollection_properties.append(property)
        else:
            assert isinstance(property, ScalarAttributeMetadata)
            scalar_properties.append(property)
    scalar_properties.sort(key=lambda p: p.name)
    subcollection_properties.sort(key=lambda p: p.name)
    if isinstance(collection, SimpleTableMetadata):
        lines.append(f"Table path: {collection.table_path}")
        lines.append(f"Unique properties of collection: {collection.unique_properties}")
    else:
        raise ValueError(
            f"Unrecognized type of collection: {collection.__class__.__name__}"
        )
    if len(scalar_properties) == 0:
        lines.append("Scalar properties: collection has no scalar properties")
    else:
        lines.append("Scalar properties:")
        for scalar_property in scalar_properties:
            lines.append(f"  {scalar_property.name}")
    if len(subcollection_properties) == 0:
        lines.append(
            "Subcollection properties: collection has no subcollection properties"
        )
    else:
        lines.append("Subcollection properties:")
        for subcollection_property in subcollection_properties:
            lines.append(f"  {subcollection_property.name}")
    if len(scalar_properties) > 0 or len(subcollection_properties) > 0:
        lines.append(
            f"Call pydough.explain_meta(graph['{collection.name}'][property_name]) to learn more about any of these properties."
        )
    return "\n".join(lines)


def explain_graph(graph: GraphMetadata) -> str:
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
            "Call pydough.explain_meta(graph[collection_name]) to learn more about any of these collections."
        )
    return "\n".join(lines)


def explain_meta(metadata: AbstractMetadata) -> str:
    """
    TODO
    """
    match metadata:
        case GraphMetadata():
            return explain_graph(metadata)
        case CollectionMetadata():
            return explain_collection(metadata)
        case PropertyMetadata():
            return explain_property(metadata)
        case _:
            raise ValueError(
                f"Cannot call pydough.explain_meta on argument of type {metadata.__class__.__name__}"
            )
