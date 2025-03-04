"""
Implementation of the `pydough.explain_structure` function, which provides
detailed explanations about the overall structure of a PyDough metadata graph.
"""

__all__ = ["explain_structure"]


import pydough
from pydough.configs import PyDoughConfigs
from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.graphs import GraphMetadata
from pydough.metadata.properties import (
    PropertyMetadata,
    ReversiblePropertyMetadata,
    ScalarAttributeMetadata,
    SubcollectionRelationshipMetadata,
)


def explain_structure(
    graph: GraphMetadata, config: PyDoughConfigs | None = None
) -> str:
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
        `config`: the configuration to use for the explanation. If not provided,
        the active session's configuration will be used.

    Returns:
        The string representation of the graph's structure.
    """
    assert isinstance(graph, GraphMetadata)
    if config is None:
        config = pydough.active_session.config
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
