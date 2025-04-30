"""
The logic used to parse PyDough metadata from a JSON file.
"""

__all__ = ["parse_json_metadata_from_file"]

import json

from .collections import CollectionMetadata, SimpleTableMetadata
from .errors import (
    HasPropertyWith,
    HasType,
    PyDoughMetadataException,
    extract_array,
    extract_bool,
    extract_string,
    is_json_object,
    is_string,
)
from .graphs import GraphMetadata
from .properties import (
    CartesianProductMetadata,
    GeneralJoinMetadata,
    PropertyMetadata,
    ReversiblePropertyMetadata,
    SimpleJoinMetadata,
)


def parse_json_metadata_from_file(file_path: str, graph_name: str) -> GraphMetadata:
    """
    Reads a JSON file to obtain a specific PyDough metadata graph.

    Args:
        `file_path`: the path to the file containing the PyDough metadata for
        the desired graph. This should be a JSON file.
        `graph_name`: the name of the graph from the metadata file that is
        being requested. This should be a key in the JSON file.

    Returns:
        The metadata for the PyDough graph, including all of the collections
        and properties defined within.

    Raises:
        `PyDoughMetadataException`: if the file is malformed in any way that
        prevents parsing it to obtain the desired graph.
    """
    with open(file_path) as f:
        as_json = json.load(f)
    if not isinstance(as_json, list):
        raise PyDoughMetadataException(
            "PyDough metadata expected to be a JSON file containing a JSON "
            + f"array of JSON objects representing metadata graphs, received: {as_json.__class__.__name__}."
        )
    for graph_json in as_json:
        HasType(dict).verify(graph_json, "metadata for PyDough graph")
        HasPropertyWith("name", is_string).verify(
            graph_json, "metadata for PyDough graph"
        )
        name: str = extract_string(graph_json, "name", "metadata for PyDough graph")
        if name == graph_name:
            version: str = extract_string(
                graph_json, "version", "metadata for PyDough graph"
            )
            match version:
                case "V2":
                    return parse_graph_v2(name, graph_json)
                case _:
                    raise PyDoughMetadataException(
                        f"Unrecognized PyDough metadata version: {version!r}"
                    )
    # If we reach this point, then the graph was not found in the file
    raise PyDoughMetadataException(
        f"PyDough metadata file located at {file_path!r} does not "
        + f"contain a graph named {graph_name!r}"
    )


def parse_graph_v2(graph_name: str, graph_json: dict) -> GraphMetadata:
    """
    TODO
    """
    graph: GraphMetadata = GraphMetadata(graph_name)

    # Parse and extract the metadata for all of the collections in the graph.
    collections_json: list = extract_array(graph_json, "collections", graph.error_name)
    for collection_json in collections_json:
        is_json_object.verify(
            collection_json,
            f"metadata for collection descriptions inside {graph.error_name}",
        )
        assert isinstance(collection_json, dict)
        parse_collection_v2(graph, collection_json)

    # Parse and extract the metadata for all of the relationships in the graph.
    relationships_json: list = extract_array(
        graph_json, "relationships", graph.error_name
    )
    for relationship_json in relationships_json:
        is_json_object.verify(
            relationship_json,
            f"metadata for relationship descriptions inside {graph.error_name}",
        )
        assert isinstance(relationship_json, dict)
        parse_relationship_v2(graph, relationship_json)

    # If it exists, parse the additional definitions and verified analysis
    # within the graph to verify they are well-formed.
    if "additional definitions" in graph_json:
        additional_definitions_json: list = extract_array(
            graph_json, "additional definitions", graph.error_name
        )
        for definition_json in additional_definitions_json:
            is_json_object.verify(
                definition_json,
                f"metadata for additional definitions inside {graph.error_name}",
            )
            assert isinstance(definition_json, dict)
            HasPropertyWith("definition", is_string).verify(
                definition_json, "metadata for verified pydough analysis"
            )
            HasPropertyWith("code", is_string).verify(
                definition_json, "metadata for verified pydough analysis"
            )
    if "verified pydough analysis" in graph_json:
        verified_analysis_json: list = extract_array(
            graph_json, "verified pydough analysis", graph.error_name
        )
        for verified_json in verified_analysis_json:
            is_json_object.verify(
                verified_json,
                f"metadata for verified pydough analysis inside {graph.error_name}",
            )
            assert isinstance(verified_json, dict)
            HasPropertyWith("question", is_string).verify(
                verified_json, "metadata for verified pydough analysis"
            )
            HasPropertyWith("code", is_string).verify(
                verified_json, "metadata for verified pydough analysis"
            )
    return graph


def parse_collection_v2(graph: GraphMetadata, collection_json: dict):
    """
    TODO
    """
    collection_name: str = extract_string(
        collection_json, "name", f"metadata for collections within {graph.error_name}"
    )
    collection_type: str = extract_string(
        collection_json, "type", f"metadata for collections within {graph.error_name}"
    )
    match collection_type:
        case "simple table":
            SimpleTableMetadata.parse_from_json(graph, collection_name, collection_json)
        case _:
            raise PyDoughMetadataException(
                f"Unrecognized PyDough collection type for collection {collection_name!r}: {collection_type!r}"
            )


def parse_relationship_v2(graph: GraphMetadata, relationship_json: dict):
    """
    TODO
    """
    relationship_name: str = extract_string(
        relationship_json,
        "name",
        f"metadata for relationships within {graph.error_name}",
    )
    relationship_type: str = extract_string(
        relationship_json,
        "type",
        f"metadata for relationships within {graph.error_name}",
    )
    match relationship_type:
        case "cartesian product":
            CartesianProductMetadata.parse_from_json(
                graph, relationship_name, relationship_json
            )
        case "simple join":
            SimpleJoinMetadata.parse_from_json(
                graph, relationship_name, relationship_json
            )
        case "general join":
            GeneralJoinMetadata.parse_from_json(
                graph, relationship_name, relationship_json
            )
        case "reverse":
            original_collection_name: str = relationship_json["original parent"]
            original_property_name: str = relationship_json["original property"]
            original_collection = graph.get_collection(original_collection_name)
            assert isinstance(original_collection, CollectionMetadata)
            original_property = original_collection.get_property(original_property_name)
            assert isinstance(original_property, PropertyMetadata)
            is_singular: bool = extract_bool(
                relationship_json,
                "singular",
                f"metadata for reverse relationship {relationship_name!r} relationships within {graph.error_name}",
            )
            if not isinstance(original_property, ReversiblePropertyMetadata):
                raise PyDoughMetadataException(
                    f"Property {original_property_name!r} in collection {original_collection_name!r} is not reversible."
                )
            reverse_collection: CollectionMetadata = original_property.child_collection
            reverse_property: ReversiblePropertyMetadata = (
                original_property.build_reverse_relationship(
                    relationship_name, is_singular=is_singular
                )
            )
            reverse_collection.add_property(reverse_property)
        case "custom":
            raise NotImplementedError("Custom relationships are not yet supported.")
        case _:
            raise PyDoughMetadataException(
                f"Unrecognized PyDough relationship type for relationship {relationship_name!r}: {relationship_type!r}"
            )
