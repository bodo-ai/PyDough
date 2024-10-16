"""
TODO: add file-level docstring
"""

from typing import Dict, List, Tuple
from .graphs import GraphMetadata
from .errors import (
    PyDoughMetadataException,
    verify_json_has_property_with_type,
    verify_has_type,
)
from .collections import CollectionMetadata
from .properties import PropertyMetadata
import json


def parse_json_metadata(file_path: str, graph_name: str) -> GraphMetadata:
    """
    TODO: add function docstring.
    """
    with open(file_path, "r") as f:
        as_json = json.load(f)
    if not isinstance(as_json, dict):
        raise PyDoughMetadataException(
            "PyDough metadata expected to be a JSON file containing a JSON object."
        )
    if graph_name not in as_json:
        raise PyDoughMetadataException(
            f"PyDough metadata does not contain a graph named {graph_name!r}"
        )
    graph_json = as_json[graph_name]
    return parse_graph(graph_name, graph_json)


def parse_graph(graph_name: str, graph_json: Dict) -> None:
    """
    TODO: add function docstring.
    """
    verify_has_type(graph_json, dict, "metadata for PyDough graph")
    graph = GraphMetadata(graph_name)

    # A list that will store each collection property in the metadata
    # before it is defined and added to its collection, so all of the properties
    # can be sorted based on their dependencies. The list stores the properties
    # as tuples in the form (collection_name, property_name, property_json)
    raw_properties: List[Tuple[str, str, dict]] = []

    # Iterate through all the key-value pairs in the graph to set up the
    # corresponding collections as empty metadata that will later be filled
    # with properties, and also obtain each of the properties.
    for collection_name in graph_json:
        # Add the raw collection metadata to the collections dictionary
        collection_json = graph_json[collection_name]
        CollectionMetadata.parse_from_json(graph, collection_name, collection_json)

        # Add the unprocessed properties of each collection to the properties list
        # (the parsing of the collection verified that the 'properties' key exists)
        properties_json = graph_json[collection_name]["properties"]
        for property_name in properties_json:
            property_json = properties_json[property_name]
            raw_properties.append((collection_name, property_name, property_json))

    ordered_properties = topologically_sort_properties(raw_properties)
    for collection_name, property_name, property_json in ordered_properties:
        verify_json_has_property_with_type(
            graph.collections, collection_name, CollectionMetadata, graph.error_name
        )
        collection = graph.collections[collection_name]
        PropertyMetadata.parse_from_json(collection, property_name, property_json)

    return graph


def topologically_sort_properties(
    raw_properties: List[PropertyMetadata],
) -> List[PropertyMetadata]:
    ordered_properties = sorted(
        raw_properties, key=lambda x: x[2]["type"] == "compound"
    )
    for i, p in enumerate(ordered_properties):
        print(i, p)
    return ordered_properties
