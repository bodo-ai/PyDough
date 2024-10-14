"""
TODO: add file-level docstring
"""

from typing import Dict, List
from .graphs import GraphMetadata
from .errors import PyDoughMetadataException
from .collections import CollectionMetadata, SimpleTableMetadata
from .properties import (
    PropertyMetadata,
    TableColumnMetadata,
    SimpleJoinMetadata,
    CompoundRelationshipMetadata,
)
import json


def parse_json_metadata(file_path: str, graph_name: str) -> GraphMetadata:
    """
    TODO: add function doscstring.
    """
    with open(file_path, "r") as f:
        as_json = json.load(f)
    if not isinstance(as_json, dict):
        raise PyDoughMetadataException(
            "PyDough metadata expected to be a JSON file containing a JSON object."
        )
    if graph_name not in as_json:
        raise PyDoughMetadataException(
            f"PyDough metadata does not contain a graph named {repr(graph_name)}"
        )
    graph_json = as_json[graph_name]
    GraphMetadata.verify_json_metadata(graph_name, graph_json)
    return parse_graph(graph_name, graph_json)


def parse_graph(graph_name: str, graph_json: Dict) -> GraphMetadata:
    """
    TODO: add function doscstring.
    """

    # A dictionary that will be used to map each collection name to the
    # collection metadata object it corresponds to.
    collections: Dict[str, CollectionMetadata] = {}

    # A list that will store each collection property in the metadata
    # before it is added to the collection, so all of the properties can
    # be sorted based on their dependencies.
    raw_properties: List[PropertyMetadata] = []

    # Iterate through all the key-value pairs in the graph to set up the
    # corresponding collections as empty metadata that will later be filled
    # with properties, and also obtain each of the properties.
    for collection_name in graph_json:
        # Add the raw collection metadata to the collections dictionary
        collection_json = graph_json[collection_name]
        match collection_json["type"]:
            case "simple_table":
                collections[collection_name] = SimpleTableMetadata(
                    graph_name, collection_name
                )
            case collection_type:
                raise Exception(f"Unrecognized collection type: '{collection_type}'")

        # Add the properties collection metadata to the properties list
        properties_json = collection_json["properties"]
        for property_name in properties_json:
            property_json = properties_json[property_name]
            match property_json["type"]:
                case "table_column":
                    property = TableColumnMetadata(
                        graph_name, collection_name, property_name
                    )
                case "simple_join":
                    property = SimpleJoinMetadata(
                        graph_name, collection_name, property_name
                    )
                case "compound":
                    property = CompoundRelationshipMetadata(
                        graph_name, collection_name, property_name
                    )
                case collection_type:
                    raise Exception(f"Unrecognized property type: '{collection_type}'")
            raw_properties.append(property)

    for collection_name in graph_json:
        collection = collections[collection_name]
        collection.parse_from_json(collections)

    ordered_properties = topologically_sort_properties(raw_properties)
    for property in ordered_properties:
        collection = collections[property.collection_name]
        property.parse_from_json(collections, graph_json)
        collection.add_property(property)

    return GraphMetadata(graph_name, collections)


def topologically_sort_properties(
    raw_properties: List[PropertyMetadata],
) -> List[PropertyMetadata]:
    return raw_properties
