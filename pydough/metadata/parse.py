from typing import Dict
from pydough.metadata.graph import (
    GraphMetadata,
    SimpleTableMetadata,
    PyDoughMetadataException,
)
import json


def parse_metadata(file_path: str, graph_name: str) -> GraphMetadata:
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
    verify_pydough_graph(graph_name, graph_json)
    return parse_graph(graph_name, graph_json)


def verify_pydough_graph(graph_name: str, graph_json: Dict) -> None:
    """
    TODO: add function doscstring.
    """
    if not isinstance(graph_json, dict):
        raise PyDoughMetadataException(
            f"PyDough metadata for Graph {repr(graph_name)} must be a JSON object."
        )
    for collection_name in graph_json:
        verify_pydough_collection(graph_name, collection_name, graph_json)


def verify_pydough_collection(
    graph_name: str, collection_name: str, graph_json: Dict
) -> None:
    """
    TODO: add function doscstring.
    """
    if collection_name == graph_name:
        raise PyDoughMetadataException(
            f"Cannot have collection named {repr(collection_name)} share the same name as the graph containing it."
        )
    collection_json = graph_json[collection_name]
    error_name = f"collection {repr(collection_name)} in graph {repr(graph_name)}"
    if "type" not in collection_json:
        raise PyDoughMetadataException(
            f"Metadata for {error_name} missing required property 'type'."
        )
    if not isinstance(collection_json["type"], str):
        raise PyDoughMetadataException(
            f"Property 'type' of {error_name} must be a string."
        )
    match collection_json["type"]:
        case "simple_table":
            SimpleTableMetadata.verify_metadata(
                graph_name, collection_name, collection_json
            )
        case collection_type:
            raise PyDoughMetadataException(
                f"Unrecognized collection type for {error_name}: {repr(collection_type)}"
            )


def verify_simple_table_collection(graph_name, collection_name, graph_json) -> None:
    """
    TODO: add function doscstring.
    """
    pass


def parse_graph(graph_name: str, graph_json: Dict) -> GraphMetadata:
    """
    TODO: add function doscstring.
    """

    # A dictionary that will be used to map each collection name to the
    # collection metadata object it corresponds to.
    collections = {}

    # A list that will store tuples corresponding to each collection property
    # in the metadata before it is added to the collection, so all of the
    # properties can be sorted based on their dependencies. The tuples
    # are in the form (collection_name, property_name, property_json)
    # raw_properties = []

    # Iterate through all the key-value pairs in the graph to set up the
    # corresponding collections as empty metadata that will later be filled
    # with properties.
    for collection_name in graph_json:
        if collection_name == graph_name:
            raise Exception(
                f"Cannot have collection named '{collection_name}' share the same name as the graph containing it."
            )
        collection_json = graph_json[collection_name]
        if "type" not in collection_json:
            raise Exception("Collection metadata missing required property 'type'.")
        if "properties" not in collection_json:
            raise Exception(
                "Collection metadata missing required property 'properties'."
            )
        match collection_json["type"]:
            case "simple_table":
                collections[collection_name] = SimpleTableMetadata(collection_name)
            case collection_type:
                raise Exception(f"Unrecognized collection type: '{collection_type}'")

    for collection_name in graph_json:
        collection = collections[collection_name]
        collection.parse_from_json(graph_json[collection_name], collections)
    return GraphMetadata(graph_name, collections)
