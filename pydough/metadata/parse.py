from typing import Dict
from pydough.metadata.graph import GraphMetadata, CollectionMetadata, PropertyMetadata
import json

def parse_metadata(file_path : str, graph_name : str) -> GraphMetadata:
    """
    TODO: add function doscstring.
    """
    with open(file_path, "r") as f:
        as_json = json.load(f)
    if graph_name not in as_json:
        raise Exception(f"PyDough metadata does not contain a graph named '{graph_name}'")
    return parse_graph(graph_name, as_json[graph_name])

def parse_graph(graph_name : str, graph_json : Dict) -> GraphMetadata:
    """
    TODO: add function doscstring.
    """
    collections = []
    for collection_name in graph_json:
        if collection_name == graph_name:
            raise Exception(f"Cannot have collection named '{collection_name}' share the same name as the graph containing it.")
        collections.append(parse_collection(collection_name, graph_json[collection_name]))
    return GraphMetadata(graph_name, collections)

def parse_collection(collection_name : str, collection_json : Dict) -> CollectionMetadata:
    """
    TODO: add function doscstring & finish the implementation.
    """
    collections = []
    return CollectionMetadata(collection_name)