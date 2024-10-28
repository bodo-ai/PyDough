"""
TODO: add file-level docstring
"""

__all__ = ["graph_fetcher", "noun_fetcher", "map_over_dict_values", "get_table_column"]

from pydough.metadata import (
    GraphMetadata,
    CollectionMetadata,
    PropertyMetadata,
    TableColumnMetadata,
    PyDoughMetadataException,
)
from typing import Dict, Set, Callable, Any

# Type alias for a function that takes in a string and generates metadata
# for a graph based on it.
graph_fetcher = Callable[[str], GraphMetadata]

# Type alias for a function that takes in a string and generates the
# representation of all the nouns in a metadata graphs based on it.
noun_fetcher = Callable[[str], Dict[str, Set[str]]]


def map_over_dict_values(dictionary: dict, func: Callable[[Any], Any]) -> dict:
    """
    Applies a lambda function to the values of a dictionary, returning a
    new dictionary with the transformation applied.

    Args:
        `dictionary`: The input dictionary whose values are to be transformed.
        `func`: The lambda to call that transforms each value in `dictionary`.

    Returns:
        The transformed dictionary, with the same keys as `dictionary`.
    """
    return {key: func(val) for key, val in dictionary.items()}


def get_table_column(
    get_sample_graph: graph_fetcher,
    graph_name: str,
    collection_name: str,
    property_name: str,
) -> TableColumnMetadata:
    """
    Fetches a table column property from one of the sample graphs.

    Args:
        `get_sample_graph`: the function used to fetch the graph.
        `graph_name`: the name of the graph to fetch.
        `collection_name`: the name of the desired collection from the graph.
        `property_name`: the name of the desired property

    Returns:
        The desired table column property.

    Raises:
        `PyDoughMetadataException` if the desired property is not a table
        column property or does not exist.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection: CollectionMetadata = graph.get_collection(collection_name)
    property: PropertyMetadata = collection.get_property(property_name)
    if not isinstance(property, TableColumnMetadata):
        raise PyDoughMetadataException(
            f"Expected {property.error_name} to be a table column property"
        )
    return property
