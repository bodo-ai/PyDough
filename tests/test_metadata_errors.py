"""
TODO: add file-level docstring
"""

import pytest
from pydough.metadata.errors import PyDoughMetadataException
from pydough.metadata.graphs import GraphMetadata
from pydough.metadata.collections import CollectionMetadata


def test_missing_collection(get_sample_graph):
    """
    Testing the error handling for trying to fetch a collection that does not
    exist.
    """
    graph: GraphMetadata = get_sample_graph("tpch")
    with pytest.raises(
        PyDoughMetadataException,
        match="graph 'TPCH' does not have a collection named 'Inventory'",
    ):
        graph.get_collection("Inventory")


def test_missing_property(get_sample_graph):
    """
    Testing the error handling for trying to fetch a collection that does not
    exist.
    """
    graph: GraphMetadata = get_sample_graph("tpch")
    with pytest.raises(
        PyDoughMetadataException,
        match="simple table collection 'Parts' in graph 'TPCH' does not have a property 'color'",
    ):
        collection: CollectionMetadata = graph.get_collection("Parts")
        collection.get_property("color")
