"""
TODO: add file-level docstring
"""

import pytest
from pydough.metadata.graphs.graph_metadata import GraphMetadata


def test_graph_structure(sample_graphs):
    """
    Testing that the sample graphs, when parsed, each produce correctly formatted
    GraphMetadata objects.
    """
    assert isinstance(sample_graphs, GraphMetadata)


@pytest.mark.parametrize(
    "graph_name, answer",
    [
        pytest.param("amazon", [], id="amazon"),
        pytest.param(
            "tpch",
            [
                "Regions",
                "Nations",
                "Suppliers",
                "Parts",
                "PartSupp",
                "Lineitems",
                "Customers",
                "Orders",
            ],
            id="tpch",
        ),
        pytest.param("empty", [], id="empty"),
    ],
)
def test_get_collection_names(graph_name, answer, get_graph):
    """
    Testing that the get_collection_names method of GraphMetadata correctly fetches
    the names of all collections in the metadata for a graph.
    """
    graph: GraphMetadata = get_graph(graph_name)
    collection_names = graph.get_collection_names()
    assert sorted(collection_names) == sorted(answer)
