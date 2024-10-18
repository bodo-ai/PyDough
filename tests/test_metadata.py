"""
TODO: add file-level docstring
"""

import pytest
from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.graphs import GraphMetadata
from pydough.metadata.collections import CollectionMetadata
from typing import List, Dict, Set


def test_graph_structure(sample_graphs):
    """
    Testing that the sample graphs, when parsed, each produce correctly formatted
    GraphMetadata objects.
    """
    assert isinstance(sample_graphs, GraphMetadata)


@pytest.mark.parametrize(
    "graph_name, answer",
    [
        pytest.param(
            "amazon",
            [
                "Addresses",
                "Customers",
                "Occupancies",
                "PackageContents",
                "Packages",
                "Products",
            ],
            id="amazon",
        ),
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
    Testing that the get_collection_names method of GraphMetadata correctly
    fetches the names of all collections in the metadata for a graph.
    """
    graph: GraphMetadata = get_graph(graph_name)
    collection_names = graph.get_collection_names()
    assert sorted(collection_names) == sorted(answer)


@pytest.mark.parametrize(
    "graph_name, collection_name, answer",
    [
        pytest.param(
            "amazon",
            "Addresses",
            [
                "id",
                "street_name",
                "street_number",
                "apartment",
                "zip_code",
                "city",
                "state",
                "occupancies",
                "occupants",
                "packages_shipped_to",
                "packages_billed_to",
            ],
            id="amazon-addresses",
        ),
        pytest.param(
            "amazon",
            "Products",
            [
                "name",
                "product_type",
                "product_category",
                "price_per_unit",
                "containment_records",
                "packages_containing",
            ],
            id="amazon-products",
        ),
        pytest.param(
            "tpch",
            "Regions",
            [
                "key",
                "name",
                "comment",
                "nations",
                "customers",
                "suppliers",
                "orders_shipped_to",
            ],
            id="tpch-regions",
        ),
    ],
)
def test_get_property_names(graph_name, collection_name, answer, get_graph):
    """
    Testing that the get_property_names method of CollectionMetadata correctly
    fetches the names of all properties in the metadata for a collection.
    """
    graph: GraphMetadata = get_graph(graph_name)
    collection: CollectionMetadata = graph.get_collection(collection_name)
    property_names: List[str] = collection.get_property_names()
    assert sorted(property_names) == sorted(answer)


@pytest.mark.parametrize(
    "graph_name, answer",
    [
        pytest.param(
            "tpch",
            {},
            id="tpch",
        ),
        pytest.param(
            "empty",
            {"empty": {"empty"}},
            id="empty",
        ),
    ],
)
def test_get_graph_nouns(graph_name, answer, get_graph):
    """
    Testing that the get_nouns method of CollectionMetadata correctly
    identifies each noun in the graph and all of its meanings.
    """
    graph: GraphMetadata = get_graph(graph_name)
    nouns: Dict[str, List[AbstractMetadata]] = graph.get_nouns()
    processed_nouns: Dict[str, Set[str]] = {}
    for noun_name, noun_values in nouns.items():
        processed_values: Set[str] = set()
        for noun_value in noun_values:
            processed_values.add(noun_value.path)
        processed_nouns[noun_name] = processed_values
    assert processed_nouns == answer
