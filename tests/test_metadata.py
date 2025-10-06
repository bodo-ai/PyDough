"""
Unit tests for the PyDough metadata module.
"""

import pytest

from pydough.metadata import (
    CollectionMetadata,
    GraphMetadata,
    PropertyMetadata,
    SimpleJoinMetadata,
    SimpleTableMetadata,
    TableColumnMetadata,
)
from pydough.types import (
    DatetimeType,
    NumericType,
    PyDoughType,
    StringType,
)
from tests.testing_utilities import graph_fetcher


def test_graph_structure(sample_graphs: GraphMetadata) -> None:
    """
    Testing that the sample graphs, when parsed, each produce correctly formatted
    GraphMetadata objects.
    """
    assert isinstance(sample_graphs, GraphMetadata), (
        "Expected to be metadata for a PyDough graph"
    )


@pytest.mark.parametrize(
    "graph_name, answer",
    [
        pytest.param(
            "TPCH",
            [
                "regions",
                "nations",
                "suppliers",
                "parts",
                "supply_records",
                "lines",
                "customers",
                "orders",
            ],
            id="TPCH",
        ),
        pytest.param("Empty", [], id="Empty"),
    ],
)
def test_get_collection_names(
    graph_name: str, answer, get_sample_graph: graph_fetcher
) -> None:
    """
    Testing that the get_collection_names method of GraphMetadata correctly
    fetches the names of all collections in the metadata for a graph.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection_names: list[str] = graph.get_collection_names()
    assert sorted(collection_names) == sorted(answer), (
        f"Mismatch between names of collections in {graph!r} versus expected values"
    )


@pytest.mark.parametrize(
    "graph_name, collection_name, answer",
    [
        pytest.param(
            "TPCH",
            "regions",
            [
                "key",
                "name",
                "comment",
                "nations",
            ],
            id="tpch-regions",
        ),
    ],
)
def test_get_property_names(
    graph_name: str,
    collection_name: str,
    answer: list[str],
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Testing that the get_property_names method of CollectionMetadata correctly
    fetches the names of all properties in the metadata for a collection.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection = graph.get_collection(collection_name)
    assert isinstance(collection, CollectionMetadata)
    property_names: list[str] = collection.get_property_names()
    assert sorted(property_names) == sorted(answer), (
        f"Mismatch between names of properties in {collection!r} versus expected values"
    )


@pytest.mark.parametrize(
    "graph_name, collection_name, table_path, unique_properties",
    [
        pytest.param(
            "TPCH",
            "regions",
            "tpch.REGION",
            ["key", "name"],
            id="tpch-region",
        ),
        pytest.param(
            "TPCH",
            "supply_records",
            "tpch.PARTSUPP",
            [["part_key", "supplier_key"]],
            id="tpch-partsupp",
        ),
        pytest.param(
            "TPCH",
            "lines",
            "tpch.LINEITEM",
            [["order_key", "line_number"]],
            id="tpch-lineitem",
        ),
    ],
)
def test_simple_table_info(
    graph_name: str,
    collection_name: str,
    table_path: str,
    unique_properties: list[str | list[str]],
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Testing that the table path and unique properties fields of simple table
    collections are set correctly.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection = graph.get_collection(collection_name)
    assert isinstance(collection, CollectionMetadata)
    assert isinstance(collection, SimpleTableMetadata), (
        "Expected 'collection' to be metadata for a simple table"
    )
    assert collection.table_path == table_path, (
        f"Mismatch between 'table_path' of {collection!r} and expected value"
    )
    assert collection.unique_properties == unique_properties, (
        f"Mismatch between 'unique_properties' of {collection!r} and expected value"
    )


@pytest.mark.parametrize(
    "graph_name, collection_name, property_name, column_name, data_type",
    [
        pytest.param(
            "TPCH",
            "regions",
            "name",
            "r_name",
            StringType(),
            id="tpch-region-name",
        ),
        pytest.param(
            "TPCH",
            "customers",
            "account_balance",
            "c_acctbal",
            NumericType(),
            id="tpch-customer-account_balance",
        ),
        pytest.param(
            "TPCH",
            "orders",
            "order_date",
            "o_orderdate",
            DatetimeType(),
            id="tpch-lineitem-orderdate",
        ),
        pytest.param(
            "TPCH",
            "lines",
            "line_number",
            "l_linenumber",
            NumericType(),
            id="tpch-lineitem-linenumber",
        ),
        pytest.param(
            "TPCH",
            "suppliers",
            "key",
            "s_suppkey",
            NumericType(),
            id="tpch-supplier-suppkey",
        ),
    ],
)
def test_table_column_info(
    graph_name: str,
    collection_name: str,
    property_name: str,
    column_name: str,
    data_type: PyDoughType,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Testing that the type and column name fields of properties are set
    correctly.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection = graph.get_collection(collection_name)
    assert isinstance(collection, CollectionMetadata)
    property = collection.get_property(property_name)
    assert isinstance(property, PropertyMetadata)
    assert isinstance(property, TableColumnMetadata), (
        "Expected 'property' to be metadata for a table column"
    )
    assert property.column_name == column_name, (
        f"Mismatch between 'table_path' of {property!r} and expected value"
    )
    assert property.data_type == data_type, (
        f"Mismatch between 'data_type' of {property!r} and expected value"
    )


@pytest.mark.parametrize(
    [
        "graph_name",
        "collection_name",
        "property_name",
        "other_collection",
        "reverse_name",
        "singular",
        "no_collisions",
        "keys",
    ],
    [
        pytest.param(
            "TPCH",
            "regions",
            "nations",
            "nations",
            "region",
            False,
            True,
            {"key": ["region_key"]},
            id="tpch-region-nations",
        ),
        pytest.param(
            "TPCH",
            "supply_records",
            "part",
            "parts",
            "supply_records",
            True,
            False,
            {"part_key": ["key"]},
            id="tpch-partsupp-part",
        ),
    ],
)
def test_simple_join_info(
    graph_name: str,
    collection_name: str,
    property_name: str,
    other_collection: str,
    reverse_name: str,
    singular: bool,
    no_collisions: bool,
    keys: dict[str, list[str]],
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Testing that the fields of simple join properties are set correctly.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection = graph.get_collection(collection_name)
    assert isinstance(collection, CollectionMetadata)
    property = collection.get_property(property_name)
    assert isinstance(property, PropertyMetadata)

    # Verify that the properties of the join property match the passed in values
    assert isinstance(property, SimpleJoinMetadata), (
        "Expected 'property' to be metadata for a simple join"
    )
    assert property.is_reversible, (
        f"Mismatch between 'is_reversible' of {property!r} and expected value"
    )
    assert property.is_subcollection, (
        f"Mismatch between 'is_subcollection' of {property!r} and expected value"
    )
    assert property.is_plural != singular, (
        f"Mismatch between 'is_plural' of {property!r} and expected value"
    )
    assert property.keys == keys, (
        f"Mismatch between 'keys' of {property!r} and expected value"
    )
