"""
Unit tests for the PyDough metadata module.
"""

from typing import Any

import pytest

from pydough import parse_metadata_from_list
from pydough.errors import PyDoughMetadataException
from pydough.metadata import (
    CollectionMetadata,
    GraphMetadata,
    PropertyMetadata,
    ScalarAttributeMetadata,
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


def test_semantic_info(get_sample_graph: graph_fetcher) -> None:
    """
    Testing that the semantic fields of the metadata are set correctly.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")

    # Verify the semantic info fields of the overall grapah
    assert graph.verified_pydough_analysis == [
        {
            "question": "How many customers are in China?",
            "code": "TPCH.CALCULATE(n_chinese_customers=COUNT(customers.WHERE(nation.name == 'CHINA')))",
        },
        {
            "question": "What was the most ordered part in 1995, by quantity, by Brazilian customers?",
            "code": "parts.CALCULATE(name, quantity=SUM(lines.WHERE((YEAR(ship_date) == 1995) & (order.customer.nation.name == 'BRAZIL')).quantity)).TOP_K(1, by=quantity)",
        },
        {
            "question": "Who is the wealthiest customer in each nation in Africa?",
            "code": "nations.WHERE(region.name == 'AFRICA').CALCULATE(nation_name=name, richest_customer=customers.BEST(per='nation', by=account_balance.DESC()).name)",
        },
    ]

    assert graph.additional_definitions == [
        "Revenue for a lineitem is the extended_price * (1 - discount) * (1 - tax) minus quantity * supply_cost from the corresponding supply record",
        "A domestic shipment is a lineitem where the customer and supplier are from the same nation",
        "Frequent buyers are customers that have placed more than 5 orders in a single year for at least two different years",
    ]

    assert graph.extra_semantic_info == {
        "data source": "TPC-H Benchmark Dataset",
        "data generation tool": "TPC-H dbgen tool",
        "dataset download link": "https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H.db",
        "schema diagram link": "https://docs.snowflake.com/en/user-guide/sample-data-tpch",
        "dataset specification link": "https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf",
        "data scale factor": 1,
        "intended use": "Simulating decision support systems for complex ad-hoc queries and concurrent data modifications",
        "notable characteristics": "Highly normalized schema with multiple tables and relationships, designed to represent a wholesale supplier's business environment",
        "data description": "Contains information about orders. Every order has one or more lineitems, each representing the purchase and shipment of a specific part from a specific supplier. Each order is placed by a customer, and both customers and suppliers belong to nations which in turn belong to regions. Additionally, there are supply records indicating every combination of a supplier and the parts they supply.",
    }

    # Verify the semantic info fields for a collection (parts)
    collection = graph.get_collection("parts")
    assert isinstance(collection, CollectionMetadata)
    assert (
        collection.description
        == "The various products supplied by various companies in shipments to different customers"
    )
    assert collection.synonyms == [
        "products",
        "components",
        "items",
        "goods",
    ]
    assert collection.extra_semantic_info == {
        "nrows": 200000,
        "distinct values": {
            "key": 200000,
            "name": 200000,
            "manufacturer": 5,
            "brand": 25,
            "part_type": 150,
            "size": 50,
            "container": 40,
            "retail_price": 20899,
            "comment": 131753,
        },
        "correlations": {
            "brand": "each brand is associated with exactly one manufacturer, and each manufacturer has exactly 5 distinct brands"
        },
    }

    # Verify the semantic info fields for a scalar property (part.size)
    scalar_property = collection.get_property("size")
    assert isinstance(scalar_property, ScalarAttributeMetadata)
    assert scalar_property.sample_values == [1, 10, 31, 46, 50]
    assert scalar_property.description == "The size of the part"
    assert scalar_property.synonyms == [
        "dimension",
        "measurement",
        "length",
        "width",
        "height",
        "volume",
    ]
    assert scalar_property.extra_semantic_info == {
        "minimum value": 1,
        "maximum value": 50,
        "is dense": True,
        "distinct values": 50,
        "correlated fields": [],
    }

    # Test the semantic information for a relationship property (part.lines)
    join_property = collection.get_property("lines")
    assert isinstance(join_property, SimpleJoinMetadata)
    assert join_property.description == ("The line items for shipments of the part")
    assert join_property.synonyms == [
        "shipments",
        "packages",
        "purchases",
        "deliveries",
        "sales",
    ]
    assert join_property.extra_semantic_info == {
        "unmatched rows": 0,
        "min matches per row": 9,
        "max matches per row": 57,
        "avg matches per row": 30.01,
        "classification": "one-to-many",
    }

    # Test the semantic information for a property that does not have some
    # semantic information defined for it (part.supply_records)
    empty_semantic_property = collection.get_property("supply_records")
    assert isinstance(empty_semantic_property, SimpleJoinMetadata)
    assert (
        empty_semantic_property.description
        == "The records indicating which companies supply the part"
    )
    assert empty_semantic_property.synonyms == [
        "producers",
        "vendors",
        "suppliers of part",
    ]
    assert empty_semantic_property.extra_semantic_info is None


@pytest.mark.parametrize(
    "file_name, graph_name, valid",
    [
        pytest.param(
            "sample_graphs.json",
            "TPCH",
            True,
            id="parse-from-list-tpch-graph",
        ),
        pytest.param(
            "keywords_graph.json",
            "keywords",
            True,
            id="parse-from-list-keywords-graph",
        ),
        pytest.param(
            "masked_graphs.json",
            "CRYPTBANK",
            True,
            id="parse-from-list-cryptobank-graph",
        ),
        pytest.param(
            "sf_masked_examples.json",
            "HEALTH",
            True,
            id="parse-from-list-health-graph",
        ),
        pytest.param(
            "sf_masked_examples.json",
            "INVALID_GRAPH_NAME",
            False,
            id="parse-from-list-INVALID_GRAPH_NAME",
        ),
    ],
)
def test_parse_from_list(
    file_name: str,
    graph_name: str,
    valid: bool,
    get_custom_datasets_graph_list: Any,
) -> None:
    """
    Tests that parse_metadata_from_list successfully extracts a valid graph
    from a properly formatted list of metadata dictionaries.

    Verifies:
    - The function returns a GraphMetadata object
    - The returned graph has the correct name
    """
    json_list = get_custom_datasets_graph_list(file_name)
    if valid:
        graph: GraphMetadata = parse_metadata_from_list(json_list, graph_name)
        assert graph.name == graph_name
    else:
        with pytest.raises(
            PyDoughMetadataException,
            match=f"PyDough metadata graph '{graph_name}' not found in list",
        ):
            parse_metadata_from_list(json_list, graph_name)
