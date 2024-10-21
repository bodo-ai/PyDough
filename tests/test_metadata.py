"""
TODO: add file-level docstring
"""

import pytest
from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.graphs import GraphMetadata
from pydough.metadata.collections import CollectionMetadata, SimpleTableMetadata
from pydough.metadata.properties import (
    PropertyMetadata,
    TableColumnMetadata,
    SimpleJoinMetadata,
    CompoundRelationshipMetadata,
)
from pydough.metadata.properties.subcollection_relationship_metadata import (
    SubcollectionRelationshipMetadata,
)
from typing import List, Dict, Set
from collections import defaultdict
from pydough.types import StringType, Int8Type, Int64Type, DateType, DecimalType


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
def test_get_collection_names(graph_name, answer, get_sample_graph):
    """
    Testing that the get_collection_names method of GraphMetadata correctly
    fetches the names of all collections in the metadata for a graph.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
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
                "purchasers",
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
                "lines_sourced_from",
            ],
            id="tpch-regions",
        ),
    ],
)
def test_get_property_names(graph_name, collection_name, answer, get_sample_graph):
    """
    Testing that the get_property_names method of CollectionMetadata correctly
    fetches the names of all properties in the metadata for a collection.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection: CollectionMetadata = graph.get_collection(collection_name)
    property_names: List[str] = collection.get_property_names()
    assert sorted(property_names) == sorted(answer)


def test_get_sample_graph_nouns(
    sample_graph_names, get_sample_graph, get_sample_graph_nouns
):
    """
    Testing that the get_nouns method of CollectionMetadata correctly
    identifies each noun in the graph and all of its meanings.
    """
    graph: GraphMetadata = get_sample_graph(sample_graph_names)
    nouns: Dict[str, List[AbstractMetadata]] = graph.get_nouns()
    processed_nouns: Dict[str, Set[str]] = {}
    for noun_name, noun_values in nouns.items():
        processed_values: Set[str] = set()
        for noun_value in noun_values:
            processed_values.add(noun_value.path)
        processed_nouns[noun_name] = processed_values
    raw_answer: Dict[str, List[str]] = get_sample_graph_nouns(sample_graph_names)
    processed_answer: Dict[str, List[AbstractMetadata]] = {
        key: set(val) for key, val in raw_answer.items()
    }
    assert processed_nouns == processed_answer


@pytest.mark.parametrize(
    "graph_name, collection_name, table_path, unique_properties",
    [
        pytest.param(
            "amazon",
            "Customers",
            "amazon.CUSTOMER",
            ["username", "email", "phone_number"],
            id="amazon-customer",
        ),
        pytest.param(
            "amazon",
            "Packages",
            "amazon.PACKAGE",
            ["id"],
            id="amazon-customer",
        ),
        pytest.param(
            "tpch",
            "Regions",
            "tpch.REGION",
            ["key"],
            id="tpch-region",
        ),
        pytest.param(
            "tpch",
            "PartSupp",
            "tpch.PARTSUPP",
            [["partkey", "suppkey"]],
            id="tpch-partsupp",
        ),
        pytest.param(
            "tpch",
            "Lineitems",
            "tpch.LINEITEM",
            [["partkey", "suppkey", "orderkey"]],
            id="tpch-lineitem",
        ),
    ],
)
def test_simple_table_info(
    graph_name, collection_name, table_path, unique_properties, get_sample_graph
):
    """
    Testing that the table path and unique properties fields of simple table
    collections are set correctly.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection: CollectionMetadata = graph.get_collection(collection_name)
    assert isinstance(collection, SimpleTableMetadata)
    assert collection.table_path == table_path
    assert collection.unique_properties == unique_properties


@pytest.mark.parametrize(
    "graph_name, collection_name, property_name, column_name, data_type",
    [
        pytest.param(
            "tpch",
            "Regions",
            "name",
            "r_name",
            StringType(),
            id="tpch-region-name",
        ),
        pytest.param(
            "tpch",
            "Customers",
            "acctbal",
            "c_acctbal",
            DecimalType(12, 2),
            id="tpch-customer-acctbal",
        ),
        pytest.param(
            "tpch",
            "Orders",
            "order_date",
            "o_orderdate",
            DateType(),
            id="tpch-lineitem-orderdate",
        ),
        pytest.param(
            "tpch",
            "Lineitems",
            "line_number",
            "l_linenumber",
            Int8Type(),
            id="tpch-lineitem-linenumber",
        ),
        pytest.param(
            "tpch",
            "Suppliers",
            "key",
            "s_suppkey",
            Int64Type(),
            id="tpch-supplier-suppkey",
        ),
    ],
)
def test_table_column_info(
    graph_name, collection_name, property_name, column_name, data_type, get_sample_graph
):
    """
    Testing that the type and column name fields of properties are set
    correctly.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection: CollectionMetadata = graph.get_collection(collection_name)
    property: PropertyMetadata = collection.get_property(property_name)
    assert isinstance(property, TableColumnMetadata)
    assert property.column_name == column_name
    assert property.data_type == data_type


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
            "tpch",
            "Regions",
            "nations",
            "Nations",
            "region",
            False,
            True,
            {"key": ["region_key"]},
            id="tpch-region-nations",
        ),
        pytest.param(
            "tpch",
            "PartSupp",
            "part",
            "Parts",
            "supply_records",
            True,
            False,
            {"part_key": ["key"]},
            id="tpch-partsupp-part",
        ),
    ],
)
def test_simple_join_info(
    graph_name,
    collection_name,
    property_name,
    other_collection,
    reverse_name,
    singular,
    no_collisions,
    keys,
    get_sample_graph,
):
    """
    Testing that the fields of simple join properties are set correctly.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection: CollectionMetadata = graph.get_collection(collection_name)
    property: PropertyMetadata = collection.get_property(property_name)

    # Verify that the properties of the join property match the passed in values
    assert isinstance(property, SimpleJoinMetadata)
    assert property.is_reversible
    assert property.is_subcollection
    assert property.other_collection.name == other_collection
    assert property.reverse_name == reverse_name
    assert property.is_plural != singular
    assert property.keys == keys

    # Verify that the properties of its reverse match in a corresponding manner
    reverse: PropertyMetadata = property.reverse_property
    assert isinstance(reverse, SimpleJoinMetadata)
    assert reverse.is_reversible
    assert reverse.is_subcollection
    assert reverse.reverse_property is property
    assert reverse.collection.name == property.other_collection.name
    assert reverse.name == property.reverse_name
    assert reverse.other_collection.name == collection.name
    assert reverse.reverse_name == property_name
    assert reverse.is_plural != no_collisions
    reverse_keys: defaultdict[list] = defaultdict(list)
    for key_name, other_names in property.keys.items():
        for other_name in other_names:
            reverse_keys[other_name].append(key_name)
    assert reverse.keys == reverse_keys


@pytest.mark.parametrize(
    [
        "graph_name",
        "collection_name",
        "property_name",
        "primary_property_name",
        "secondary_property_name",
        "other_collection",
        "reverse_name",
        "singular",
        "no_collisions",
        "inherited_properties",
        "reverse_inherited_properties",
    ],
    [
        pytest.param(
            "tpch",
            "Parts",
            "lines",
            "supply_records",
            "lines",
            "Lineitems",
            "part",
            False,
            True,
            {
                "ps_supplier": "TPCH.PartSupp.supplier",
                "ps_availqty": "TPCH.PartSupp.availqty",
                "ps_supplycost": "TPCH.PartSupp.supplycost",
                "ps_comment": "TPCH.PartSupp.comment",
            },
            {
                "ps_supplier": "TPCH.PartSupp.supplier",
                "ps_availqty": "TPCH.PartSupp.availqty",
                "ps_supplycost": "TPCH.PartSupp.supplycost",
                "ps_comment": "TPCH.PartSupp.comment",
            },
            id="tpch-part-lines",
        ),
        pytest.param(
            "tpch",
            "Suppliers",
            "parts_supplied",
            "supply_records",
            "part",
            "Parts",
            "suppliers_of_part",
            False,
            False,
            {
                "ps_lines": "TPCH.PartSupp.lines",
                "ps_availqty": "TPCH.PartSupp.availqty",
                "ps_supplycost": "TPCH.PartSupp.supplycost",
                "ps_comment": "TPCH.PartSupp.comment",
            },
            {
                "ps_lines": "TPCH.PartSupp.lines",
                "ps_availqty": "TPCH.PartSupp.availqty",
                "ps_supplycost": "TPCH.PartSupp.supplycost",
                "ps_comment": "TPCH.PartSupp.comment",
            },
            id="tpch-supplier-part",
        ),
        pytest.param(
            "tpch",
            "Regions",
            "customers",
            "nations",
            "customers",
            "Customers",
            "region",
            False,
            True,
            {"nation_name": "TPCH.Nations.name"},
            {"nation_name": "TPCH.Nations.name"},
            id="tpch-region-customers",
        ),
        pytest.param(
            "tpch",
            "Regions",
            "orders_shipped_to",
            "customers",
            "orders",
            "Orders",
            "shipping_region",
            False,
            True,
            {"nation_name": "TPCH.Regions.customers.nation_name"},
            {"nation_name": "TPCH.Customers.region.nation_name"},
            id="tpch-region-orders",
        ),
    ],
)
def test_compound_relationship_info(
    graph_name,
    collection_name,
    property_name,
    primary_property_name,
    secondary_property_name,
    other_collection,
    reverse_name,
    singular,
    no_collisions,
    inherited_properties,
    reverse_inherited_properties,
    get_sample_graph,
):
    """
    Testing that the fields of compound relationships are set correctly.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection: CollectionMetadata = graph.get_collection(collection_name)
    property: PropertyMetadata = collection.get_property(property_name)

    # Verify that the properties of the compound property match the passed in values
    assert isinstance(property, CompoundRelationshipMetadata)
    for p in [property, property.primary_property, property.secondary_property]:
        assert isinstance(p, SubcollectionRelationshipMetadata)
        assert property.is_reversible
        assert property.is_subcollection
    assert property.primary_property.name == primary_property_name
    assert (
        property.primary_property.other_collection
        is property.secondary_property.collection
    )
    assert property.secondary_property.name == secondary_property_name
    assert property.other_collection.name == other_collection
    assert property.other_collection is property.secondary_property.other_collection
    assert property.reverse_name == reverse_name
    assert property.is_plural != singular
    inherited_dict: Dict[str, str] = {
        alias: inh.property_to_inherit.path
        for alias, inh in property.inherited_properties.items()
    }
    assert inherited_dict == inherited_properties

    # Verify that the properties of its reverse match in a corresponding manner
    reverse: PropertyMetadata = property.reverse_property
    assert isinstance(reverse, CompoundRelationshipMetadata)
    for p in [reverse, reverse.primary_property, reverse.secondary_property]:
        assert isinstance(p, SubcollectionRelationshipMetadata)
        assert property.is_reversible
        assert property.is_subcollection
    assert reverse.primary_property.name == property.secondary_property.reverse_name
    assert (
        reverse.primary_property.other_collection
        is property.secondary_property.collection
    )
    assert reverse.secondary_property.name == property.primary_property.reverse_name
    assert reverse.other_collection is property.collection
    assert reverse.reverse_name == property_name
    assert reverse.is_plural != no_collisions
    reverse_inherited_dict: Dict[str, str] = {
        alias: inh.property_to_inherit.path
        for alias, inh in reverse.inherited_properties.items()
    }
    assert reverse_inherited_dict == reverse_inherited_properties
