"""
TODO: add file-level docstring
"""

from collections import defaultdict
from collections.abc import MutableMapping, MutableSequence

import pytest
from test_utils import graph_fetcher, map_over_dict_values, noun_fetcher

from pydough.metadata import (
    CollectionMetadata,
    CompoundRelationshipMetadata,
    GraphMetadata,
    PropertyMetadata,
    SimpleJoinMetadata,
    SimpleTableMetadata,
    TableColumnMetadata,
)
from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.properties import (
    InheritedPropertyMetadata,
    SubcollectionRelationshipMetadata,
)
from pydough.types import (
    DateType,
    DecimalType,
    Int8Type,
    Int64Type,
    PyDoughType,
    StringType,
)


def test_graph_structure(sample_graphs: GraphMetadata):
    """
    Testing that the sample graphs, when parsed, each produce correctly formatted
    GraphMetadata objects.
    """
    assert isinstance(
        sample_graphs, GraphMetadata
    ), "Expected to be metadata for a PyDough graph"


@pytest.mark.parametrize(
    "graph_name, answer",
    [
        pytest.param(
            "Amazon",
            [
                "Addresses",
                "Customers",
                "Occupancies",
                "PackageContents",
                "Packages",
                "Products",
            ],
            id="Amazon",
        ),
        pytest.param(
            "TPCH",
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
            id="TPCH",
        ),
        pytest.param("Empty", [], id="Empty"),
    ],
)
def test_get_collection_names(graph_name: str, answer, get_sample_graph: graph_fetcher):
    """
    Testing that the get_collection_names method of GraphMetadata correctly
    fetches the names of all collections in the metadata for a graph.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection_names: MutableSequence[str] = graph.get_collection_names()
    assert sorted(collection_names) == sorted(
        answer
    ), f"Mismatch between names of collections in {graph!r} versus expected values"


@pytest.mark.parametrize(
    "graph_name, collection_name, answer",
    [
        pytest.param(
            "Amazon",
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
            "Amazon",
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
            "TPCH",
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
def test_get_property_names(
    graph_name: str,
    collection_name: str,
    answer: MutableSequence[str],
    get_sample_graph: graph_fetcher,
):
    """
    Testing that the get_property_names method of CollectionMetadata correctly
    fetches the names of all properties in the metadata for a collection.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection = graph.get_collection(collection_name)
    assert isinstance(collection, CollectionMetadata)
    property_names: MutableSequence[str] = collection.get_property_names()
    assert sorted(property_names) == sorted(
        answer
    ), f"Mismatch between names of properties in {collection!r} versus expected values"


def test_get_sample_graph_nouns(
    sample_graph_names: str,
    get_sample_graph: graph_fetcher,
    get_sample_graph_nouns: noun_fetcher,
):
    """
    Testing that the get_nouns method of CollectionMetadata correctly
    identifies each noun in the graph and all of its meanings.
    """
    graph: GraphMetadata = get_sample_graph(sample_graph_names)
    nouns: MutableMapping[str, MutableSequence[AbstractMetadata]] = graph.get_nouns()
    # Transform the nouns from metadata objects into path strings
    processed_nouns: MutableMapping[str, set[str]] = map_over_dict_values(
        nouns, lambda noun_values: {noun.path for noun in noun_values}
    )
    answer: MutableMapping[str, set[str]] = get_sample_graph_nouns(sample_graph_names)
    assert (
        processed_nouns == answer
    ), f"Mismatch between names of nouns in {graph!r} versus expected values"


@pytest.mark.parametrize(
    "graph_name, collection_name, table_path, unique_properties",
    [
        pytest.param(
            "Amazon",
            "Customers",
            "amazon.CUSTOMER",
            ["username", "email", "phone_number"],
            id="amazon-customer",
        ),
        pytest.param(
            "Amazon",
            "Packages",
            "amazon.PACKAGE",
            ["id"],
            id="amazon-customer",
        ),
        pytest.param(
            "TPCH",
            "Regions",
            "tpch.REGION",
            ["key"],
            id="tpch-region",
        ),
        pytest.param(
            "TPCH",
            "PartSupp",
            "tpch.PARTSUPP",
            [["part_key", "supplier_key"]],
            id="tpch-partsupp",
        ),
        pytest.param(
            "TPCH",
            "Lineitems",
            "tpch.LINEITEM",
            [["part_key", "supplier_key", "order_key"]],
            id="tpch-lineitem",
        ),
    ],
)
def test_simple_table_info(
    graph_name: str,
    collection_name: str,
    table_path: str,
    unique_properties: MutableSequence[str | MutableSequence[str]],
    get_sample_graph: graph_fetcher,
):
    """
    Testing that the table path and unique properties fields of simple table
    collections are set correctly.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection = graph.get_collection(collection_name)
    assert isinstance(collection, CollectionMetadata)
    assert isinstance(
        collection, SimpleTableMetadata
    ), "Expected 'collection' to be metadata for a simple table"
    assert (
        collection.table_path == table_path
    ), f"Mismatch between 'table_path' of {collection!r} and expected value"
    assert (
        collection.unique_properties == unique_properties
    ), f"Mismatch between 'unique_properties' of {collection!r} and expected value"


@pytest.mark.parametrize(
    "graph_name, collection_name, property_name, column_name, data_type",
    [
        pytest.param(
            "TPCH",
            "Regions",
            "name",
            "r_name",
            StringType(),
            id="tpch-region-name",
        ),
        pytest.param(
            "TPCH",
            "Customers",
            "acctbal",
            "c_acctbal",
            DecimalType(12, 2),
            id="tpch-customer-acctbal",
        ),
        pytest.param(
            "TPCH",
            "Orders",
            "order_date",
            "o_orderdate",
            DateType(),
            id="tpch-lineitem-orderdate",
        ),
        pytest.param(
            "TPCH",
            "Lineitems",
            "line_number",
            "l_linenumber",
            Int8Type(),
            id="tpch-lineitem-linenumber",
        ),
        pytest.param(
            "TPCH",
            "Suppliers",
            "key",
            "s_suppkey",
            Int64Type(),
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
):
    """
    Testing that the type and column name fields of properties are set
    correctly.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection = graph.get_collection(collection_name)
    assert isinstance(collection, CollectionMetadata)
    property = collection.get_property(property_name)
    assert isinstance(property, PropertyMetadata)
    assert isinstance(
        property, TableColumnMetadata
    ), "Expected 'property' to be metadata for a table column"
    assert (
        property.column_name == column_name
    ), f"Mismatch between 'table_path' of {property!r} and expected value"
    assert (
        property.data_type == data_type
    ), f"Mismatch between 'data_type' of {property!r} and expected value"


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
            "TPCH",
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
    graph_name: str,
    collection_name: str,
    property_name: str,
    other_collection: str,
    reverse_name: str,
    singular: bool,
    no_collisions: bool,
    keys: MutableMapping[str, MutableSequence[str]],
    get_sample_graph: graph_fetcher,
):
    """
    Testing that the fields of simple join properties are set correctly.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection = graph.get_collection(collection_name)
    assert isinstance(collection, CollectionMetadata)
    property = collection.get_property(property_name)
    assert isinstance(property, PropertyMetadata)

    # Verify that the properties of the join property match the passed in values
    assert isinstance(
        property, SimpleJoinMetadata
    ), "Expected 'property' to be metadata for a simple join"
    assert (
        property.is_reversible
    ), f"Mismatch between 'is_reversible' of {property!r} and expected value"
    assert (
        property.is_subcollection
    ), f"Mismatch between 'is_subcollection' of {property!r} and expected value"
    assert (
        property.other_collection.name == other_collection
    ), f"Mismatch between 'other_collection_name' of {property!r} and expected value"
    assert (
        property.reverse_name == reverse_name
    ), f"Mismatch between 'reverse_name' of {property!r} and expected value"
    assert (
        property.is_plural != singular
    ), f"Mismatch between 'is_plural' of {property!r} and expected value"
    assert (
        property.keys == keys
    ), f"Mismatch between 'keys' of {property!r} and expected value"

    # Verify that the properties of its reverse match in a corresponding manner
    reverse: PropertyMetadata = property.reverse_property
    assert isinstance(
        reverse, SimpleJoinMetadata
    ), "Expected 'reverse' to be metadata for a simple join"
    assert (
        reverse.is_reversible
    ), f"Mismatch between 'is_reversible' of {reverse!r} and expected value"
    assert (
        reverse.is_subcollection
    ), f"Mismatch between 'is_subcollection' of {reverse!r} and expected value"
    assert (
        reverse.reverse_property is property
    ), f"Mismatch between 'reverse_property' of {reverse!r} and expected value"
    assert (
        reverse.collection.name == property.other_collection.name
    ), f"Mismatch between 'collection' of {reverse!r} and expected value"
    assert (
        reverse.name == property.reverse_name
    ), f"Mismatch between 'name' of {reverse!r} and expected value"
    assert (
        reverse.other_collection.name == collection.name
    ), f"Mismatch between 'other_collection' of {reverse!r} and expected value"
    assert (
        reverse.reverse_name == property_name
    ), f"Mismatch between 'reverse_name' of {reverse!r} and expected value"
    assert (
        reverse.is_plural != no_collisions
    ), f"Mismatch between 'is_plural' of {reverse!r} and expected value"
    reverse_keys: defaultdict[str, list] = defaultdict(list)
    for key_name, other_names in property.keys.items():
        for other_name in other_names:
            reverse_keys[other_name].append(key_name)
    assert (
        reverse.keys == reverse_keys
    ), f"Mismatch between 'keys' of {reverse!r} and expected value"


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
            "TPCH",
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
            "TPCH",
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
            "TPCH",
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
            "TPCH",
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
    graph_name: str,
    collection_name: str,
    property_name: str,
    primary_property_name: str,
    secondary_property_name: str,
    other_collection: str,
    reverse_name: str,
    singular: bool,
    no_collisions: bool,
    inherited_properties: dict[str, str],
    reverse_inherited_properties: dict[str, str],
    get_sample_graph: graph_fetcher,
):
    """
    Testing that the fields of compound relationships are set correctly.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    collection = graph.get_collection(collection_name)
    assert isinstance(collection, CollectionMetadata)
    property = collection.get_property(property_name)
    assert isinstance(property, PropertyMetadata)

    # Verify that the properties of the compound property match the passed in values
    assert isinstance(
        property, CompoundRelationshipMetadata
    ), "Expected 'property' to be metadata for a compound relationship"
    for p in [property, property.primary_property, property.secondary_property]:
        assert isinstance(
            p, SubcollectionRelationshipMetadata
        ), f"Expected {p!r} to be metadata for a subcollection relationship"
        assert (
            property.is_reversible
        ), f"Mismatch between 'is_reversible' of {p!r} and expected value"
        assert (
            property.is_subcollection
        ), f"Mismatch between 'is_subcollection' of {p!r} and expected value"
    assert (
        property.primary_property.name == primary_property_name
    ), f"Mismatch between name of primary property of {property!r} and expected value"
    assert (
        property.primary_property.other_collection
        is property.secondary_property.collection
    ), f"Mismatch between target other collection of {property!r} and expected value"
    assert (
        property.secondary_property.name == secondary_property_name
    ), f"Mismatch between name of secondary property of {property!r} and expected value"
    assert (
        property.other_collection.name == other_collection
    ), f"Mismatch between name of target collection of secondary property of {property!r} and expected value"
    assert (
        property.other_collection is property.secondary_property.other_collection
    ), f"Mismatch between target of {property!r} and target of its secondary property"
    assert (
        property.reverse_name == reverse_name
    ), f"Mismatch between 'reverse_name of {property!r} and expected value"
    assert (
        property.is_plural != singular
    ), f"Mismatch between 'is_plural of {property!r} and expected value"
    inherited_dict: dict[str, str] = {}
    for alias, inh in property.inherited_properties.items():
        assert isinstance(inh, InheritedPropertyMetadata)
        inherited_dict[alias] = inh.property_to_inherit.path
    assert (
        inherited_dict == inherited_properties
    ), f"Mismatch between 'inherited_properties of {property!r} and expected value"

    # Verify that the properties of its reverse match in a corresponding manner
    reverse: PropertyMetadata = property.reverse_property
    assert isinstance(
        reverse, CompoundRelationshipMetadata
    ), "Expected 'property' to be metadata for a compound relationship"
    for p in [reverse, reverse.primary_property, reverse.secondary_property]:
        assert isinstance(
            p, SubcollectionRelationshipMetadata
        ), f"Expected {p!r} to be metadata for a subcollection relationship"
        assert (
            property.is_reversible
        ), f"Mismatch between 'is_reversible' of {p!r} and expected value"
        assert (
            property.is_subcollection
        ), f"Mismatch between 'is_subcollection' of {p!r} and expected value"
    assert (
        reverse.primary_property.name == property.secondary_property.reverse_name
    ), f"Mismatch between name of primary property of {reverse!r} and expected value"
    assert (
        reverse.primary_property.other_collection
        is property.secondary_property.collection
    ), f"Mismatch between name of the target collection of primary property of {reverse!r} and expected value"
    assert (
        reverse.secondary_property.name == property.primary_property.reverse_name
    ), f"Mismatch between name of secondary property of {reverse!r} and expected value"
    assert (
        reverse.other_collection is property.collection
    ), f"Mismatch between the target collection of the secondary property of {reverse!r} and expected value"
    assert (
        reverse.reverse_name == property_name
    ), f"Mismatch between 'reverse_name' of {reverse!r} and expected value"
    assert (
        reverse.is_plural != no_collisions
    ), f"Mismatch between 'is_plural' of {reverse!r} and expected value"
    reverse_inherited_dict: dict[str, str] = {}
    for alias, inh in reverse.inherited_properties.items():
        assert isinstance(inh, InheritedPropertyMetadata)
        reverse_inherited_dict[alias] = inh.property_to_inherit.path
    assert (
        reverse_inherited_dict == reverse_inherited_properties
    ), f"Mismatch between 'inherited_properties' of {reverse!r} and expected value"
