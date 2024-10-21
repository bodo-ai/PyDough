"""
TODO: add file-level docstring
"""

import pytest
from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.graphs import GraphMetadata
from pydough.metadata.collections import CollectionMetadata, SimpleTableMetadata
from pydough.metadata.properties import PropertyMetadata, TableColumnMetadata
from typing import List, Dict, Set
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
            "amazon",
            {
                "Amazon": {"Amazon"},
                "username": {"Amazon.Customers.username"},
                "first_name": {"Amazon.Customers.first_name"},
                "last_name": {"Amazon.Customers.last_name"},
                "phone_number": {"Amazon.Customers.phone_number"},
                "email": {"Amazon.Customers.email"},
                "signup_date": {"Amazon.Customers.signup_date"},
                "occupancies": {
                    "Amazon.Customers.occupancies",
                    "Amazon.Addresses.occupancies",
                },
                "packages_ordered": {"Amazon.Customers.packages_ordered"},
                "addresses": {"Amazon.Customers.addresses"},
                "occupant_username": {"Amazon.Occupancies.occupant_username"},
                "customer": {"Amazon.Packages.customer", "Amazon.Occupancies.customer"},
                "date_started_occupying": {
                    "Amazon.Addresses.occupants.date_started_occupying",
                    "Amazon.Customers.addresses.date_started_occupying",
                    "Amazon.Occupancies.date_started_occupying",
                },
                "date_stopped_occupying": {
                    "Amazon.Occupancies.date_stopped_occupying",
                    "Amazon.Customers.addresses.date_stopped_occupying",
                    "Amazon.Addresses.occupants.date_stopped_occupying",
                },
                "address_id": {"Amazon.Occupancies.address_id"},
                "address": {"Amazon.Occupancies.address"},
                "customer_username": {"Amazon.Packages.customer_username"},
                "id": {"Amazon.Packages.id", "Amazon.Addresses.id"},
                "order_timestamp": {"Amazon.Packages.order_timestamp"},
                "shipping_address_id": {"Amazon.Packages.shipping_address_id"},
                "billing_address_id": {"Amazon.Packages.billing_address_id"},
                "shipping_address": {"Amazon.Packages.shipping_address"},
                "billing_address": {"Amazon.Packages.billing_address"},
                "inventory": {"Amazon.Packages.inventory"},
                "contents": {"Amazon.Packages.contents"},
                "package_id": {"Amazon.PackageContents.package_id"},
                "package": {"Amazon.PackageContents.package"},
                "quantity_ordered": {
                    "Amazon.Products.packages_containing.quantity_ordered",
                    "Amazon.Packages.contents.quantity_ordered",
                    "Amazon.PackageContents.quantity_ordered",
                },
                "product_name": {"Amazon.PackageContents.product_name"},
                "product": {"Amazon.PackageContents.product"},
                "name": {"Amazon.Products.name"},
                "containment_records": {"Amazon.Products.containment_records"},
                "packages_containing": {"Amazon.Products.packages_containing"},
                "product_type": {"Amazon.Products.product_type"},
                "product_category": {"Amazon.Products.product_category"},
                "price_per_unit": {"Amazon.Products.price_per_unit"},
                "occupants": {"Amazon.Addresses.occupants"},
                "packages_shipped_to": {"Amazon.Addresses.packages_shipped_to"},
                "packages_billed_to": {"Amazon.Addresses.packages_billed_to"},
                "street_number": {"Amazon.Addresses.street_number"},
                "street_name": {"Amazon.Addresses.street_name"},
                "apartment": {"Amazon.Addresses.apartment"},
                "zip_code": {"Amazon.Addresses.zip_code"},
                "city": {"Amazon.Addresses.city"},
                "state": {"Amazon.Addresses.state"},
            },
            id="amazon",
        ),
        pytest.param(
            "tpch",
            {
                "TPCH": {"TPCH"},
                "key": {
                    "TPCH.Nations.key",
                    "TPCH.Regions.key",
                    "TPCH.Parts.key",
                    "TPCH.Orders.key",
                    "TPCH.Customers.key",
                    "TPCH.Suppliers.key",
                },
                "name": {
                    "TPCH.Nations.name",
                    "TPCH.Suppliers.name",
                    "TPCH.Regions.name",
                    "TPCH.Customers.name",
                    "TPCH.Parts.name",
                },
                "comment": {
                    "TPCH.Regions.comment",
                    "TPCH.Parts.comment",
                    "TPCH.PartSupp.comment",
                    "TPCH.Customers.comment",
                    "TPCH.Nations.comment",
                    "TPCH.Lineitems.comment",
                    "TPCH.Orders.comment",
                    "TPCH.Suppliers.comment",
                },
                "nations": {"TPCH.Regions.nations"},
                "customers": {"TPCH.Nations.customers", "TPCH.Regions.customers"},
                "suppliers": {"TPCH.Regions.suppliers", "TPCH.Nations.suppliers"},
                "orders_shipped_to": {
                    "TPCH.Nations.orders_shipped_to",
                    "TPCH.Regions.orders_shipped_to",
                },
                "region_key": {"TPCH.Nations.region_key"},
                "region": {
                    "TPCH.Customers.region",
                    "TPCH.Nations.region",
                    "TPCH.Suppliers.region",
                },
                "manufacturer": {"TPCH.Parts.manufacturer"},
                "brand": {"TPCH.Parts.brand"},
                "type": {"TPCH.Parts.type"},
                "size": {"TPCH.Parts.size"},
                "container": {"TPCH.Parts.container"},
                "retail_price": {"TPCH.Parts.retail_price"},
                "supply_records": {
                    "TPCH.Parts.supply_records",
                    "TPCH.Suppliers.supply_records",
                },
                "suppliers_of_part": {"TPCH.Parts.suppliers_of_part"},
                "lines": {
                    "TPCH.PartSupp.lines",
                    "TPCH.Suppliers.lines",
                    "TPCH.Parts.lines",
                    "TPCH.Orders.lines",
                },
                "nation_key": {
                    "TPCH.Suppliers.nation_key",
                    "TPCH.Customers.nation_key",
                },
                "nation": {"TPCH.Customers.nation", "TPCH.Suppliers.nation"},
                "parts_supplied": {"TPCH.Suppliers.parts_supplied"},
                "address": {"TPCH.Suppliers.address", "TPCH.Customers.address"},
                "phone": {"TPCH.Suppliers.phone", "TPCH.Customers.phone"},
                "account_balance": {"TPCH.Suppliers.account_balance"},
                "nation_name": {
                    "TPCH.Suppliers.region.nation_name",
                    "TPCH.Customers.region.nation_name",
                    "TPCH.Regions.suppliers.nation_name",
                    "TPCH.Regions.customers.nation_name",
                },
                "ps_lines": {
                    "TPCH.Parts.suppliers_of_part.ps_lines",
                    "TPCH.Suppliers.parts_supplied.ps_lines",
                },
                "ps_availqty": {
                    "TPCH.Suppliers.parts_supplied.ps_availqty",
                    "TPCH.Parts.lines.ps_availqty",
                    "TPCH.Parts.suppliers_of_part.ps_availqty",
                    "TPCH.Suppliers.lines.ps_availqty",
                    "TPCH.Lineitems.part.ps_availqty",
                    "TPCH.Lineitems.supplier.ps_availqty",
                },
                "ps_supplycost": {
                    "TPCH.Suppliers.parts_supplied.ps_supplycost",
                    "TPCH.Lineitems.part.ps_supplycost",
                    "TPCH.Suppliers.lines.ps_supplycost",
                    "TPCH.Lineitems.supplier.ps_supplycost",
                    "TPCH.Parts.lines.ps_supplycost",
                    "TPCH.Parts.suppliers_of_part.ps_supplycost",
                },
                "ps_comment": {
                    "TPCH.Suppliers.lines.ps_comment",
                    "TPCH.Suppliers.parts_supplied.ps_comment",
                    "TPCH.Lineitems.supplier.ps_comment",
                    "TPCH.Lineitems.part.ps_comment",
                    "TPCH.Parts.suppliers_of_part.ps_comment",
                    "TPCH.Parts.lines.ps_comment",
                },
                "part_key": {"TPCH.PartSupp.part_key", "TPCH.Lineitems.part_key"},
                "supplier_key": {
                    "TPCH.PartSupp.supplier_key",
                    "TPCH.Lineitems.supplier_key",
                },
                "part_and_supplier": {"TPCH.Lineitems.part_and_supplier"},
                "part": {"TPCH.PartSupp.part", "TPCH.Lineitems.part"},
                "supplier": {"TPCH.PartSupp.supplier", "TPCH.Lineitems.supplier"},
                "order_key": {"TPCH.Lineitems.order_key"},
                "line_number": {"TPCH.Lineitems.line_number"},
                "quantity": {"TPCH.Lineitems.quantity"},
                "extended_price": {"TPCH.Lineitems.extended_price"},
                "discount": {"TPCH.Lineitems.discount"},
                "tax": {"TPCH.Lineitems.tax"},
                "status": {"TPCH.Lineitems.status"},
                "ship_date": {"TPCH.Lineitems.ship_date"},
                "commit_date": {"TPCH.Lineitems.commit_date"},
                "receipt_date": {"TPCH.Lineitems.receipt_date"},
                "ship_instruct": {"TPCH.Lineitems.ship_instruct"},
                "ship_mode": {"TPCH.Lineitems.ship_mode"},
                "order": {"TPCH.Lineitems.order"},
                "ps_supplier": {
                    "TPCH.Lineitems.part.ps_supplier",
                    "TPCH.Parts.lines.ps_supplier",
                },
                "ps_part": {
                    "TPCH.Lineitems.supplier.ps_part",
                    "TPCH.Suppliers.lines.ps_part",
                },
                "availqty": {"TPCH.PartSupp.availqty"},
                "supplycost": {"TPCH.PartSupp.supplycost"},
                "customer_key": {"TPCH.Orders.customer_key"},
                "order_status": {"TPCH.Orders.order_status"},
                "total_price": {"TPCH.Orders.total_price"},
                "order_date": {"TPCH.Orders.order_date"},
                "order_priority": {"TPCH.Orders.order_priority"},
                "clerk": {"TPCH.Orders.clerk"},
                "ship_priority": {"TPCH.Orders.ship_priority"},
                "customer": {"TPCH.Orders.customer"},
                "shipping_nation": {"TPCH.Orders.shipping_nation"},
                "shipping_region": {"TPCH.Orders.shipping_region"},
                "orders": {"TPCH.Customers.orders"},
                "acctbal": {"TPCH.Customers.acctbal"},
            },
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
    graph_name, collection_name, table_path, unique_properties, get_graph
):
    """
    Testing that the table path and unique properties fields of simple table
    collections are set correctly.
    """
    graph: GraphMetadata = get_graph(graph_name)
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
    graph_name, collection_name, property_name, column_name, data_type, get_graph
):
    """
    Testing that the type and column name fields of properties are set
    correctly.
    """
    graph: GraphMetadata = get_graph(graph_name)
    collection: CollectionMetadata = graph.get_collection(collection_name)
    property: PropertyMetadata = collection.get_property(property_name)
    assert isinstance(property, TableColumnMetadata)
    assert property.column_name == column_name
    assert property.data_type == data_type
