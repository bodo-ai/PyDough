"""
TODO: add file-level docstring
"""

from collections.abc import Callable

import pytest
from test_utils import graph_fetcher

import pydough
from pydough.metadata import (
    CollectionMetadata,
    GraphMetadata,
    PropertyMetadata,
)


def graph_info(
    graph_name: str, collection_name: str, property_name: str, fetcher: graph_fetcher
) -> str:
    """ """
    graph: GraphMetadata = fetcher(graph_name)
    return pydough.explain_meta(graph)


def collection_info(
    graph_name: str, collection_name: str, property_name: str, fetcher: graph_fetcher
) -> str:
    """ """
    graph: GraphMetadata = fetcher(graph_name)
    collection = graph[collection_name]
    assert isinstance(collection, CollectionMetadata)
    return pydough.explain_meta(collection)


def property_info(
    graph_name: str, collection_name: str, property_name: str, fetcher: graph_fetcher
) -> str:
    """ """
    graph: GraphMetadata = fetcher(graph_name)
    collection = graph[collection_name]
    assert isinstance(collection, CollectionMetadata)
    property = collection[property_name]
    assert isinstance(property, PropertyMetadata)
    return pydough.explain_meta(property)


@pytest.fixture(
    params=[
        pytest.param(
            (
                ("Empty", "", ""),
                graph_info,
                """
PyDough graph: Empty
Collections: graph contains no collections
""",
            ),
            id="explain_graph_empty",
        ),
        pytest.param(
            (
                ("TPCH", "", ""),
                graph_info,
                """
PyDough graph: TPCH
Collections:
  Customers
  Lineitems
  Nations
  Orders
  PartSupp
  Parts
  Regions
  Suppliers
Call pydough.explain_meta(graph[collection_name]) to learn more about any of these collections.
""",
            ),
            id="explain_graph_tpch",
        ),
        pytest.param(
            (
                ("TPCH", "Customers", ""),
                collection_info,
                """
PyDough collection: simple table collection 'Customers' in graph 'TPCH'
Table path: tpch.CUSTOMER
Unique properties of collection: ['key']
Scalar properties:
  acctbal
  address
  comment
  key
  mktsegment
  name
  nation_key
  phone
Subcollection properties:
  nation
  orders
  region
Call pydough.explain_meta(graph['Customers'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_customers",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", ""),
                collection_info,
                """
PyDough collection: simple table collection 'Regions' in graph 'TPCH'
Table path: tpch.REGION
Unique properties of collection: ['key']
Scalar properties:
  comment
  key
  name
Subcollection properties:
  customers
  lines_sourced_from
  nations
  orders_shipped_to
  suppliers
Call pydough.explain_meta(graph['Regions'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_regions",
        ),
        pytest.param(
            (
                ("TPCH", "Lineitems", ""),
                collection_info,
                """
PyDough collection: simple table collection 'Lineitems' in graph 'TPCH'
Table path: tpch.LINEITEM
Unique properties of collection: [['order_key', 'line_number'], ['part_key', 'supplier_key', 'order_key']]
Scalar properties:
  comment
  commit_date
  discount
  extended_price
  line_number
  order_key
  part_key
  quantity
  receipt_date
  return_flag
  ship_date
  ship_instruct
  ship_mode
  status
  supplier_key
  tax
Subcollection properties:
  order
  part
  part_and_supplier
  supplier
  supplier_region
Call pydough.explain_meta(graph['Lineitems'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_lineitems",
        ),
        pytest.param(
            (
                ("TPCH", "PartSupp", ""),
                collection_info,
                """
PyDough collection: simple table collection 'PartSupp' in graph 'TPCH'
Table path: tpch.PARTSUPP
Unique properties of collection: [['part_key', 'supplier_key']]
Scalar properties:
  availqty
  comment
  part_key
  supplier_key
  supplycost
Subcollection properties:
  lines
  part
  supplier
Call pydough.explain_meta(graph['PartSupp'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_partsupp",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "key"),
                property_info,
                """
PyDough property: table column property 'key' of simple table collection 'Regions' in graph 'TPCH'
Column name: tpch.REGION.r_regionkey
Data type: int64
""",
            ),
            id="explain_property_tpch_regions_key",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "name"),
                property_info,
                """
PyDough property: table column property 'name' of simple table collection 'Regions' in graph 'TPCH'
Column name: tpch.REGION.r_name
Data type: string
""",
            ),
            id="explain_property_tpch_regions_name",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "comment"),
                property_info,
                """
PyDough property: table column property 'comment' of simple table collection 'Regions' in graph 'TPCH'
Column name: tpch.REGION.r_comment
Data type: string
""",
            ),
            id="explain_property_tpch_regions_comment",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "nations"),
                property_info,
                """
PyDough property: simple join property 'nations' of simple table collection 'Regions' in graph 'TPCH'
Connects collection Regions to Nations
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Nations.region
The subcollection relationship is defined by the following join conditions:
    Regions.key == Nations.region_key
""",
            ),
            id="explain_property_tpch_regions_nations",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "customers"),
                property_info,
                """
PyDough property: compound property 'customers' of simple table collection 'Regions' in graph 'TPCH'
Connects collection Regions to Customers
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Customers.region
Note: this property is a compound property; it is an alias for Regions.nations.customers.
The following properties are inherited from Regions.nations:
  nation_name is an alias for Regions.nations.nation_name
""",
            ),
            id="explain_property_tpch_regions_customers",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "suppliers"),
                property_info,
                """
PyDough property: compound property 'suppliers' of simple table collection 'Regions' in graph 'TPCH'
Connects collection Regions to Suppliers
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Suppliers.region
Note: this property is a compound property; it is an alias for Regions.nations.suppliers.
The following properties are inherited from Regions.nations:
  nation_name is an alias for Regions.nations.nation_name
""",
            ),
            id="explain_property_tpch_regions_suppliers",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "orders_shipped_to"),
                property_info,
                """
PyDough property: compound property 'orders_shipped_to' of simple table collection 'Regions' in graph 'TPCH'
Connects collection Regions to Orders
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Orders.shipping_region
Note: this property is a compound property; it is an alias for Regions.customers.orders.
The following properties are inherited from Regions.customers:
  nation_name is an alias for Regions.customers.nation_name
""",
            ),
            id="explain_property_tpch_regions_orders_shipped_to",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "lines_sourced_from"),
                property_info,
                """
PyDough property: compound property 'lines_sourced_from' of simple table collection 'Regions' in graph 'TPCH'
Connects collection Regions to Lineitems
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Lineitems.supplier_region
Note: this property is a compound property; it is an alias for Regions.suppliers.lines.
The following properties are inherited from Regions.suppliers:
  nation_name is an alias for Regions.suppliers.nation_name
  other_parts_supplied is an alias for Regions.suppliers.other_parts_supplied
  supplier_address is an alias for Regions.suppliers.supplier_address
  supplier_name is an alias for Regions.suppliers.supplier_name
""",
            ),
            id="explain_property_tpch_regions_lines_sourced_from",
        ),
        pytest.param(
            (
                ("TPCH", "PartSupp", "part"),
                property_info,
                """
PyDough property: simple join property 'part' of simple table collection 'PartSupp' in graph 'TPCH'
Connects collection PartSupp to Parts
Cardinality of connection: Many -> One
Is reversible: yes
Reverse property: Parts.supply_records
The subcollection relationship is defined by the following join conditions:
    PartSupp.part_key == Parts.key
""",
            ),
            id="explain_property_tpch_partsupp_part",
        ),
        pytest.param(
            (
                ("TPCH", "PartSupp", "supplier"),
                property_info,
                """
PyDough property: simple join property 'supplier' of simple table collection 'PartSupp' in graph 'TPCH'
Connects collection PartSupp to Suppliers
Cardinality of connection: Many -> One
Is reversible: yes
Reverse property: Suppliers.supply_records
The subcollection relationship is defined by the following join conditions:
    PartSupp.supplier_key == Suppliers.key
""",
            ),
            id="explain_property_tpch_partsupp_supplier",
        ),
        pytest.param(
            (
                ("TPCH", "PartSupp", "lines"),
                property_info,
                """
PyDough property: simple join property 'lines' of simple table collection 'PartSupp' in graph 'TPCH'
Connects collection PartSupp to Lineitems
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Lineitems.part_and_supplier
The subcollection relationship is defined by the following join conditions:
    PartSupp.part_key == Lineitems.part_key
    PartSupp.supplier_key == Lineitems.supplier_key
""",
            ),
            id="explain_property_tpch_partsupp_lines",
        ),
        pytest.param(
            (
                ("TPCH", "Suppliers", "nation"),
                property_info,
                """
PyDough property: simple join property 'nation' of simple table collection 'Suppliers' in graph 'TPCH'
Connects collection Suppliers to Nations
Cardinality of connection: Many -> One
Is reversible: yes
Reverse property: Nations.suppliers
The subcollection relationship is defined by the following join conditions:
    Suppliers.nation_key == Nations.key
""",
            ),
            id="explain_property_tpch_suppliers_nation",
        ),
        pytest.param(
            (
                ("TPCH", "Suppliers", "supply_records"),
                property_info,
                """
PyDough property: simple join property 'supply_records' of simple table collection 'Suppliers' in graph 'TPCH'
Connects collection Suppliers to PartSupp
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: PartSupp.supplier
The subcollection relationship is defined by the following join conditions:
    Suppliers.key == PartSupp.supplier_key
""",
            ),
            id="explain_property_tpch_suppliers_supply_records",
        ),
        pytest.param(
            (
                ("TPCH", "Suppliers", "parts_supplied"),
                property_info,
                """
PyDough property: compound property 'parts_supplied' of simple table collection 'Suppliers' in graph 'TPCH'
Connects collection Suppliers to Parts
Cardinality of connection: Many -> Many
Is reversible: yes
Reverse property: Parts.suppliers_of_part
Note: this property is a compound property; it is an alias for Suppliers.supply_records.part.
The following properties are inherited from Suppliers.supply_records:
  ps_availqty is an alias for Suppliers.supply_records.ps_availqty
  ps_comment is an alias for Suppliers.supply_records.ps_comment
  ps_lines is an alias for Suppliers.supply_records.ps_lines
  ps_supplycost is an alias for Suppliers.supply_records.ps_supplycost
""",
            ),
            id="explain_property_tpch_suppliers_parts_supplied",
        ),
        pytest.param(
            (
                ("TPCH", "Suppliers", "lines"),
                property_info,
                """
PyDough property: simple join property 'lines' of simple table collection 'Suppliers' in graph 'TPCH'
Connects collection Suppliers to Lineitems
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Lineitems.supplier
The subcollection relationship is defined by the following join conditions:
    Suppliers.key == Lineitems.supplier_key
""",
            ),
            id="explain_property_tpch_suppliers_lines",
        ),
        pytest.param(
            (
                ("TPCH", "Suppliers", "region"),
                property_info,
                """
PyDough property: compound property 'region' of simple table collection 'Suppliers' in graph 'TPCH'
Connects collection Suppliers to Regions
Cardinality of connection: Many -> One
Is reversible: yes
Reverse property: Regions.suppliers
Note: this property is a compound property; it is an alias for Suppliers.nation.region.
The following properties are inherited from Suppliers.nation:
  nation_name is an alias for Suppliers.nation.nation_name
""",
            ),
            id="explain_property_tpch_suppliers_region",
        ),
    ]
)
def graph_exploration_test_data(request) -> tuple[Callable[[graph_fetcher], str], str]:
    """
    TODO
    """
    args: tuple[str, str, str] = request.param[0]
    test_impl: Callable[[str, str, str, graph_fetcher], str] = request.param[1]
    refsol: str = request.param[2]

    def wrapped_test_impl(fetcher: graph_fetcher):
        return test_impl(args[0], args[1], args[2], fetcher)

    return wrapped_test_impl, refsol.strip()


def test_graph_exploration(
    graph_exploration_test_data: tuple[Callable[[graph_fetcher], str], str],
    get_sample_graph: graph_fetcher,
) -> None:
    """
    TODO
    """
    test_impl, answer = graph_exploration_test_data
    explanation_string: str = test_impl(get_sample_graph)
    assert (
        explanation_string == answer
    ), "Mismatch between produced string and expected answer"
