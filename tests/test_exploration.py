"""
TODO: add file-level docstring
"""

from collections.abc import Callable

import pytest
from exploration_examples import (
    contextless_aggfunc_impl,
    contextless_back_impl,
    contextless_collections_impl,
    contextless_expr_impl,
    contextless_func_impl,
    filter_impl,
    global_agg_calc_impl,
    global_calc_impl,
    global_impl,
    nation_expr_impl,
    nation_impl,
    nation_name_impl,
    order_impl,
    partition_child_impl,
    partition_impl,
    subcollection_calc_backref_impl,
    table_calc_impl,
    top_k_impl,
)
from test_utils import graph_fetcher

import pydough
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode


@pytest.fixture(
    params=[
        pytest.param(
            (
                ("Empty", None, None),
                """
PyDough graph: Empty
Collections: graph contains no collections
""",
                """
PyDough graph: Empty
Collections: graph contains no collections
""",
            ),
            id="explain_graph_empty",
        ),
        pytest.param(
            (
                ("TPCH", None, None),
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
Call pydough.explain(graph[collection_name]) to learn more about any of these collections.
Call pydough.explain_structure(graph) to see how all of the collections in the graph are connected.
""",
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
Call pydough.explain(graph[collection_name]) to learn more about any of these collections.
Call pydough.explain_structure(graph) to see how all of the collections in the graph are connected.
""",
            ),
            id="explain_graph_tpch",
        ),
        pytest.param(
            (
                ("TPCH", "Customers", None),
                """
PyDough collection: Customers
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
Call pydough.explain(graph['Customers'][property_name]) to learn more about any of these properties.
""",
                """
PyDough collection: Customers
Scalar properties: acctbal, address, comment, key, mktsegment, name, nation_key, phone
Subcollection properties: nation, orders, region
Call pydough.explain(graph['Customers'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_customers",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", None),
                """
PyDough collection: Regions
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
Call pydough.explain(graph['Regions'][property_name]) to learn more about any of these properties.
""",
                """
PyDough collection: Regions
Scalar properties: comment, key, name
Subcollection properties: customers, lines_sourced_from, nations, orders_shipped_to, suppliers
Call pydough.explain(graph['Regions'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_regions",
        ),
        pytest.param(
            (
                ("TPCH", "Lineitems", None),
                """
PyDough collection: Lineitems
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
Call pydough.explain(graph['Lineitems'][property_name]) to learn more about any of these properties.
""",
                """
PyDough collection: Lineitems
Scalar properties: comment, commit_date, discount, extended_price, line_number, order_key, part_key, quantity, receipt_date, return_flag, ship_date, ship_instruct, ship_mode, status, supplier_key, tax
Subcollection properties: order, part, part_and_supplier, supplier, supplier_region
Call pydough.explain(graph['Lineitems'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_lineitems",
        ),
        pytest.param(
            (
                ("TPCH", "PartSupp", None),
                """
PyDough collection: PartSupp
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
Call pydough.explain(graph['PartSupp'][property_name]) to learn more about any of these properties.
""",
                """
PyDough collection: PartSupp
Scalar properties: availqty, comment, part_key, supplier_key, supplycost
Subcollection properties: lines, part, supplier
Call pydough.explain(graph['PartSupp'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_partsupp",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "key"),
                """
PyDough property: Regions.key
Column name: tpch.REGION.r_regionkey
Data type: int64
""",
                """
PyDough property: Regions.key
Column name: tpch.REGION.r_regionkey
Data type: int64
""",
            ),
            id="explain_property_tpch_regions_key",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "name"),
                """
PyDough property: Regions.name
Column name: tpch.REGION.r_name
Data type: string
""",
                """
PyDough property: Regions.name
Column name: tpch.REGION.r_name
Data type: string
""",
            ),
            id="explain_property_tpch_regions_name",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "comment"),
                """
PyDough property: Regions.comment
Column name: tpch.REGION.r_comment
Data type: string
""",
                """
PyDough property: Regions.comment
Column name: tpch.REGION.r_comment
Data type: string
""",
            ),
            id="explain_property_tpch_regions_comment",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "nations"),
                """
PyDough property: Regions.nations
This property connects collection Regions to Nations.
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Nations.region
The subcollection relationship is defined by the following join conditions:
    Regions.key == Nations.region_key
""",
                """
PyDough property: Regions.nations
This property connects collection Regions to Nations.
Use pydough.explain(graph['Regions']['nations'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_regions_nations",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "customers"),
                """
PyDough property: Regions.customers
This property connects collection Regions to Customers.
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Customers.region
Note: this property is a compound property; it is an alias for Regions.nations.customers.
The following properties are inherited from Regions.nations:
  nation_name is an alias for Regions.nations.nation_name
""",
                """
PyDough property: Regions.customers
This property connects collection Regions to Customers.
Use pydough.explain(graph['Regions']['customers'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_regions_customers",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "suppliers"),
                """
PyDough property: Regions.suppliers
This property connects collection Regions to Suppliers.
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Suppliers.region
Note: this property is a compound property; it is an alias for Regions.nations.suppliers.
The following properties are inherited from Regions.nations:
  nation_name is an alias for Regions.nations.nation_name
""",
                """
PyDough property: Regions.suppliers
This property connects collection Regions to Suppliers.
Use pydough.explain(graph['Regions']['suppliers'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_regions_suppliers",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "orders_shipped_to"),
                """
PyDough property: Regions.orders_shipped_to
This property connects collection Regions to Orders.
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Orders.shipping_region
Note: this property is a compound property; it is an alias for Regions.customers.orders.
The following properties are inherited from Regions.customers:
  nation_name is an alias for Regions.customers.nation_name
""",
                """
PyDough property: Regions.orders_shipped_to
This property connects collection Regions to Orders.
Use pydough.explain(graph['Regions']['orders_shipped_to'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_regions_orders_shipped_to",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "lines_sourced_from"),
                """
PyDough property: Regions.lines_sourced_from
This property connects collection Regions to Lineitems.
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
                """
PyDough property: Regions.lines_sourced_from
This property connects collection Regions to Lineitems.
Use pydough.explain(graph['Regions']['lines_sourced_from'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_regions_lines_sourced_from",
        ),
        pytest.param(
            (
                ("TPCH", "PartSupp", "part"),
                """
PyDough property: PartSupp.part
This property connects collection PartSupp to Parts.
Cardinality of connection: Many -> One
Is reversible: yes
Reverse property: Parts.supply_records
The subcollection relationship is defined by the following join conditions:
    PartSupp.part_key == Parts.key
""",
                """
PyDough property: PartSupp.part
This property connects collection PartSupp to Parts.
Use pydough.explain(graph['PartSupp']['part'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_partsupp_part",
        ),
        pytest.param(
            (
                ("TPCH", "PartSupp", "supplier"),
                """
PyDough property: PartSupp.supplier
This property connects collection PartSupp to Suppliers.
Cardinality of connection: Many -> One
Is reversible: yes
Reverse property: Suppliers.supply_records
The subcollection relationship is defined by the following join conditions:
    PartSupp.supplier_key == Suppliers.key
""",
                """
PyDough property: PartSupp.supplier
This property connects collection PartSupp to Suppliers.
Use pydough.explain(graph['PartSupp']['supplier'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_partsupp_supplier",
        ),
        pytest.param(
            (
                ("TPCH", "PartSupp", "lines"),
                """
PyDough property: PartSupp.lines
This property connects collection PartSupp to Lineitems.
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Lineitems.part_and_supplier
The subcollection relationship is defined by the following join conditions:
    PartSupp.part_key == Lineitems.part_key
    PartSupp.supplier_key == Lineitems.supplier_key
""",
                """
PyDough property: PartSupp.lines
This property connects collection PartSupp to Lineitems.
Use pydough.explain(graph['PartSupp']['lines'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_partsupp_lines",
        ),
        pytest.param(
            (
                ("TPCH", "Suppliers", "nation"),
                """
PyDough property: Suppliers.nation
This property connects collection Suppliers to Nations.
Cardinality of connection: Many -> One
Is reversible: yes
Reverse property: Nations.suppliers
The subcollection relationship is defined by the following join conditions:
    Suppliers.nation_key == Nations.key
""",
                """
PyDough property: Suppliers.nation
This property connects collection Suppliers to Nations.
Use pydough.explain(graph['Suppliers']['nation'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_suppliers_nation",
        ),
        pytest.param(
            (
                ("TPCH", "Suppliers", "supply_records"),
                """
PyDough property: Suppliers.supply_records
This property connects collection Suppliers to PartSupp.
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: PartSupp.supplier
The subcollection relationship is defined by the following join conditions:
    Suppliers.key == PartSupp.supplier_key
""",
                """
PyDough property: Suppliers.supply_records
This property connects collection Suppliers to PartSupp.
Use pydough.explain(graph['Suppliers']['supply_records'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_suppliers_supply_records",
        ),
        pytest.param(
            (
                ("TPCH", "Suppliers", "parts_supplied"),
                """
PyDough property: Suppliers.parts_supplied
This property connects collection Suppliers to Parts.
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
                """
PyDough property: Suppliers.parts_supplied
This property connects collection Suppliers to Parts.
Use pydough.explain(graph['Suppliers']['parts_supplied'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_suppliers_parts_supplied",
        ),
        pytest.param(
            (
                ("TPCH", "Suppliers", "lines"),
                """
PyDough property: Suppliers.lines
This property connects collection Suppliers to Lineitems.
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: Lineitems.supplier
The subcollection relationship is defined by the following join conditions:
    Suppliers.key == Lineitems.supplier_key
""",
                """
PyDough property: Suppliers.lines
This property connects collection Suppliers to Lineitems.
Use pydough.explain(graph['Suppliers']['lines'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_suppliers_lines",
        ),
        pytest.param(
            (
                ("TPCH", "Suppliers", "region"),
                """
PyDough property: Suppliers.region
This property connects collection Suppliers to Regions.
Cardinality of connection: Many -> One
Is reversible: yes
Reverse property: Regions.suppliers
Note: this property is a compound property; it is an alias for Suppliers.nation.region.
The following properties are inherited from Suppliers.nation:
  nation_name is an alias for Suppliers.nation.nation_name
""",
                """
PyDough property: Suppliers.region
This property connects collection Suppliers to Regions.
Use pydough.explain(graph['Suppliers']['region'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_suppliers_region",
        ),
    ]
)
def metadata_exploration_test_data(
    request,
) -> tuple[Callable[[graph_fetcher, bool], str], str, str]:
    """
    Testing data used for `test_metadata_exploration`. Creates a function that
    takes in a `graph_fetcher` instance and returns the result of calling
    `pydough.explain` on the requested information based on the input
    tuple, as well as the expected output string. The input tuple is in one of
    the following formats:
    - `(graph_name, None, None)` -> get metadata for the graph
    - `(graph_name, collection_name, None)` -> get metadata for a collection
    - `(graph_name, collection_name, property_name)` -> get metadata for a
    property

    Returns both the string representations when verbose=True and when
    verbose=False.
    """
    args: tuple[str, str | None, str | None] = request.param[0]
    verbose_refsol: str = request.param[1]
    non_verbose_refsol: str = request.param[2]

    graph_name: str = args[0]
    collection_name: str | None = args[1]
    property_name: str | None = args[2]

    def wrapped_test_impl(fetcher: graph_fetcher, verbose: bool):
        graph: GraphMetadata = fetcher(graph_name)
        if collection_name is None:
            return pydough.explain(graph, verbose=verbose)
        elif property_name is None:
            return pydough.explain(graph[collection_name], verbose=verbose)
        else:
            return pydough.explain(
                graph[collection_name][property_name], verbose=verbose
            )

    return wrapped_test_impl, verbose_refsol.strip(), non_verbose_refsol.strip()


@pytest.mark.parametrize(
    "verbose",
    [
        pytest.param(True, id="verbose"),
        pytest.param(False, id="non_verbose"),
    ],
)
def test_metadata_exploration(
    metadata_exploration_test_data: tuple[
        Callable[[graph_fetcher, bool], str], str, str
    ],
    verbose: bool,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Verifies that `pydough.explain` called on metadata produces the exepcted
    strings.
    """
    test_impl, verbose_answer, non_verbose_answer = metadata_exploration_test_data
    explanation_string: str = test_impl(get_sample_graph, verbose)
    answer: str = verbose_answer if verbose else non_verbose_answer
    assert (
        explanation_string == answer
    ), "Mismatch between produced string and expected answer"


@pytest.mark.parametrize(
    "graph_name, answer",
    [
        pytest.param(
            "Empty",
            """
Structure of PyDough graph: Empty
  Graph contains no collections
""",
            id="empty",
        ),
        pytest.param(
            "TPCH",
            """
Structure of PyDough graph: TPCH

  Customers
  ├── acctbal
  ├── address
  ├── comment
  ├── key
  ├── mktsegment
  ├── name
  ├── nation_key
  ├── phone
  ├── nation [one member of Nations] (reverse of Nations.customers)
  ├── orders [multiple Orders] (reverse of Orders.customer)
  └── region [one member of Regions] (reverse of Regions.customers)

  Lineitems
  ├── comment
  ├── commit_date
  ├── discount
  ├── extended_price
  ├── line_number
  ├── order_key
  ├── part_key
  ├── quantity
  ├── receipt_date
  ├── return_flag
  ├── ship_date
  ├── ship_instruct
  ├── ship_mode
  ├── status
  ├── supplier_key
  ├── tax
  ├── order [one member of Orders] (reverse of Orders.lines)
  ├── part [one member of Parts] (reverse of Parts.lines)
  ├── part_and_supplier [one member of PartSupp] (reverse of PartSupp.lines)
  ├── supplier [one member of Suppliers] (reverse of Suppliers.lines)
  └── supplier_region [one member of Regions] (reverse of Regions.lines_sourced_from)

  Nations
  ├── comment
  ├── key
  ├── name
  ├── region_key
  ├── customers [multiple Customers] (reverse of Customers.nation)
  ├── orders_shipped_to [multiple Orders] (reverse of Orders.shipping_nation)
  ├── region [one member of Regions] (reverse of Regions.nations)
  └── suppliers [multiple Suppliers] (reverse of Suppliers.nation)

  Orders
  ├── clerk
  ├── comment
  ├── customer_key
  ├── key
  ├── order_date
  ├── order_priority
  ├── order_status
  ├── ship_priority
  ├── total_price
  ├── customer [one member of Customers] (reverse of Customers.orders)
  ├── lines [multiple Lineitems] (reverse of Lineitems.order)
  ├── shipping_nation [one member of Nations] (reverse of Nations.orders_shipped_to)
  └── shipping_region [one member of Regions] (reverse of Regions.orders_shipped_to)

  PartSupp
  ├── availqty
  ├── comment
  ├── part_key
  ├── supplier_key
  ├── supplycost
  ├── lines [multiple Lineitems] (reverse of Lineitems.part_and_supplier)
  ├── part [one member of Parts] (reverse of Parts.supply_records)
  └── supplier [one member of Suppliers] (reverse of Suppliers.supply_records)

  Parts
  ├── brand
  ├── comment
  ├── container
  ├── key
  ├── manufacturer
  ├── name
  ├── part_type
  ├── retail_price
  ├── size
  ├── lines [multiple Lineitems] (reverse of Lineitems.part)
  ├── suppliers_of_part [multiple Suppliers] (reverse of Suppliers.parts_supplied)
  └── supply_records [multiple PartSupp] (reverse of PartSupp.part)

  Regions
  ├── comment
  ├── key
  ├── name
  ├── customers [multiple Customers] (reverse of Customers.region)
  ├── lines_sourced_from [multiple Lineitems] (reverse of Lineitems.supplier_region)
  ├── nations [multiple Nations] (reverse of Nations.region)
  ├── orders_shipped_to [multiple Orders] (reverse of Orders.shipping_region)
  └── suppliers [multiple Suppliers] (reverse of Suppliers.region)

  Suppliers
  ├── account_balance
  ├── address
  ├── comment
  ├── key
  ├── name
  ├── nation_key
  ├── phone
  ├── lines [multiple Lineitems] (reverse of Lineitems.supplier)
  ├── nation [one member of Nations] (reverse of Nations.suppliers)
  ├── parts_supplied [multiple Parts] (reverse of Parts.suppliers_of_part)
  ├── region [one member of Regions] (reverse of Regions.suppliers)
  └── supply_records [multiple PartSupp] (reverse of PartSupp.supplier)
""",
            id="tpch",
        ),
    ],
)
def test_graph_structure(
    graph_name: str,
    answer: str,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Verifies that `pydough.explain` called on metadata produces the expected
    strings.
    """
    graph: GraphMetadata = get_sample_graph(graph_name)
    structure_string: str = pydough.explain_structure(graph)
    assert (
        structure_string == answer.strip()
    ), "Mismatch between produced string and expected answer"


@pytest.fixture(
    params=[
        pytest.param(
            (
                "TPCH",
                nation_impl,
                """
PyDough collection representing the following logic:
  ──┬─ TPCH
    └─── TableCollection[Nations]

This node, specifically, accesses the collection Nations. Call pydough.explain(graph['Nations']) to learn more about this collection.

The following terms will be included in the result if this collection is executed:
  comment, key, name, region_key

It is possible to use BACK to go up to 1 level above this collection.

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, orders_shipped_to, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.
""",
                """
This node, specifically, accesses the collection Nations. Call pydough.explain(graph['Nations']) to learn more about this collection.

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, orders_shipped_to, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
""",
            ),
            id="nation",
        ),
        pytest.param(
            (
                "TPCH",
                global_impl,
                """
PyDough collection representing the following logic:
  TPCH

This node is a reference to the global context for the entire graph. An operation must be done onto this node (e.g. a CALC or accessing a collection) before it can be executed.

The collection does not have any terms that can be included in a result if it is executed.

It is not possible to use BACK from this collection.

The collection has access to the following collections:
  Customers, Lineitems, Nations, Orders, PartSupp, Parts, Regions, Suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.
""",
                """
This node is a reference to the global context for the entire graph. An operation must be done onto this node (e.g. a CALC or accessing a collection) before it can be executed.

The collection has access to the following collections:
  Customers, Lineitems, Nations, Orders, PartSupp, Parts, Regions, Suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
""",
            ),
            id="global",
        ),
        pytest.param(
            (
                "TPCH",
                global_calc_impl,
                """
PyDough collection representing the following logic:
  ┌─── TPCH
  └─── Calc[x=42, y=13]

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  x <- 42
  y <- 13

The following terms will be included in the result if this collection is executed:
  x, y

It is not possible to use BACK from this collection.

The collection has access to the following expressions:
  x, y

The collection has access to the following collections:
  Customers, Lineitems, Nations, Orders, PartSupp, Parts, Regions, Suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.
""",
                """
The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  x <- 42
  y <- 13

The collection has access to the following expressions:
  x, y

The collection has access to the following collections:
  Customers, Lineitems, Nations, Orders, PartSupp, Parts, Regions, Suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
""",
            ),
            id="global_calc",
        ),
        pytest.param(
            (
                "TPCH",
                global_agg_calc_impl,
                """
PyDough collection representing the following logic:
  ┌─── TPCH
  └─┬─ Calc[n_customers=COUNT($1), avg_part_price=AVG($2.retail_price)]
    ├─┬─ AccessChild
    │ └─── TableCollection[Customers]
    └─┬─ AccessChild
      └─── TableCollection[Parts]

This node first derives the following children before doing its main task:
  child $1:
    └─── TableCollection[Customers]
  child $2:
    └─── TableCollection[Parts]

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  avg_part_price <- AVG($2.retail_price), aka AVG(Parts.retail_price)
  n_customers <- COUNT($1), aka COUNT(Customers)

The following terms will be included in the result if this collection is executed:
  avg_part_price, n_customers

It is not possible to use BACK from this collection.

The collection has access to the following expressions:
  avg_part_price, n_customers

The collection has access to the following collections:
  Customers, Lineitems, Nations, Orders, PartSupp, Parts, Regions, Suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.
""",
                """
This node first derives the following children before doing its main task:
  child $1:
    └─── TableCollection[Customers]
  child $2:
    └─── TableCollection[Parts]

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  avg_part_price <- AVG($2.retail_price), aka AVG(Parts.retail_price)
  n_customers <- COUNT($1), aka COUNT(Customers)

The collection has access to the following expressions:
  avg_part_price, n_customers

The collection has access to the following collections:
  Customers, Lineitems, Nations, Orders, PartSupp, Parts, Regions, Suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
""",
            ),
            id="global_agg_calc",
        ),
        pytest.param(
            (
                "TPCH",
                table_calc_impl,
                """
PyDough collection representing the following logic:
  ──┬─ TPCH
    ├─── TableCollection[Nations]
    └─┬─ Calc[name=name, region_name=$1.name, num_customers=COUNT($2)]
      ├─┬─ AccessChild
      │ └─── SubCollection[region]
      └─┬─ AccessChild
        └─── SubCollection[customers]

This node first derives the following children before doing its main task:
  child $1:
    └─── SubCollection[region]
  child $2:
    └─── SubCollection[customers]

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  name <- name (propagated from previous collection)
  num_customers <- COUNT($2), aka COUNT(customers)
  region_name <- $1.name, aka region.name

The following terms will be included in the result if this collection is executed:
  name, num_customers, region_name

It is possible to use BACK to go up to 1 level above this collection.

The collection has access to the following expressions:
  comment, key, name, num_customers, region_key, region_name

The collection has access to the following collections:
  customers, orders_shipped_to, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.
                """,
                """
This node first derives the following children before doing its main task:
  child $1:
    └─── SubCollection[region]
  child $2:
    └─── SubCollection[customers]

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  name <- name (propagated from previous collection)
  num_customers <- COUNT($2), aka COUNT(customers)
  region_name <- $1.name, aka region.name

The collection has access to the following expressions:
  comment, key, name, num_customers, region_key, region_name

The collection has access to the following collections:
  customers, orders_shipped_to, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
                """,
            ),
            id="table_calc",
        ),
        pytest.param(
            (
                "TPCH",
                subcollection_calc_backref_impl,
                """
PyDough collection representing the following logic:
  ──┬─ TPCH
    └─┬─ TableCollection[Regions]
      └─┬─ SubCollection[nations]
        ├─── SubCollection[customers]
        └─── Calc[name=name, nation_name=BACK(1).name, region_name=BACK(2).name]

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  name <- name (propagated from previous collection)
  nation_name <- BACK(1).name
  region_name <- BACK(2).name

The following terms will be included in the result if this collection is executed:
  name, nation_name, region_name

It is possible to use BACK to go up to 3 levels above this collection.

The collection has access to the following expressions:
  acctbal, address, comment, key, mktsegment, name, nation_key, nation_name, phone, region_name

The collection has access to the following collections:
  nation, orders, region

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.
                """,
                """
The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  name <- name (propagated from previous collection)
  nation_name <- BACK(1).name
  region_name <- BACK(2).name

The collection has access to the following expressions:
  acctbal, address, comment, key, mktsegment, name, nation_key, nation_name, phone, region_name

The collection has access to the following collections:
  nation, orders, region

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
                """,
            ),
            id="subcollection_calc_backref",
        ),
        pytest.param(
            (
                "TPCH",
                filter_impl,
                """
PyDough collection representing the following logic:
  ──┬─ TPCH
    ├─── TableCollection[Nations]
    ├─── Calc[name=name]
    └─┬─ Where[($1.name == 'ASIA') & HAS($2) & (COUNT($3) > 100)]
      ├─┬─ AccessChild
      │ └─── SubCollection[region]
      ├─┬─ AccessChild
      │ └─┬─ SubCollection[customers]
      │   └─┬─ SubCollection[orders]
      │     ├─── SubCollection[lines]
      │     └─┬─ Where[CONTAINS($1.name, 'STEEL')]
      │       └─┬─ AccessChild
      │         └─── SubCollection[part]
      └─┬─ AccessChild
        ├─── SubCollection[suppliers]
        └─── Where[account_balance >= 0.0]

This node first derives the following children before doing its main task:
  child $1:
    └─── SubCollection[region]
  child $2:
    └─┬─ SubCollection[customers]
      └─┬─ SubCollection[orders]
        ├─── SubCollection[lines]
        └─┬─ Where[CONTAINS($1.name, 'STEEL')]
          └─┬─ AccessChild
            └─── SubCollection[part]
  child $3:
    ├─── SubCollection[suppliers]
    └─── Where[account_balance >= 0.0]

The main task of this node is to filter on the following conditions:
  $1.name == 'ASIA', aka region.name == 'ASIA'
  HAS($2), aka HAS(customers.orders.lines.WHERE(CONTAINS(part.name, 'STEEL')))
  COUNT($3) > 100, aka COUNT(suppliers.WHERE(account_balance >= 0.0)) > 100

The following terms will be included in the result if this collection is executed:
  name

It is possible to use BACK to go up to 1 level above this collection.

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, orders_shipped_to, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.
                """,
                """
This node first derives the following children before doing its main task:
  child $1:
    └─── SubCollection[region]
  child $2:
    └─┬─ SubCollection[customers]
      └─┬─ SubCollection[orders]
        ├─── SubCollection[lines]
        └─┬─ Where[CONTAINS($1.name, 'STEEL')]
          └─┬─ AccessChild
            └─── SubCollection[part]
  child $3:
    ├─── SubCollection[suppliers]
    └─── Where[account_balance >= 0.0]

The main task of this node is to filter on the following conditions:
  $1.name == 'ASIA', aka region.name == 'ASIA'
  HAS($2), aka HAS(customers.orders.lines.WHERE(CONTAINS(part.name, 'STEEL')))
  COUNT($3) > 100, aka COUNT(suppliers.WHERE(account_balance >= 0.0)) > 100

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, orders_shipped_to, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
                """,
            ),
            id="filter",
        ),
        pytest.param(
            (
                "TPCH",
                order_impl,
                """
PyDough collection representing the following logic:
  ──┬─ TPCH
    ├─── TableCollection[Nations]
    ├─── Calc[name=name]
    └─┬─ OrderBy[COUNT($1).DESC(na_pos='last'), name.ASC(na_pos='last')]
      └─┬─ AccessChild
        └─── SubCollection[suppliers]

This node first derives the following children before doing its main task:
  child $1:
    └─── SubCollection[suppliers]

The main task of this node is to sort the collection on the following:
  COUNT($1), aka COUNT(suppliers), in descending order with nulls at the end
  with ties broken by: name, in ascending order with nulls at the end

The following terms will be included in the result if this collection is executed:
  name

It is possible to use BACK to go up to 1 level above this collection.

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, orders_shipped_to, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.
                """,
                """
This node first derives the following children before doing its main task:
  child $1:
    └─── SubCollection[suppliers]

The main task of this node is to sort the collection on the following:
  COUNT($1), aka COUNT(suppliers), in descending order with nulls at the end
  with ties broken by: name, in ascending order with nulls at the end

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, orders_shipped_to, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
                """,
            ),
            id="order",
        ),
        pytest.param(
            (
                "TPCH",
                top_k_impl,
                """
PyDough collection representing the following logic:
  ──┬─ TPCH
    ├─── TableCollection[Parts]
    ├─┬─ Calc[name=name, n_suppliers=COUNT($1)]
    │ └─┬─ AccessChild
    │   └─── SubCollection[suppliers_of_part]
    └─── TopK[100, n_suppliers.DESC(na_pos='last'), name.ASC(na_pos='last')]

The main task of this node is to sort the collection on the following and keep the first 100 records:
  n_suppliers, in descending order with nulls at the end
  with ties broken by: name, in ascending order with nulls at the end

The following terms will be included in the result if this collection is executed:
  n_suppliers, name

It is possible to use BACK to go up to 1 level above this collection.

The collection has access to the following expressions:
  brand, comment, container, key, manufacturer, n_suppliers, name, part_type, retail_price, size

The collection has access to the following collections:
  lines, suppliers_of_part, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.
                """,
                """
The main task of this node is to sort the collection on the following and keep the first 100 records:
  n_suppliers, in descending order with nulls at the end
  with ties broken by: name, in ascending order with nulls at the end

The collection has access to the following expressions:
  brand, comment, container, key, manufacturer, n_suppliers, name, part_type, retail_price, size

The collection has access to the following collections:
  lines, suppliers_of_part, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
                """,
            ),
            id="top_k",
        ),
        pytest.param(
            (
                "TPCH",
                partition_impl,
                """
PyDough collection representing the following logic:
  ┌─── TPCH
  └─┬─ Partition[name='p', by=part_type]
    └─┬─ AccessChild
      └─── TableCollection[Parts]

This node first derives the following children before doing its main task:
  child $1:
    └─── TableCollection[Parts]

The main task of this node is to partition the child data on the following keys:
  $1.part_type
Note: the subcollection of this collection containing records from the unpartitioned data is called 'p'.

The following terms will be included in the result if this collection is executed:
  part_type

It is possible to use BACK to go up to 1 level above this collection.

The collection has access to the following expressions:
  part_type

The collection has access to the following collections:
  p

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.
                """,
                """
This node first derives the following children before doing its main task:
  child $1:
    └─── TableCollection[Parts]

The main task of this node is to partition the child data on the following keys:
  $1.part_type
Note: the subcollection of this collection containing records from the unpartitioned data is called 'p'.

The collection has access to the following expressions:
  part_type

The collection has access to the following collections:
  p

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
                """,
            ),
            id="partition",
        ),
        pytest.param(
            (
                "TPCH",
                partition_child_impl,
                """
PyDough collection representing the following logic:
  ┌─── TPCH
  ├─┬─ Partition[name='p', by=part_type]
  │ └─┬─ AccessChild
  │   └─── TableCollection[Parts]
  ├─┬─ Calc[part_type=part_type, avg_price=AVG($1.retail_price)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[p]
  └─┬─ Where[avg_price >= 27.5]
    └─── PartitionChild[p]

This node, specifically, accesses the unpartitioned data of a partitioning (child name: p).
Using BACK(1) will access the partitioned data.

The following terms will be included in the result if this collection is executed:
  brand, comment, container, key, manufacturer, name, part_type, retail_price, size

It is possible to use BACK to go up to 2 levels above this collection.

The collection has access to the following expressions:
  brand, comment, container, key, manufacturer, name, part_type, retail_price, size

The collection has access to the following collections:
  lines, suppliers_of_part, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.
                """,
                """
This node, specifically, accesses the unpartitioned data of a partitioning (child name: p).
Using BACK(1) will access the partitioned data.

The collection has access to the following expressions:
  brand, comment, container, key, manufacturer, name, part_type, retail_price, size

The collection has access to the following collections:
  lines, suppliers_of_part, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
                """,
            ),
            id="partition_child",
        ),
        pytest.param(
            (
                "TPCH",
                nation_expr_impl,
                """
Property 'name' of TPCH.Nations is not a collection, therefore it cannot be an argument to pydough.explain.
Did you mean to use pydough.explain_term?
                """,
                """
Property 'name' of TPCH.Nations is not a collection, therefore it cannot be an argument to pydough.explain.
Did you mean to use pydough.explain_term?
                """,
            ),
            id="not_qualified_collection_a",
        ),
        pytest.param(
            (
                "TPCH",
                contextless_collections_impl,
                """
Unrecognized term of graph 'TPCH': 'lines'
This could mean you accessed a property using a name that does not exist, or that you need to place your PyDough code into a context for it to make sense.
Did you mean to use pydough.explain_term?
                """,
                """
Unrecognized term of graph 'TPCH': 'lines'
This could mean you accessed a property using a name that does not exist, or that you need to place your PyDough code into a context for it to make sense.
Did you mean to use pydough.explain_term?
                """,
            ),
            id="not_qualified_collection_b",
        ),
        pytest.param(
            (
                "TPCH",
                contextless_expr_impl,
                """
Unrecognized term of graph 'TPCH': 'name'
This could mean you accessed a property using a name that does not exist, or that you need to place your PyDough code into a context for it to make sense.
Did you mean to use pydough.explain_term?
                """,
                """
Unrecognized term of graph 'TPCH': 'name'
This could mean you accessed a property using a name that does not exist, or that you need to place your PyDough code into a context for it to make sense.
Did you mean to use pydough.explain_term?
                """,
            ),
            id="not_qualified_collection_c",
        ),
        pytest.param(
            (
                "TPCH",
                contextless_back_impl,
                """
Cannot call pydough.explain on BACK(1).fizz.
Did you mean to use pydough.explain_term?
""",
                """
Cannot call pydough.explain on BACK(1).fizz.
Did you mean to use pydough.explain_term?
""",
            ),
            id="not_qualified_collection_d",
        ),
        pytest.param(
            (
                "TPCH",
                contextless_aggfunc_impl,
                """
Cannot call pydough.explain on COUNT(?.customers).
Did you mean to use pydough.explain_term?
""",
                """
Cannot call pydough.explain on COUNT(?.customers).
Did you mean to use pydough.explain_term?
""",
            ),
            id="not_qualified_collection_e",
        ),
        pytest.param(
            (
                "TPCH",
                contextless_func_impl,
                """
Cannot call pydough.explain on LOWER(((?.first_name + ' ') + ?.last_name)).
Did you mean to use pydough.explain_term?
""",
                """
Cannot call pydough.explain on LOWER(((?.first_name + ' ') + ?.last_name)).
Did you mean to use pydough.explain_term?
""",
            ),
            id="not_qualified_collection_f",
        ),
    ]
)
def unqualified_exploration_test_data(
    request,
) -> tuple[str, Callable[[GraphMetadata], Callable[[], UnqualifiedNode]], str, str]:
    """
    Testing data used for test_unqualified_node_exploration. Returns a tuple of
    the graph name to use, a function that takes in a graph and returns the
    unqualified node for a collection, and the expected explanation strings
    for when pydough.explain is called on the unqualified node, both with and
    without verbose mode.
    """
    graph_name: str = request.param[0]
    test_impl: Callable[[GraphMetadata], Callable[[], UnqualifiedNode]] = request.param[
        1
    ]
    verbose_refsol: str = request.param[2]
    non_verbose_refsol: str = request.param[3]
    return graph_name, test_impl, verbose_refsol.strip(), non_verbose_refsol.strip()


@pytest.mark.parametrize(
    "verbose",
    [
        pytest.param(True, id="verbose"),
        pytest.param(False, id="non_verbose"),
    ],
)
def test_unqualified_node_exploration(
    unqualified_exploration_test_data: tuple[
        str, Callable[[GraphMetadata], Callable[[], UnqualifiedNode]], str, str
    ],
    verbose: bool,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Verifies that `pydough.explain` called on unqualified nodes produces the
    exepcted strings.
    """
    graph_name, test_impl, verbose_answer, non_verbose_answer = (
        unqualified_exploration_test_data
    )
    graph: GraphMetadata = get_sample_graph(graph_name)
    node: UnqualifiedNode = test_impl(graph)()
    answer: str = pydough.explain(node, verbose=verbose)
    expected_answer: str = verbose_answer if verbose else non_verbose_answer
    assert (
        answer == expected_answer
    ), "Mismatch between produced string and expected answer"


@pytest.fixture(
    params=[
        pytest.param(
            (
                "TPCH",
                nation_name_impl,
                """
""",
                """

""",
            ),
            id="nation-name",
        ),
    ]
)
def unqualified_term_exploration_test_data(
    request,
) -> tuple[
    str,
    Callable[[GraphMetadata], Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]]],
    str,
    str,
]:
    """
    Testing data used for test_unqualified_term_exploration. Returns a tuple of
    the graph name to use, a function that takes in a graph and returns the
    tuple of the unqualified node for a collection and a term within it, and
    the expected explanation strings for when pydough.explain is called on the
    unqualified node, both with and without verbose mode.
    """
    graph_name: str = request.param[0]
    test_impl: Callable[
        [GraphMetadata], Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]]
    ] = request.param[1]
    verbose_refsol: str = request.param[2]
    non_verbose_refsol: str = request.param[3]
    return graph_name, test_impl, verbose_refsol.strip(), non_verbose_refsol.strip()


@pytest.mark.parametrize(
    "verbose",
    [
        pytest.param(True, id="verbose"),
        pytest.param(False, id="non_verbose"),
    ],
)
def test_unqualified_term_exploration(
    unqualified_term_exploration_test_data: tuple[
        str,
        Callable[
            [GraphMetadata], Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]]
        ],
        str,
        str,
    ],
    verbose: bool,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Verifies that `pydough.explain` called on unqualified nodes produces the
    expected strings.
    """
    graph_name, test_impl, verbose_answer, non_verbose_answer = (
        unqualified_term_exploration_test_data
    )
    graph: GraphMetadata = get_sample_graph(graph_name)
    node, term = test_impl(graph)()
    answer: str = pydough.explain_term(node, term, verbose=verbose)
    expected_answer: str = verbose_answer if verbose else non_verbose_answer
    assert (
        answer == expected_answer
    ), "Mismatch between produced string and expected answer"
