"""
Unit tests for the PyDough exploration APIs.
"""

from collections.abc import Callable

import pytest

import pydough
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode
from tests.test_pydough_functions.exploration_examples import (
    calc_subcollection_impl,
    contextless_aggfunc_impl,
    contextless_collections_impl,
    contextless_expr_impl,
    contextless_func_impl,
    customers_without_orders_impl,
    filter_impl,
    global_agg_calc_impl,
    global_calc_impl,
    global_impl,
    lineitems_arithmetic_impl,
    nation_expr_impl,
    nation_impl,
    nation_name_impl,
    nation_region_impl,
    nation_region_name_impl,
    nations_lowercase_name_impl,
    order_by_impl,
    partition_child_impl,
    partition_impl,
    parts_avg_price_child_impl,
    parts_avg_price_impl,
    parts_with_german_supplier,
    region_n_suppliers_in_red_impl,
    region_nations_back_name,
    region_nations_suppliers_impl,
    region_nations_suppliers_name_impl,
    subcollection_calc_backref_impl,
    suppliers_iff_balance_impl,
    table_calc_impl,
    top_k_impl,
)
from tests.testing_utilities import graph_fetcher


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
  customers
  lines
  nations
  orders
  parts
  regions
  suppliers
  supply_records
Call pydough.explain(graph[collection_name]) to learn more about any of these collections.
Call pydough.explain_structure(graph) to see how all of the collections in the graph are connected.
""",
                """
PyDough graph: TPCH
Collections: customers, lines, nations, orders, parts, regions, suppliers, supply_records
Call pydough.explain(graph[collection_name]) to learn more about any of these collections.
Call pydough.explain_structure(graph) to see how all of the collections in the graph are connected.
""",
            ),
            id="explain_graph_tpch",
        ),
        pytest.param(
            (
                ("TPCH", "customers", None),
                """
PyDough collection: customers
Table path: tpch.CUSTOMER
Unique properties of collection: ['key', 'name']
Scalar properties:
  account_balance
  address
  comment
  key
  market_segment
  name
  nation_key
  phone
Subcollection properties:
  nation
  orders
Call pydough.explain(graph['customers'][property_name]) to learn more about any of these properties.
""",
                """
PyDough collection: customers
Scalar properties: account_balance, address, comment, key, market_segment, name, nation_key, phone
Subcollection properties: nation, orders
Call pydough.explain(graph['customers'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_customers",
        ),
        pytest.param(
            (
                ("TPCH", "regions", None),
                """
PyDough collection: regions
Table path: tpch.REGION
Unique properties of collection: ['key']
Scalar properties:
  comment
  key
  name
Subcollection properties:
  nations
Call pydough.explain(graph['regions'][property_name]) to learn more about any of these properties.
""",
                """
PyDough collection: regions
Scalar properties: comment, key, name
Subcollection properties: nations
Call pydough.explain(graph['regions'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_regions",
        ),
        pytest.param(
            (
                ("TPCH", "lines", None),
                """
PyDough collection: lines
Table path: tpch.LINEITEM
Unique properties of collection: [['order_key', 'line_number']]
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
Call pydough.explain(graph['lines'][property_name]) to learn more about any of these properties.
""",
                """
PyDough collection: lines
Scalar properties: comment, commit_date, discount, extended_price, line_number, order_key, part_key, quantity, receipt_date, return_flag, ship_date, ship_instruct, ship_mode, status, supplier_key, tax
Subcollection properties: order, part, part_and_supplier, supplier
Call pydough.explain(graph['lines'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_lineitems",
        ),
        pytest.param(
            (
                ("TPCH", "supply_records", None),
                """
PyDough collection: supply_records
Table path: tpch.PARTSUPP
Unique properties of collection: [['part_key', 'supplier_key']]
Scalar properties:
  available_quantity
  comment
  part_key
  supplier_key
  supply_cost
Subcollection properties:
  lines
  part
  supplier
Call pydough.explain(graph['supply_records'][property_name]) to learn more about any of these properties.
""",
                """
PyDough collection: supply_records
Scalar properties: available_quantity, comment, part_key, supplier_key, supply_cost
Subcollection properties: lines, part, supplier
Call pydough.explain(graph['supply_records'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_partsupp",
        ),
        pytest.param(
            (
                ("TPCH", "regions", "key"),
                """
PyDough property: regions.key
Column name: tpch.REGION.r_regionkey
Data type: numeric
""",
                """
PyDough property: regions.key
Column name: tpch.REGION.r_regionkey
Data type: numeric
""",
            ),
            id="explain_property_tpch_regions_key",
        ),
        pytest.param(
            (
                ("TPCH", "regions", "name"),
                """
PyDough property: regions.name
Column name: tpch.REGION.r_name
Data type: string
""",
                """
PyDough property: regions.name
Column name: tpch.REGION.r_name
Data type: string
""",
            ),
            id="explain_property_tpch_regions_name",
        ),
        pytest.param(
            (
                ("TPCH", "regions", "comment"),
                """
PyDough property: regions.comment
Column name: tpch.REGION.r_comment
Data type: string
""",
                """
PyDough property: regions.comment
Column name: tpch.REGION.r_comment
Data type: string
""",
            ),
            id="explain_property_tpch_regions_comment",
        ),
        pytest.param(
            (
                ("TPCH", "regions", "nations"),
                """
PyDough property: regions.nations
This property connects collection regions to nations.
Cardinality of connection: plural
The subcollection relationship is defined by the following join conditions:
    regions.key == nations.region_key
""",
                """
PyDough property: regions.nations
This property connects collection regions to nations.
Use pydough.explain(graph['regions']['nations'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_regions_nations",
        ),
        pytest.param(
            (
                ("TPCH", "supply_records", "part"),
                """
PyDough property: supply_records.part
This property connects collection supply_records to parts.
Cardinality of connection: singular
The subcollection relationship is defined by the following join conditions:
    supply_records.part_key == parts.key
""",
                """
PyDough property: supply_records.part
This property connects collection supply_records to parts.
Use pydough.explain(graph['supply_records']['part'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_partsupp_part",
        ),
        pytest.param(
            (
                ("TPCH", "supply_records", "supplier"),
                """
PyDough property: supply_records.supplier
This property connects collection supply_records to suppliers.
Cardinality of connection: singular
The subcollection relationship is defined by the following join conditions:
    supply_records.supplier_key == suppliers.key
""",
                """
PyDough property: supply_records.supplier
This property connects collection supply_records to suppliers.
Use pydough.explain(graph['supply_records']['supplier'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_partsupp_supplier",
        ),
        pytest.param(
            (
                ("TPCH", "supply_records", "lines"),
                """
PyDough property: supply_records.lines
This property connects collection supply_records to lines.
Cardinality of connection: plural
The subcollection relationship is defined by the following join conditions:
    supply_records.part_key == lines.part_key
    supply_records.supplier_key == lines.supplier_key
""",
                """
PyDough property: supply_records.lines
This property connects collection supply_records to lines.
Use pydough.explain(graph['supply_records']['lines'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_partsupp_lines",
        ),
        pytest.param(
            (
                ("TPCH", "suppliers", "nation"),
                """
PyDough property: suppliers.nation
This property connects collection suppliers to nations.
Cardinality of connection: singular
The subcollection relationship is defined by the following join conditions:
    suppliers.nation_key == nations.key
""",
                """
PyDough property: suppliers.nation
This property connects collection suppliers to nations.
Use pydough.explain(graph['suppliers']['nation'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_suppliers_nation",
        ),
        pytest.param(
            (
                ("TPCH", "suppliers", "supply_records"),
                """
PyDough property: suppliers.supply_records
This property connects collection suppliers to supply_records.
Cardinality of connection: plural
The subcollection relationship is defined by the following join conditions:
    suppliers.key == supply_records.supplier_key
""",
                """
PyDough property: suppliers.supply_records
This property connects collection suppliers to supply_records.
Use pydough.explain(graph['suppliers']['supply_records'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_suppliers_supply_records",
        ),
        pytest.param(
            (
                ("TPCH", "suppliers", "lines"),
                """
PyDough property: suppliers.lines
This property connects collection suppliers to lines.
Cardinality of connection: plural
The subcollection relationship is defined by the following join conditions:
    suppliers.key == lines.supplier_key
""",
                """
PyDough property: suppliers.lines
This property connects collection suppliers to lines.
Use pydough.explain(graph['suppliers']['lines'], verbose=True) to learn more details.
""",
            ),
            id="explain_property_tpch_suppliers_lines",
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
    assert explanation_string == answer, (
        "Mismatch between produced string and expected answer"
    )


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

  customers
  ├── account_balance
  ├── address
  ├── comment
  ├── key
  ├── market_segment
  ├── name
  ├── nation_key
  ├── phone
  ├── nation [one member of nations]
  └── orders [multiple orders]

  lines
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
  ├── order [one member of orders]
  ├── part [one member of parts]
  ├── part_and_supplier [one member of supply_records]
  └── supplier [one member of suppliers]

  nations
  ├── comment
  ├── key
  ├── name
  ├── region_key
  ├── customers [multiple customers]
  ├── region [one member of regions]
  └── suppliers [multiple suppliers]

  orders
  ├── clerk
  ├── comment
  ├── customer_key
  ├── key
  ├── order_date
  ├── order_priority
  ├── order_status
  ├── ship_priority
  ├── total_price
  ├── customer [one member of customers]
  └── lines [multiple lines]

  parts
  ├── brand
  ├── comment
  ├── container
  ├── key
  ├── manufacturer
  ├── name
  ├── part_type
  ├── retail_price
  ├── size
  ├── lines [multiple lines]
  └── supply_records [multiple supply_records]

  regions
  ├── comment
  ├── key
  ├── name
  └── nations [multiple nations]

  suppliers
  ├── account_balance
  ├── address
  ├── comment
  ├── key
  ├── name
  ├── nation_key
  ├── phone
  ├── lines [multiple lines]
  ├── nation [one member of nations]
  └── supply_records [multiple supply_records]

  supply_records
  ├── available_quantity
  ├── comment
  ├── part_key
  ├── supplier_key
  ├── supply_cost
  ├── lines [multiple lines]
  ├── part [one member of parts]
  └── supplier [one member of suppliers]
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
    assert structure_string == answer.strip(), (
        "Mismatch between produced string and expected answer"
    )


@pytest.fixture(
    params=[
        pytest.param(
            (
                "TPCH",
                nation_impl,
                """
PyDough collection representing the following logic:
  ──┬─ TPCH
    └─── TableCollection[nations]

This node, specifically, accesses the collection nations.
Call pydough.explain(graph['nations']) to learn more about this collection.

The following terms will be included in the result if this collection is executed:
  comment, key, name, region_key

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
""",
                """
This node, specifically, accesses the collection nations.
Call pydough.explain(graph['nations']) to learn more about this collection.

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

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

This node is a reference to the global context for the entire graph. An operation must be done onto this node (e.g. a CALCULATE or accessing a collection) before it can be executed.

The collection does not have any terms that can be included in a result if it is executed.

The collection has access to the following collections:
  customers, lines, nations, orders, parts, regions, suppliers, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
""",
                """
This node is a reference to the global context for the entire graph. An operation must be done onto this node (e.g. a CALCULATE or accessing a collection) before it can be executed.

The collection has access to the following collections:
  customers, lines, nations, orders, parts, regions, suppliers, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

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
  └─── Calculate[x=42, y=13]

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  x <- 42
  y <- 13

The following terms will be included in the result if this collection is executed:
  x, y

The collection has access to the following expressions:
  x, y

The collection has access to the following collections:
  customers, lines, nations, orders, parts, regions, suppliers, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
""",
                """
The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  x <- 42
  y <- 13

The collection has access to the following expressions:
  x, y

The collection has access to the following collections:
  customers, lines, nations, orders, parts, regions, suppliers, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

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
  └─┬─ Calculate[n_customers=COUNT($1), avg_part_price=AVG($2.retail_price)]
    ├─┬─ AccessChild
    │ └─── TableCollection[customers]
    └─┬─ AccessChild
      └─── TableCollection[parts]

This node first derives the following children before doing its main task:
  child $1:
    └─── TableCollection[customers]
  child $2:
    └─── TableCollection[parts]

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  avg_part_price <- AVG($2.retail_price), aka AVG(parts.retail_price)
  n_customers <- COUNT($1), aka COUNT(customers)

The following terms will be included in the result if this collection is executed:
  avg_part_price, n_customers

The collection has access to the following expressions:
  avg_part_price, n_customers

The collection has access to the following collections:
  customers, lines, nations, orders, parts, regions, suppliers, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
""",
                """
This node first derives the following children before doing its main task:
  child $1: customers
  child $2: parts

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  avg_part_price <- AVG($2.retail_price), aka AVG(parts.retail_price)
  n_customers <- COUNT($1), aka COUNT(customers)

The collection has access to the following expressions:
  avg_part_price, n_customers

The collection has access to the following collections:
  customers, lines, nations, orders, parts, regions, suppliers, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

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
    ├─── TableCollection[nations]
    └─┬─ Calculate[name=name, region_name=$1.name, num_customers=COUNT($2)]
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

The collection has access to the following expressions:
  comment, key, name, num_customers, region_key, region_name

The collection has access to the following collections:
  customers, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
                """,
                """
This node first derives the following children before doing its main task:
  child $1: region
  child $2: customers

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  name <- name (propagated from previous collection)
  num_customers <- COUNT($2), aka COUNT(customers)
  region_name <- $1.name, aka region.name

The collection has access to the following expressions:
  comment, key, name, num_customers, region_key, region_name

The collection has access to the following collections:
  customers, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

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
    ├─── TableCollection[regions]
    └─┬─ Calculate[region_name=name]
      ├─── SubCollection[nations]
      └─┬─ Calculate[nation_name=name]
        ├─── SubCollection[customers]
        └─── Calculate[name=name, nation_name=nation_name, region_name=region_name]

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  name <- name (propagated from previous collection)
  nation_name <- nation_name (propagated from previous collection)
  region_name <- region_name (propagated from previous collection)

The following terms will be included in the result if this collection is executed:
  name, nation_name, region_name

The collection has access to the following expressions:
  account_balance, address, comment, key, market_segment, name, nation_key, nation_name, phone, region_name

The collection has access to the following collections:
  nation, orders

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
                """,
                """
The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  name <- name (propagated from previous collection)
  nation_name <- nation_name (propagated from previous collection)
  region_name <- region_name (propagated from previous collection)

The collection has access to the following expressions:
  account_balance, address, comment, key, market_segment, name, nation_key, nation_name, phone, region_name

The collection has access to the following collections:
  nation, orders

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
                """,
            ),
            id="subcollection_calc_backref",
        ),
        pytest.param(
            (
                "TPCH",
                calc_subcollection_impl,
                """
PyDough collection representing the following logic:
  ──┬─ TPCH
    ├─── TableCollection[nations]
    └─┬─ Calculate[nation_name=name]
      └─── SubCollection[region]

This node, specifically, accesses the subcollection nations.region. Call pydough.explain(graph['nations']['region']) to learn more about this subcollection property.

The following terms will be included in the result if this collection is executed:
  comment, key, name

The collection has access to the following expressions:
  comment, key, name, nation_name

The collection has access to the following collections:
  nations

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
                """,
                """
This node, specifically, accesses the subcollection nations.region. Call pydough.explain(graph['nations']['region']) to learn more about this subcollection property.

The collection has access to the following expressions:
  comment, key, name, nation_name

The collection has access to the following collections:
  nations

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
                """,
            ),
            id="calc_subcollection",
        ),
        pytest.param(
            (
                "TPCH",
                filter_impl,
                """
PyDough collection representing the following logic:
  ──┬─ TPCH
    ├─── TableCollection[nations]
    ├─── Calculate[nation_name=name]
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
  nation_name

The collection has access to the following expressions:
  comment, key, name, nation_name, region_key

The collection has access to the following collections:
  customers, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
                """,
                """
This node first derives the following children before doing its main task:
  child $1: region
  child $2: customers.orders.lines.WHERE(CONTAINS(part.name, 'STEEL'))
  child $3: suppliers.WHERE(account_balance >= 0.0)

The main task of this node is to filter on the following conditions:
  $1.name == 'ASIA', aka region.name == 'ASIA'
  HAS($2), aka HAS(customers.orders.lines.WHERE(CONTAINS(part.name, 'STEEL')))
  COUNT($3) > 100, aka COUNT(suppliers.WHERE(account_balance >= 0.0)) > 100

The collection has access to the following expressions:
  comment, key, name, nation_name, region_key

The collection has access to the following collections:
  customers, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

Call pydough.explain(collection, verbose=True) for more details.
                """,
            ),
            id="filter",
        ),
        pytest.param(
            (
                "TPCH",
                order_by_impl,
                """
PyDough collection representing the following logic:
  ──┬─ TPCH
    ├─── TableCollection[nations]
    ├─── Calculate[name=name]
    └─┬─ OrderBy[COUNT($1).DESC(na_pos='last'), name.ASC(na_pos='first')]
      └─┬─ AccessChild
        └─── SubCollection[suppliers]

This node first derives the following children before doing its main task:
  child $1:
    └─── SubCollection[suppliers]

The main task of this node is to sort the collection on the following:
  COUNT($1), aka COUNT(suppliers), in descending order with nulls at the end
  with ties broken by: name, in ascending order with nulls at the start

The following terms will be included in the result if this collection is executed:
  name

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
                """,
                """
This node first derives the following children before doing its main task:
  child $1: suppliers

The main task of this node is to sort the collection on the following:
  COUNT($1), aka COUNT(suppliers), in descending order with nulls at the end
  with ties broken by: name, in ascending order with nulls at the start

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

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
    ├─── TableCollection[parts]
    ├─┬─ Calculate[name=name, n_suppliers=COUNT($1)]
    │ └─┬─ AccessChild
    │   └─── SubCollection[supply_records]
    └─── TopK[100, n_suppliers.DESC(na_pos='last'), name.ASC(na_pos='first')]

The main task of this node is to sort the collection on the following and keep the first 100 records:
  n_suppliers, in descending order with nulls at the end
  with ties broken by: name, in ascending order with nulls at the start

The following terms will be included in the result if this collection is executed:
  n_suppliers, name

The collection has access to the following expressions:
  brand, comment, container, key, manufacturer, n_suppliers, name, part_type, retail_price, size

The collection has access to the following collections:
  lines, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
                """,
                """
The main task of this node is to sort the collection on the following and keep the first 100 records:
  n_suppliers, in descending order with nulls at the end
  with ties broken by: name, in ascending order with nulls at the start

The collection has access to the following expressions:
  brand, comment, container, key, manufacturer, n_suppliers, name, part_type, retail_price, size

The collection has access to the following collections:
  lines, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

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
  ──┬─ TPCH
    └─┬─ Partition[name='part_types', by=part_type]
      └─┬─ AccessChild
        └─── TableCollection[parts]

This node first derives the following children before doing its main task:
  child $1:
    └─── TableCollection[parts]

The main task of this node is to partition the child data on the following keys:
  $1.part_type
Note: the subcollection of this collection containing records from the unpartitioned data is called 'parts'.

The following terms will be included in the result if this collection is executed:
  part_type

The collection has access to the following expressions:
  part_type

The collection has access to the following collections:
  parts

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
                """,
                """
This node first derives the following children before doing its main task:
  child $1: parts

The main task of this node is to partition the child data on the following keys:
  $1.part_type
Note: the subcollection of this collection containing records from the unpartitioned data is called 'parts'.

The collection has access to the following expressions:
  part_type

The collection has access to the following collections:
  parts

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

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
  ──┬─ TPCH
    ├─┬─ Partition[name='part_types', by=part_type]
    │ └─┬─ AccessChild
    │   └─── TableCollection[parts]
    ├─┬─ Calculate[avg_price=AVG($1.retail_price)]
    │ └─┬─ AccessChild
    │   └─── PartitionChild[parts]
    └─┬─ Where[avg_price >= 27.5]
      └─── PartitionChild[parts]

This node, specifically, accesses the unpartitioned data of a partitioning (child name: parts).

The following terms will be included in the result if this collection is executed:
  brand, comment, container, key, manufacturer, name, part_type, retail_price, size

The collection has access to the following expressions:
  avg_price, brand, comment, container, key, manufacturer, name, part_type, retail_price, size

The collection has access to the following collections:
  lines, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
                """,
                """
This node, specifically, accesses the unpartitioned data of a partitioning (child name: parts).

The collection has access to the following expressions:
  avg_price, brand, comment, container, key, manufacturer, name, part_type, retail_price, size

The collection has access to the following collections:
  lines, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.

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
If pydough.explain is called on an unqualified PyDough code, it is expected to
be a collection, but instead received the following expression:
 TPCH.nations.name
Did you mean to use pydough.explain_term?
                """,
                """
If pydough.explain is called on an unqualified PyDough code, it is expected to
be a collection, but instead received the following expression:
 TPCH.nations.name
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
Unrecognized term of TPCH: 'line_items'. Did you mean: lines, parts, regions?
This could mean you accessed a property using a name that does not exist, or
that you need to place your PyDough code into a context for it to make sense.
Did you mean to use pydough.explain_term?
                """,
                """
Unrecognized term of TPCH: 'line_items'. Did you mean: lines, parts, regions?
This could mean you accessed a property using a name that does not exist, or
that you need to place your PyDough code into a context for it to make sense.
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
Unrecognized term of TPCH: 'name'. Did you mean: lines, parts, nations, orders?
This could mean you accessed a property using a name that does not exist, or
that you need to place your PyDough code into a context for it to make sense.
Did you mean to use pydough.explain_term?
                """,
                """
Unrecognized term of TPCH: 'name'. Did you mean: lines, parts, nations, orders?
This could mean you accessed a property using a name that does not exist, or
that you need to place your PyDough code into a context for it to make sense.
Did you mean to use pydough.explain_term?
                """,
            ),
            id="not_qualified_collection_c",
        ),
        pytest.param(
            (
                "TPCH",
                contextless_aggfunc_impl,
                """
Cannot call pydough.explain on COUNT(customers).
Did you mean to use pydough.explain_term?
""",
                """
Cannot call pydough.explain on COUNT(customers).
Did you mean to use pydough.explain_term?
""",
            ),
            id="not_qualified_collection_d",
        ),
        pytest.param(
            (
                "TPCH",
                contextless_func_impl,
                """
Cannot call pydough.explain on LOWER(((first_name + ' ') + last_name)).
Did you mean to use pydough.explain_term?
""",
                """
Cannot call pydough.explain on LOWER(((first_name + ' ') + last_name)).
Did you mean to use pydough.explain_term?
""",
            ),
            id="not_qualified_collection_e",
        ),
    ]
)
def unqualified_exploration_test_data(
    request,
) -> tuple[str, Callable[[], UnqualifiedNode], str, str]:
    """
    Testing data used for test_unqualified_node_exploration. Returns a tuple of
    the graph name to use, a function that takes in a graph and returns the
    unqualified node for a collection, and the expected explanation strings
    for when pydough.explain is called on the unqualified node, both with and
    without verbose mode.
    """
    graph_name: str = request.param[0]
    test_impl: Callable[[], UnqualifiedNode] = request.param[1]
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
        str, Callable[[], UnqualifiedNode], str, str
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
    node: UnqualifiedNode = pydough.init_pydough_context(graph)(test_impl)()
    answer: str = pydough.explain(node, verbose=verbose)
    expected_answer: str = verbose_answer if verbose else non_verbose_answer
    assert answer == expected_answer, (
        "Mismatch between produced string and expected answer"
    )


@pytest.fixture(
    params=[
        pytest.param(
            (
                "TPCH",
                nation_name_impl,
                """
Collection:
  ──┬─ TPCH
    └─── TableCollection[nations]

The term is the following expression: name

This is column 'name' of collection 'nations'

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.nations.CALCULATE(name)
""",
                """
Collection: TPCH.nations

The term is the following expression: name

This is column 'name' of collection 'nations'
""",
            ),
            id="nation-name",
        ),
        pytest.param(
            (
                "TPCH",
                nation_region_name_impl,
                """
Collection:
  ──┬─ TPCH
    └─── TableCollection[nations]

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1:
    └─── SubCollection[region]

The term is the following expression: $1.name

This is a reference to expression 'name' of child $1

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.nations.CALCULATE(region.name)
""",
                """
Collection: TPCH.nations

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1: region

The term is the following expression: $1.name

This is a reference to expression 'name' of child $1
""",
            ),
            id="nation-region_name",
        ),
        pytest.param(
            (
                "TPCH",
                nation_region_impl,
                """
Collection:
  ──┬─ TPCH
    └─── TableCollection[nations]

The term is the following child of the collection:
  └─┬─ AccessChild
    └─── SubCollection[region]

This child is singular with regards to the collection, meaning its scalar terms can be accessed by the collection as if they were scalar terms of the expression.
For example, the following is valid:
  TPCH.nations.CALCULATE(region.comment)

To learn more about this child, you can try calling pydough.explain on the following:
  TPCH.nations.region
""",
                """
Collection: TPCH.nations

The term is the following child of the collection:
  region
""",
            ),
            id="nation-region",
        ),
        pytest.param(
            (
                "TPCH",
                region_nations_suppliers_impl,
                """
Collection:
  ──┬─ TPCH
    └─── TableCollection[regions]

The term is the following child of the collection:
  └─┬─ AccessChild
    └─┬─ SubCollection[nations]
      └─── SubCollection[suppliers]

This child is plural with regards to the collection, meaning its scalar terms can only be accessed by the collection if they are aggregated.
For example, the following are valid:
  TPCH.regions.CALCULATE(COUNT(nations.suppliers.account_balance))
  TPCH.regions.WHERE(HAS(nations.suppliers))
  TPCH.regions.ORDER_BY(COUNT(nations.suppliers).DESC())

To learn more about this child, you can try calling pydough.explain on the following:
  TPCH.regions.nations.suppliers
""",
                """
Collection: TPCH.regions

The term is the following child of the collection:
  nations.suppliers
""",
            ),
            id="region-nations_suppliers",
        ),
        pytest.param(
            (
                "TPCH",
                region_nations_suppliers_name_impl,
                """
Collection:
  ──┬─ TPCH
    └─── TableCollection[regions]

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1:
    └─┬─ SubCollection[nations]
      └─── SubCollection[suppliers]

The term is the following expression: $1.name

This is a reference to expression 'name' of child $1

This expression is plural with regards to the collection, meaning it can be placed in a CALCULATE of a collection if it is aggregated.
For example, the following is valid:
  TPCH.regions.CALCULATE(COUNT(nations.suppliers.name))
""",
                """
Collection: TPCH.regions

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1: nations.suppliers

The term is the following expression: $1.name

This is a reference to expression 'name' of child $1
""",
            ),
            id="region-nations_suppliers_name",
        ),
        pytest.param(
            (
                "TPCH",
                region_nations_back_name,
                """
Collection:
  ──┬─ TPCH
    ├─── TableCollection[regions]
    └─┬─ Calculate[region_name=name]
      └─── SubCollection[nations]

The term is the following expression: region_name

This is a reference to expression 'region_name' of the 1st ancestor of the collection, which is the following:
  ──┬─ TPCH
    ├─── TableCollection[regions]
    └─── Calculate[region_name=name]

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.regions.CALCULATE(region_name=name).nations.CALCULATE(region_name)
""",
                """
Collection: TPCH.regions.CALCULATE(region_name=name).nations

The term is the following expression: region_name

This is a reference to expression 'region_name' of the 1st ancestor of the collection, which is the following:
  TPCH.regions.CALCULATE(region_name=name)
""",
            ),
            id="region_nations-back_name",
        ),
        pytest.param(
            (
                "TPCH",
                region_n_suppliers_in_red_impl,
                """
Collection:
  ──┬─ TPCH
    └─── TableCollection[regions]

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1:
    └─┬─ SubCollection[nations]
      ├─── SubCollection[suppliers]
      └─── Where[account_balance > 0]

The term is the following expression: COUNT($1)

This expression counts how many records of the following subcollection exist for each record of the collection:
  nations.suppliers.WHERE(account_balance > 0)

Call pydough.explain_term with this collection and any of the arguments to learn more about them.

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.regions.CALCULATE(COUNT(nations.suppliers.WHERE(account_balance > 0)))
        """,
                """
Collection: TPCH.regions

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1: nations.suppliers.WHERE(account_balance > 0)

The term is the following expression: COUNT($1)

This expression counts how many records of the following subcollection exist for each record of the collection:
  nations.suppliers.WHERE(account_balance > 0)

Call pydough.explain_term with this collection and any of the arguments to learn more about them.
        """,
            ),
            id="region-n_suppliers_in_red",
        ),
        pytest.param(
            (
                "TPCH",
                parts_avg_price_impl,
                """
Collection:
  ──┬─ TPCH
    └─┬─ Partition[name='part_types', by=part_type]
      └─┬─ AccessChild
        └─── TableCollection[parts]

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1:
    └─── PartitionChild[parts]

The term is the following expression: AVG($1.retail_price)

This expression calls the function 'AVG' on the following arguments, aggregating them into a single value for each record of the collection:
  parts.retail_price

Call pydough.explain_term with this collection and any of the arguments to learn more about them.

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.Partition(parts, name='part_types', by=part_type).CALCULATE(AVG(parts.retail_price))
        """,
                """
Collection: TPCH.Partition(parts, name='part_types', by=part_type)

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1: parts

The term is the following expression: AVG($1.retail_price)

This expression calls the function 'AVG' on the following arguments, aggregating them into a single value for each record of the collection:
  parts.retail_price

Call pydough.explain_term with this collection and any of the arguments to learn more about them.
        """,
            ),
            id="agg_parts-aggfunc",
        ),
        pytest.param(
            (
                "TPCH",
                parts_avg_price_child_impl,
                """
Collection:
  ──┬─ TPCH
    ├─┬─ Partition[name='part_types', by=part_type]
    │ └─┬─ AccessChild
    │   └─── TableCollection[parts]
    └─┬─ Where[AVG($1.retail_price) >= 27.5]
      └─┬─ AccessChild
        └─── PartitionChild[parts]

The term is the following child of the collection:
  └─┬─ AccessChild
    └─── PartitionChild[parts]

This child is plural with regards to the collection, meaning its scalar terms can only be accessed by the collection if they are aggregated.
For example, the following are valid:
  TPCH.Partition(parts, name='part_types', by=part_type).WHERE(AVG(parts.retail_price) >= 27.5).CALCULATE(COUNT(parts.brand))
  TPCH.Partition(parts, name='part_types', by=part_type).WHERE(AVG(parts.retail_price) >= 27.5).WHERE(HAS(parts))
  TPCH.Partition(parts, name='part_types', by=part_type).WHERE(AVG(parts.retail_price) >= 27.5).ORDER_BY(COUNT(parts).DESC())

To learn more about this child, you can try calling pydough.explain on the following:
  TPCH.Partition(parts, name='part_types', by=part_type).WHERE(AVG(parts.retail_price) >= 27.5).parts
        """,
                """
Collection: TPCH.Partition(parts, name='part_types', by=part_type).WHERE(AVG(parts.retail_price) >= 27.5)

The term is the following child of the collection:
  parts
        """,
            ),
            id="agg_parts-child",
        ),
        pytest.param(
            (
                "TPCH",
                nations_lowercase_name_impl,
                """
Collection:
  ──┬─ TPCH
    └─── TableCollection[nations]

The term is the following expression: LOWER(name)

This expression calls the function 'LOWER' on the following arguments:
  name

Call pydough.explain_term with this collection and any of the arguments to learn more about them.

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.nations.CALCULATE(LOWER(name))
        """,
                """
Collection: TPCH.nations

The term is the following expression: LOWER(name)

This expression calls the function 'LOWER' on the following arguments:
  name

Call pydough.explain_term with this collection and any of the arguments to learn more about them.
        """,
            ),
            id="nations-lowercase_name",
        ),
        pytest.param(
            (
                "TPCH",
                lineitems_arithmetic_impl,
                """
Collection:
  ──┬─ TPCH
    └─── TableCollection[lines]

The term is the following expression: extended_price * (1 - discount)

This expression combines the following arguments with the '*' operator:
  extended_price
  1 - discount

Call pydough.explain_term with this collection and any of the arguments to learn more about them.

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.lines.CALCULATE(extended_price * (1 - discount))
        """,
                """
Collection: TPCH.lines

The term is the following expression: extended_price * (1 - discount)

This expression combines the following arguments with the '*' operator:
  extended_price
  1 - discount

Call pydough.explain_term with this collection and any of the arguments to learn more about them.
        """,
            ),
            id="lineitems-arithmetic",
        ),
        pytest.param(
            (
                "TPCH",
                suppliers_iff_balance_impl,
                """
Collection:
  ──┬─ TPCH
    └─── TableCollection[suppliers]

The term is the following expression: IFF(account_balance < 0, 0, account_balance)

This expression calls the function 'IFF' on the following arguments:
  account_balance < 0
  0
  account_balance

Call pydough.explain_term with this collection and any of the arguments to learn more about them.

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.suppliers.CALCULATE(IFF(account_balance < 0, 0, account_balance))
        """,
                """
Collection: TPCH.suppliers

The term is the following expression: IFF(account_balance < 0, 0, account_balance)

This expression calls the function 'IFF' on the following arguments:
  account_balance < 0
  0
  account_balance

Call pydough.explain_term with this collection and any of the arguments to learn more about them.
        """,
            ),
            id="suppliers-iff_balance",
        ),
        pytest.param(
            (
                "TPCH",
                customers_without_orders_impl,
                """
Collection:
  ──┬─ TPCH
    └─── TableCollection[customers]

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1:
    └─── SubCollection[orders]

The term is the following expression: HASNOT($1)

This expression returns whether the collection does not have any records of the following subcollection:
  orders

Call pydough.explain_term with this collection and any of the arguments to learn more about them.

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.customers.CALCULATE(HASNOT(orders))
        """,
                """
Collection: TPCH.customers

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1: orders

The term is the following expression: HASNOT($1)

This expression returns whether the collection does not have any records of the following subcollection:
  orders

Call pydough.explain_term with this collection and any of the arguments to learn more about them.
        """,
            ),
            id="customers-without_orders",
        ),
        pytest.param(
            (
                "TPCH",
                parts_with_german_supplier,
                """
Collection:
  ──┬─ TPCH
    └─── TableCollection[parts]

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1:
    └─┬─ SubCollection[supply_records]
      ├─── SubCollection[supplier]
      └─┬─ Where[$1.name == 'GERMANY']
        └─┬─ AccessChild
          └─── SubCollection[nation]

The term is the following expression: HAS($1)

This expression returns whether the collection has any records of the following subcollection:
  supply_records.supplier.WHERE(nation.name == 'GERMANY')

Call pydough.explain_term with this collection and any of the arguments to learn more about them.

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.parts.CALCULATE(HAS(supply_records.supplier.WHERE(nation.name == 'GERMANY')))
        """,
                """
Collection: TPCH.parts

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1: supply_records.supplier.WHERE(nation.name == 'GERMANY')

The term is the following expression: HAS($1)

This expression returns whether the collection has any records of the following subcollection:
  supply_records.supplier.WHERE(nation.name == 'GERMANY')

Call pydough.explain_term with this collection and any of the arguments to learn more about them.
        """,
            ),
            id="customers-with_german_supplier",
        ),
    ]
)
def unqualified_term_exploration_test_data(
    request,
) -> tuple[
    str,
    Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]],
    str,
    str,
]:
    """
    Testing data used for test_unqualified_term_exploration. Returns a tuple of
    the graph name to use, a function that, when decorated by pydough returns a
    tuple of the unqualified node for a collection and a term within it, and
    the expected explanation strings for when pydough.explain is called on the
    unqualified node, both with and without verbose mode.
    """
    graph_name: str = request.param[0]
    test_impl: Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]] = request.param[1]
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
        Callable[[], tuple[UnqualifiedNode, UnqualifiedNode]],
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
    node, term = pydough.init_pydough_context(graph)(test_impl)()
    answer: str = pydough.explain_term(node, term, verbose=verbose)
    expected_answer: str = verbose_answer if verbose else non_verbose_answer
    assert answer == expected_answer, (
        "Mismatch between produced string and expected answer"
    )
