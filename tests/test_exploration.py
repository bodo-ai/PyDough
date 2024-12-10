"""
TODO: add file-level docstring
"""

from collections.abc import Callable

import pytest
from test_utils import graph_fetcher

import pydough
from pydough.metadata import (
    GraphMetadata,
)


@pytest.fixture(
    params=[
        pytest.param(
            (
                ("Empty", None, None),
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
""",
            ),
            id="explain_graph_tpch",
        ),
        pytest.param(
            (
                ("TPCH", "Customers", None),
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
Call pydough.explain(graph['Customers'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_customers",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", None),
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
Call pydough.explain(graph['Regions'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_regions",
        ),
        pytest.param(
            (
                ("TPCH", "Lineitems", None),
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
Call pydough.explain(graph['Lineitems'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_lineitems",
        ),
        pytest.param(
            (
                ("TPCH", "PartSupp", None),
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
Call pydough.explain(graph['PartSupp'][property_name]) to learn more about any of these properties.
""",
            ),
            id="explain_collection_tpch_partsupp",
        ),
        pytest.param(
            (
                ("TPCH", "Regions", "key"),
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
def metadata_exploration_test_data(
    request,
) -> tuple[Callable[[graph_fetcher], str], str]:
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
    """
    args: tuple[str, str | None, str | None] = request.param[0]
    refsol: str = request.param[1]

    graph_name: str = args[0]
    collection_name: str | None = args[1]
    property_name: str | None = args[2]

    def wrapped_test_impl(fetcher: graph_fetcher):
        graph: GraphMetadata = fetcher(graph_name)
        if collection_name is None:
            return pydough.explain(graph)
        elif property_name is None:
            return pydough.explain(graph[collection_name])
        else:
            return pydough.explain(graph[collection_name][property_name])

    return wrapped_test_impl, refsol.strip()


def test_metadata_exploration(
    metadata_exploration_test_data: tuple[Callable[[graph_fetcher], str], str],
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Verifies that `pydough.explain` called on metadata produces the exepcted
    strings.
    """
    test_impl, answer = metadata_exploration_test_data
    explanation_string: str = test_impl(get_sample_graph)
    assert (
        explanation_string == answer
    ), "Mismatch between produced string and expected answer"


@pytest.fixture(
    params=[
        # pytest.param(
        #     (
        #         "TPCH",
        #         "\n"
        #         "\n"
        #         "\n",
        #         "",
        #         "\n"
        #         "\n"
        #         "\n"
        #     ),
        #     id="collection-explain_collection"
        # ),
        # pytest.param(
        #     (
        #         "TPCH",
        #         "\n"
        #         "\n"
        #         "\n",
        #         "",
        #         "\n"
        #         "\n"
        #         "\n"
        #     ),
        #     id="collection-explain_collection"
        # ),
        # pytest.param(
        #     (
        #         "TPCH",
        #         "\n"
        #         "\n"
        #         "\n",
        #         "",
        #         "\n"
        #         "\n"
        #         "\n"
        #     ),
        #     id="collection-explain_collection"
        # ),
    ]
)
def unqualified_exploration_test_data(request) -> tuple[str, str, str, str]:
    """
    Testing data used for test_unqualified_node_exploration. Returns a tuple of
    the graph name to use, the func text for the PyDough code to use, the term
    that should be explained from the code, and the expected explanation
    string.
    """
    return request.param


def test_unqualified_node_exploration(
    unqualified_exploration_test_data: tuple[str, str, str, str],
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Verifies that `pydough.explain` called on unqualified nodes produces the
    exepcted strings.
    """
    graph_name, func_text_body, explain_term, answer = unqualified_exploration_test_data
    graph: GraphMetadata = get_sample_graph(graph_name)
    func_text = "@pydough.init_pydough_context(graph)\n"
    func_text += "def impl():\n"
    func_text += "\n".join([" " + line for line in func_text_body.splitlines()])
    func_text += f" return pydough.explain({explain_term})\n"
    func_text += "answer = impl()"

    genv: dict[str, object] = {"pydough": pydough, "graph": graph}
    lenv: dict[str, object] = {}
    exec(func_text, genv, lenv)

    assert (
        lenv["answer"] == answer
    ), "Mismatch between produced string and expected answer"
