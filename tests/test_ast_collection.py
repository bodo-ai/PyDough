"""
TODO: add file-level docstring.
"""

import pytest
from test_utils import (
    BackReferenceCollectionInfo,
    BackReferenceExpressionInfo,
    CalcInfo,
    ChildReferenceInfo,
    CollectionTestInfo,
    FunctionInfo,
    LiteralInfo,
    OrderInfo,
    ReferenceInfo,
    SubCollectionInfo,
    TableCollectionInfo,
    WhereInfo,
)

from pydough.pydough_ast import AstNodeBuilder, PyDoughCollectionAST
from pydough.types import (
    Float64Type,
    Int64Type,
    StringType,
)


@pytest.fixture
def region_intra_ratio() -> tuple[CollectionTestInfo, str, str]:
    """
    The AST node info for a query that calculates the ratio for each region
    between the of all part sale values (retail price of the part times the
    quantity purchased for each time it was purchased) that were sold to a
    customer in the same region vs all part sale values from that region, only
    counting sales where the shipping mode was 'AIR'.

    Equivalent SQL query:
    ```
    SELECT
        R1.name AS region_name
        SUM(P.p_retailprice * IFF(R1.region_name == R2.region_name, L.quantity, 0)) /
        SUM(P.p_retailprice * L.quantity) AS intra_ratio
    FROM
        REGION R1,
        NATION N1,
        SUPPLIER S,
        PART_SUPP PS,
        LINEITEM L,
        ORDER O,
        CUSTOMER C,
        NATION N2,
        REGION R2
    WHERE R1.r_regionkey = N1.n_regionkey
        AND N1.n_nationkey = S.s_nationkey
        AND S.s_suppkey = PS.ps_suppkey
        AND PS.ps_partkey = P.p_partkey
        AND PS.ps_partkey = L.l_partkey AND PS.ps_suppkey = L.l_suppkey
        AND L.l_orderkey = O.o_orderkey
        AND O.o_custkey = C.c_custkey
        AND C.c_nationkey = N2.n_nationkey
        AND N2.n_regionkey = R2.r_regionkey
        AND L.l_shipmode = 'AIR'
    GROUP BY region_name
    ```

    Equivalent PyDough code:
    ```
    is_intra_line = IFF(order.customer.region.name == BACK(3).name, 1, 0)
    part_sales = suppliers.parts_supplied.ps_lines.WHERE(
        shipmode == 'AIR
    )(
        intra_value = BACK(1).retail_price * quantity * is_intra_line,
        total_value = BACK(1).retail_price * quantity,
    )
    Regions(
        region_name = name
        intra_sales = SUM(part_sales.intra_value) / SUM(part_sales.total_value)
    )
    ```
    """
    test_info: CollectionTestInfo = TableCollectionInfo("Regions") ** CalcInfo(
        [
            SubCollectionInfo("suppliers")
            ** SubCollectionInfo("parts_supplied")
            ** SubCollectionInfo("ps_lines")
            ** WhereInfo(
                [],
                FunctionInfo(
                    "EQU",
                    [ReferenceInfo("ship_mode"), LiteralInfo("AIR", StringType())],
                ),
            )
            ** CalcInfo(
                [
                    SubCollectionInfo("order")
                    ** SubCollectionInfo("customer")
                    ** SubCollectionInfo("region")
                ],
                value=FunctionInfo(
                    "MUL",
                    [
                        BackReferenceExpressionInfo("retail_price", 1),
                        ReferenceInfo("quantity"),
                    ],
                ),
                adj_value=FunctionInfo(
                    "MUL",
                    [
                        BackReferenceExpressionInfo("retail_price", 1),
                        FunctionInfo(
                            "MUL",
                            [
                                ReferenceInfo("quantity"),
                                FunctionInfo(
                                    "IFF",
                                    [
                                        FunctionInfo(
                                            "EQU",
                                            [
                                                BackReferenceExpressionInfo("name", 3),
                                                ChildReferenceInfo("name", 0),
                                            ],
                                        ),
                                        LiteralInfo(1, Int64Type()),
                                        LiteralInfo(0, Int64Type()),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            )
        ],
        region_name=ReferenceInfo("name"),
        intra_ratio=FunctionInfo(
            "DIV",
            [
                FunctionInfo("SUM", [ChildReferenceInfo("adj_value", 0)]),
                FunctionInfo("SUM", [ChildReferenceInfo("value", 0)]),
            ],
        ),
    )
    adjustment: str = "IFF(BACK(3).name == order.customer.region.name, 1, 0)"
    base_value: str = "BACK(1).retail_price * quantity"
    adjusted_value: str = f"BACK(1).retail_price * (quantity * {adjustment})"
    part_values: str = f"suppliers.parts_supplied.ps_lines.WHERE(ship_mode == 'AIR')(value={base_value}, adj_value={adjusted_value})"
    string_representation: str = f"Regions(region_name=name, intra_ratio=SUM({part_values}.adj_value) / SUM({part_values}.value))"
    tree_string_representation: str = """\
──┬─ TPCH
  ├─── TableCollection[Regions]
  └─┬─ Calc[region_name=name, intra_ratio=SUM($1.adj_value) / SUM($1.value)]
    └─┬─ CalcSubCollection
      └─┬─ SubCollection[suppliers]
        └─┬─ SubCollection[parts_supplied]
          ├─── SubCollection[ps_lines]
          ├─── Where[ship_mode == 'AIR']
          └─┬─ Calc[value=BACK(1).retail_price * quantity, adj_value=BACK(1).retail_price * (quantity * IFF(BACK(3).name == $1.name, 1, 0))]
            └─┬─ CalcSubCollection
              └─┬─ SubCollection[order]
                └─┬─ SubCollection[customer]
                  └─── SubCollection[region]\
"""
    return test_info, string_representation, tree_string_representation


@pytest.mark.parametrize(
    "calc_pipeline, expected_calcs, expected_total_names",
    [
        pytest.param(
            CalcInfo([], x=LiteralInfo(1, Int64Type()), y=LiteralInfo(3, Int64Type())),
            {"x": 0, "y": 1},
            {
                "x",
                "y",
                "Customers",
                "Lineitems",
                "Nations",
                "Orders",
                "PartSupp",
                "Parts",
                "Regions",
                "Suppliers",
            },
            id="global_calc",
        ),
        pytest.param(
            CalcInfo(
                [
                    TableCollectionInfo("Suppliers"),
                    TableCollectionInfo("Parts")
                    ** SubCollectionInfo("lines")
                    ** CalcInfo(
                        [],
                        value=FunctionInfo(
                            "MUL", [ReferenceInfo("ps_availqty"), ReferenceInfo("tax")]
                        ),
                    ),
                ],
                n_balance=FunctionInfo(
                    "SUM", [ChildReferenceInfo("account_balance", 0)]
                ),
                t_value=FunctionInfo("SUM", [ChildReferenceInfo("value", 1)]),
            ),
            {"n_balance": 0, "t_value": 1},
            {
                "n_balance",
                "t_value",
                "Customers",
                "Lineitems",
                "Nations",
                "Orders",
                "PartSupp",
                "Parts",
                "Regions",
                "Suppliers",
            },
            id="global_nested_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions"),
            {"key": 0, "name": 1, "comment": 2},
            {
                "name",
                "key",
                "comment",
                "nations",
                "customers",
                "lines_sourced_from",
                "orders_shipped_to",
                "suppliers",
            },
            id="regions",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("nations"),
            {"key": 0, "name": 1, "region_key": 2, "comment": 3},
            {
                "name",
                "key",
                "region_key",
                "comment",
                "region",
                "customers",
                "suppliers",
                "orders_shipped_to",
            },
            id="regions_nations",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo(
                [],
            ),
            {},
            {
                "name",
                "key",
                "comment",
                "nations",
                "customers",
                "lines_sourced_from",
                "orders_shipped_to",
                "suppliers",
            },
            id="regions_empty_calc",
        ),
        pytest.param(
            (
                TableCollectionInfo("Regions")
                ** CalcInfo(
                    [], foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name")
                )
            ),
            {"foo": 0, "bar": 1},
            {
                "name",
                "key",
                "comment",
                "nations",
                "customers",
                "lines_sourced_from",
                "orders_shipped_to",
                "suppliers",
                "foo",
                "bar",
            },
            id="regions_calc",
        ),
        pytest.param(
            (
                TableCollectionInfo("Regions")
                ** CalcInfo(
                    [], foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name")
                )
                ** SubCollectionInfo("nations")
            ),
            {"key": 0, "name": 1, "region_key": 2, "comment": 3},
            {
                "name",
                "key",
                "region_key",
                "comment",
                "customers",
                "region",
                "orders_shipped_to",
                "suppliers",
            },
            id="regions_calc_nations",
        ),
        pytest.param(
            (
                TableCollectionInfo("Regions")
                ** SubCollectionInfo("nations")
                ** CalcInfo(
                    [], foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name")
                )
            ),
            {"foo": 0, "bar": 1},
            {
                "name",
                "key",
                "region_key",
                "comment",
                "customers",
                "region",
                "orders_shipped_to",
                "suppliers",
                "foo",
                "bar",
            },
            id="regions_nations_calc",
        ),
        pytest.param(
            (
                TableCollectionInfo("Regions")
                ** CalcInfo(
                    [], foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name")
                )
                ** CalcInfo(
                    [],
                    fizz=FunctionInfo(
                        "ADD", [ReferenceInfo("foo"), LiteralInfo(1, Int64Type())]
                    ),
                    buzz=FunctionInfo("LOWER", [ReferenceInfo("name")]),
                )
            ),
            {"fizz": 0, "buzz": 1},
            {
                "name",
                "key",
                "comment",
                "nations",
                "customers",
                "lines_sourced_from",
                "orders_shipped_to",
                "suppliers",
                "foo",
                "bar",
                "fizz",
                "buzz",
            },
            id="regions_calc_calc",
        ),
        pytest.param(
            TableCollectionInfo("Parts") ** SubCollectionInfo("suppliers_of_part"),
            {
                "key": 0,
                "name": 1,
                "address": 2,
                "nation_key": 3,
                "phone": 4,
                "account_balance": 5,
                "comment": 6,
                "ps_availqty": 7,
                "ps_supplycost": 8,
                "ps_comment": 9,
            },
            {
                "key",
                "name",
                "address",
                "nation_key",
                "phone",
                "account_balance",
                "comment",
                "lines",
                "supply_records",
                "lines",
                "parts_supplied",
                "nation",
                "region",
                "ps_availqty",
                "ps_comment",
                "ps_supplycost",
                "ps_lines",
            },
            id="parts_suppliers",
        ),
        pytest.param(
            (
                TableCollectionInfo("Parts")
                ** SubCollectionInfo("suppliers_of_part")
                ** CalcInfo(
                    [],
                    good_comment=FunctionInfo(
                        "EQU",
                        [ReferenceInfo("comment"), LiteralInfo("good", StringType())],
                    ),
                    negative_balance=FunctionInfo(
                        "LET",
                        [
                            ReferenceInfo("account_balance"),
                            LiteralInfo(0.0, Float64Type()),
                        ],
                    ),
                )
            ),
            {"good_comment": 0, "negative_balance": 1},
            {
                "key",
                "name",
                "address",
                "nation_key",
                "phone",
                "account_balance",
                "comment",
                "lines",
                "supply_records",
                "lines",
                "parts_supplied",
                "nation",
                "region",
                "ps_availqty",
                "ps_comment",
                "ps_supplycost",
                "ps_lines",
                "good_comment",
                "negative_balance",
            },
            id="parts_suppliers_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("suppliers"),
            {
                "key": 0,
                "name": 1,
                "address": 2,
                "nation_key": 3,
                "phone": 4,
                "account_balance": 5,
                "comment": 6,
                "nation_name": 7,
            },
            {
                "key",
                "name",
                "address",
                "nation_key",
                "phone",
                "account_balance",
                "comment",
                "lines",
                "supply_records",
                "lines",
                "parts_supplied",
                "nation",
                "region",
                "nation_name",
            },
            id="regions_suppliers",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("lines_sourced_from"),
            {
                "order_key": 0,
                "part_key": 1,
                "supplier_key": 2,
                "line_number": 3,
                "quantity": 4,
                "extended_price": 5,
                "discount": 6,
                "tax": 7,
                "status": 8,
                "ship_date": 9,
                "commit_date": 10,
                "receipt_date": 11,
                "ship_instruct": 12,
                "ship_mode": 13,
                "comment": 14,
                "nation_name": 15,
                "ps_availqty": 16,
                "ps_supplycost": 17,
                "ps_comment": 18,
                "supplier_name": 19,
                "supplier_address": 20,
            },
            {
                "order_key",
                "part_key",
                "supplier_key",
                "line_number",
                "quantity",
                "extended_price",
                "discount",
                "tax",
                "status",
                "ship_date",
                "commit_date",
                "receipt_date",
                "ship_instruct",
                "ship_mode",
                "comment",
                "part_and_supplier",
                "order",
                "supplier",
                "part",
                "nation_name",
                "ps_part",
                "ps_availqty",
                "ps_supplycost",
                "ps_comment",
                "supplier_name",
                "supplier_address",
                "other_parts_supplied",
                "supplier_region",
            },
            id="regions_lines",
        ),
        pytest.param(
            (
                TableCollectionInfo("Regions")
                ** WhereInfo(
                    [],
                    FunctionInfo(
                        "NEQ",
                        [ReferenceInfo("name"), LiteralInfo("ASIA", StringType())],
                    ),
                )
                ** SubCollectionInfo("nations")
                ** WhereInfo(
                    [],
                    FunctionInfo(
                        "NEQ", [ReferenceInfo("name"), LiteralInfo("USA", StringType())]
                    ),
                )
                ** CalcInfo(
                    [],
                    region_name=BackReferenceExpressionInfo("name", 1),
                    nation_name=ReferenceInfo("name"),
                )
            ),
            {"region_name": 0, "nation_name": 1},
            {
                "region_name",
                "nation_name",
                "name",
                "key",
                "region_key",
                "comment",
                "customers",
                "region",
                "orders_shipped_to",
                "suppliers",
            },
            id="regions_nations_backcalc",
        ),
        pytest.param(
            (
                TableCollectionInfo("Regions")
                ** SubCollectionInfo("nations")
                ** SubCollectionInfo("suppliers")
                ** SubCollectionInfo("supply_records")
                ** SubCollectionInfo("lines")
                ** CalcInfo(
                    [],
                    region_name=BackReferenceExpressionInfo("name", 4),
                    nation_name=BackReferenceExpressionInfo("name", 3),
                    supplier_name=BackReferenceExpressionInfo("name", 2),
                    date=ReferenceInfo("ship_date"),
                )
                ** WhereInfo(
                    [],
                    FunctionInfo(
                        "EQU",
                        [ReferenceInfo("ship_mode"), LiteralInfo("AIR", StringType())],
                    ),
                )
            ),
            {"region_name": 0, "nation_name": 1, "supplier_name": 2, "date": 3},
            {
                "region_name",
                "nation_name",
                "supplier_name",
                "date",
                "order_key",
                "part_key",
                "supplier_key",
                "line_number",
                "quantity",
                "extended_price",
                "discount",
                "tax",
                "status",
                "ship_date",
                "commit_date",
                "receipt_date",
                "ship_instruct",
                "ship_mode",
                "comment",
                "part_and_supplier",
                "order",
                "supplier",
                "part",
                "supplier_region",
            },
            id="regions_nations_suppliers_ps_lines_backcalc",
        ),
        pytest.param(
            (
                TableCollectionInfo("Regions")
                ** SubCollectionInfo("lines_sourced_from")
                ** CalcInfo(
                    [],
                    source_region_name=BackReferenceExpressionInfo("name", 1),
                    taxation=ReferenceInfo("tax"),
                    name_of_nation=ReferenceInfo("nation_name"),
                )
            ),
            {"source_region_name": 0, "taxation": 1, "name_of_nation": 2},
            {
                "source_region_name",
                "taxation",
                "name_of_nation",
                "order_key",
                "part_key",
                "supplier_key",
                "line_number",
                "quantity",
                "extended_price",
                "discount",
                "tax",
                "status",
                "ship_date",
                "commit_date",
                "receipt_date",
                "ship_instruct",
                "ship_mode",
                "comment",
                "part_and_supplier",
                "order",
                "supplier",
                "part",
                "nation_name",
                "ps_part",
                "ps_availqty",
                "ps_supplycost",
                "ps_comment",
                "supplier_name",
                "supplier_address",
                "other_parts_supplied",
                "supplier_region",
            },
            id="regions_lines_backcalc",
        ),
    ],
)
def test_collections_calc_terms(
    calc_pipeline: CollectionTestInfo,
    expected_calcs: dict[str, int],
    expected_total_names: set[str],
    tpch_node_builder: AstNodeBuilder,
):
    """
    Tests that a sequence of collection-producing AST nodes results in the
    correct calc terms & total set of available terms.
    """
    collection: PyDoughCollectionAST = calc_pipeline.build(tpch_node_builder)
    assert collection.calc_terms == set(
        expected_calcs
    ), "Mismatch between set of calc terms and expected value"
    actual_calcs: dict[str, int] = {
        expr: collection.get_expression_position(expr) for expr in collection.calc_terms
    }
    assert (
        actual_calcs == expected_calcs
    ), "Mismatch between positions of calc terms and expected value"
    assert (
        collection.all_terms == expected_total_names
    ), "Mismatch between set of all terms and expected value"


@pytest.mark.parametrize(
    "calc_pipeline, expected_string, expected_tree_string",
    [
        pytest.param(
            CalcInfo([], x=LiteralInfo(1, Int64Type()), y=LiteralInfo(3, Int64Type())),
            "TPCH(x=1, y=3)",
            """\
┌─── TPCH
└─── Calc[x=1, y=3]\
""",
            id="global_calc",
        ),
        pytest.param(
            CalcInfo(
                [
                    TableCollectionInfo("Suppliers"),
                    TableCollectionInfo("Parts")
                    ** SubCollectionInfo("lines")
                    ** CalcInfo(
                        [],
                        value=FunctionInfo(
                            "MUL", [ReferenceInfo("ps_availqty"), ReferenceInfo("tax")]
                        ),
                    ),
                ],
                n_balance=FunctionInfo(
                    "SUM", [ChildReferenceInfo("account_balance", 0)]
                ),
                t_value=FunctionInfo("SUM", [ChildReferenceInfo("value", 1)]),
            ),
            "TPCH(n_balance=SUM(Suppliers.account_balance), t_value=SUM(Parts.lines(value=ps_availqty * tax).value))",
            """\
┌─── TPCH
└─┬─ Calc[n_balance=SUM($1.account_balance), t_value=SUM($2.value)]
  ├─┬─ CalcSubCollection
  │ └─── TableCollection[Suppliers]
  └─┬─ CalcSubCollection
    └─┬─ TableCollection[Parts]
      ├─── SubCollection[lines]
      └─── Calc[value=ps_availqty * tax]\
""",
            id="global_nested_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions"),
            "Regions",
            """\
──┬─ TPCH
  └─── TableCollection[Regions]\
""",
            id="regions",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("nations"),
            "Regions.nations",
            """\
──┬─ TPCH
  └─┬─ TableCollection[Regions]
    └─── SubCollection[nations]\
""",
            id="regions_nations",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("nations"),
            "Regions.nations",
            """\
──┬─ TPCH
  └─┬─ TableCollection[Regions]
    └─── SubCollection[nations]\
""",
            id="regions_filter_nations_where",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo(
                [],
            ),
            "Regions()",
            """\
──┬─ TPCH
  ├─── TableCollection[Regions]
  └─── Calc[]\
""",
            id="regions_empty_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo(
                [],
                region_name=ReferenceInfo("name"),
                adjusted_key=FunctionInfo(
                    "MUL",
                    [
                        FunctionInfo(
                            "SUB", [ReferenceInfo("key"), LiteralInfo(1, Int64Type())]
                        ),
                        LiteralInfo(2, Int64Type()),
                    ],
                ),
            ),
            "Regions(region_name=name, adjusted_key=(key - 1) * 2)",
            """\
──┬─ TPCH
  ├─── TableCollection[Regions]
  └─── Calc[region_name=name, adjusted_key=(key - 1) * 2]\
""",
            id="regions_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** WhereInfo(
                [],
                FunctionInfo(
                    "NEQ", [ReferenceInfo("name"), LiteralInfo("ASIA", StringType())]
                ),
            )
            ** SubCollectionInfo("nations")
            ** WhereInfo(
                [],
                FunctionInfo(
                    "NEQ", [ReferenceInfo("name"), LiteralInfo("USA", StringType())]
                ),
            )
            ** CalcInfo(
                [],
                region_name=BackReferenceExpressionInfo("name", 1),
                nation_name=ReferenceInfo("name"),
            ),
            "Regions.WHERE(name != 'ASIA').nations.WHERE(name != 'USA')(region_name=BACK(1).name, nation_name=name)",
            """\
──┬─ TPCH
  ├─── TableCollection[Regions]
  └─┬─ Where[name != 'ASIA']
    ├─── SubCollection[nations]
    ├─── Where[name != 'USA']
    └─── Calc[region_name=BACK(1).name, nation_name=name]\
""",
            id="regions_nations_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("suppliers")
            ** CalcInfo(
                [],
                region_name=BackReferenceExpressionInfo("name", 1),
                nation_name=ReferenceInfo("nation_name"),
                supplier_name=ReferenceInfo("name"),
            ),
            "Regions.suppliers(region_name=BACK(1).name, nation_name=nation_name, supplier_name=name)",
            """\
──┬─ TPCH
  └─┬─ TableCollection[Regions]
    ├─── SubCollection[suppliers]
    └─── Calc[region_name=BACK(1).name, nation_name=nation_name, supplier_name=name]\
""",
            id="regions_suppliers_calc",
        ),
        pytest.param(
            TableCollectionInfo("Parts")
            ** SubCollectionInfo("suppliers_of_part")
            ** SubCollectionInfo("ps_lines"),
            "Parts.suppliers_of_part.ps_lines",
            """\
──┬─ TPCH
  └─┬─ TableCollection[Parts]
    └─┬─ SubCollection[suppliers_of_part]
      └─── SubCollection[ps_lines]\
""",
            id="parts_suppliers_lines",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [SubCollectionInfo("suppliers")],
                nation_name=ReferenceInfo("name"),
                total_supplier_balances=FunctionInfo(
                    "SUM", [ChildReferenceInfo("account_balance", 0)]
                ),
            ),
            "Nations(nation_name=name, total_supplier_balances=SUM(suppliers.account_balance))",
            """\
──┬─ TPCH
  ├─── TableCollection[Nations]
  └─┬─ Calc[nation_name=name, total_supplier_balances=SUM($1.account_balance)]
    └─┬─ CalcSubCollection
      └─── SubCollection[suppliers]\
""",
            id="nations_childcalc_suppliers",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo([], adj_name=FunctionInfo("LOWER", [ReferenceInfo("name")]))
            ** SubCollectionInfo("nations")
            ** CalcInfo(
                [],
                region_name=BackReferenceExpressionInfo("adj_name", 1),
                nation_name=ReferenceInfo("name"),
            ),
            "Regions(adj_name=LOWER(name)).nations(region_name=BACK(1).adj_name, nation_name=name)",
            """\
──┬─ TPCH
  ├─── TableCollection[Regions]
  └─┬─ Calc[adj_name=LOWER(name)]
    ├─── SubCollection[nations]
    └─── Calc[region_name=BACK(1).adj_name, nation_name=name]\
""",
            id="regions_calc_nations_calc",
        ),
        pytest.param(
            TableCollectionInfo("Suppliers")
            ** CalcInfo(
                [SubCollectionInfo("parts_supplied")],
                supplier_name=ReferenceInfo("name"),
                total_retail_price=FunctionInfo(
                    "SUM",
                    [
                        FunctionInfo(
                            "SUB",
                            [
                                ChildReferenceInfo("retail_price", 0),
                                LiteralInfo(1.0, Float64Type()),
                            ],
                        )
                    ],
                ),
            ),
            "Suppliers(supplier_name=name, total_retail_price=SUM(parts_supplied.retail_price - 1.0))",
            """\
──┬─ TPCH
  ├─── TableCollection[Suppliers]
  └─┬─ Calc[supplier_name=name, total_retail_price=SUM($1.retail_price - 1.0)]
    └─┬─ CalcSubCollection
      └─── SubCollection[parts_supplied]\
""",
            id="suppliers_childcalc_parts_a",
        ),
        pytest.param(
            TableCollectionInfo("Suppliers")
            ** CalcInfo(
                [
                    SubCollectionInfo("parts_supplied")
                    ** CalcInfo(
                        [],
                        adj_retail_price=FunctionInfo(
                            "SUB",
                            [
                                ReferenceInfo("retail_price"),
                                LiteralInfo(1.0, Float64Type()),
                            ],
                        ),
                    )
                ],
                supplier_name=ReferenceInfo("name"),
                total_retail_price=FunctionInfo(
                    "SUM", [ChildReferenceInfo("adj_retail_price", 0)]
                ),
            ),
            "Suppliers(supplier_name=name, total_retail_price=SUM(parts_supplied(adj_retail_price=retail_price - 1.0).adj_retail_price))",
            """\
──┬─ TPCH
  ├─── TableCollection[Suppliers]
  └─┬─ Calc[supplier_name=name, total_retail_price=SUM($1.adj_retail_price)]
    └─┬─ CalcSubCollection
      ├─── SubCollection[parts_supplied]
      └─── Calc[adj_retail_price=retail_price - 1.0]\
""",
            id="suppliers_childcalc_parts_b",
        ),
        pytest.param(
            TableCollectionInfo("Suppliers")
            ** SubCollectionInfo("parts_supplied")
            ** CalcInfo(
                [
                    SubCollectionInfo("ps_lines"),
                    BackReferenceCollectionInfo("nation", 1)
                    ** CalcInfo([], nation_name=ReferenceInfo("name")),
                ],
                nation_name=ChildReferenceInfo("nation_name", 1),
                supplier_name=BackReferenceExpressionInfo("name", 1),
                part_name=ReferenceInfo("name"),
                ratio=FunctionInfo(
                    "DIV",
                    [ChildReferenceInfo("quantity", 0), ReferenceInfo("ps_availqty")],
                ),
            ),
            "Suppliers.parts_supplied(nation_name=BACK(1).nation(nation_name=name).nation_name, supplier_name=BACK(1).name, part_name=name, ratio=ps_lines.quantity / ps_availqty)",
            """\
──┬─ TPCH
  └─┬─ TableCollection[Suppliers]
    ├─── SubCollection[parts_supplied]
    └─┬─ Calc[nation_name=$2.nation_name, supplier_name=BACK(1).name, part_name=name, ratio=$1.quantity / ps_availqty]
      ├─┬─ CalcSubCollection
      │ └─── SubCollection[ps_lines]
      └─┬─ CalcSubCollection
        ├─── SubCollection[BACK(1).nation]
        └─── Calc[nation_name=name]\
""",
            id="suppliers_parts_childcalc_a",
        ),
        pytest.param(
            TableCollectionInfo("Suppliers")
            ** SubCollectionInfo("parts_supplied")
            ** CalcInfo(
                [
                    SubCollectionInfo("ps_lines")
                    ** CalcInfo(
                        [],
                        ratio=FunctionInfo(
                            "DIV",
                            [
                                ReferenceInfo("quantity"),
                                BackReferenceExpressionInfo("ps_availqty", 1),
                            ],
                        ),
                    ),
                    BackReferenceCollectionInfo("nation", 1),
                ],
                nation_name=ChildReferenceInfo("name", 1),
                supplier_name=BackReferenceExpressionInfo("name", 1),
                part_name=ReferenceInfo("name"),
                ratio=ChildReferenceInfo("ratio", 0),
            ),
            "Suppliers.parts_supplied(nation_name=BACK(1).nation.name, supplier_name=BACK(1).name, part_name=name, ratio=ps_lines(ratio=quantity / BACK(1).ps_availqty).ratio)",
            """\
──┬─ TPCH
  └─┬─ TableCollection[Suppliers]
    ├─── SubCollection[parts_supplied]
    └─┬─ Calc[nation_name=$2.name, supplier_name=BACK(1).name, part_name=name, ratio=$1.ratio]
      ├─┬─ CalcSubCollection
      │ ├─── SubCollection[ps_lines]
      │ └─── Calc[ratio=quantity / BACK(1).ps_availqty]
      └─┬─ CalcSubCollection
        └─── SubCollection[BACK(1).nation]\
""",
            id="suppliers_parts_childcalc_b",
        ),
        pytest.param(
            (
                CalcInfo(
                    [TableCollectionInfo("Customers")],
                    total_balance=FunctionInfo(
                        "SUM", [ChildReferenceInfo("acctbal", 0)]
                    ),
                )
            ),
            "TPCH(total_balance=SUM(Customers.acctbal))",
            """\
┌─── TPCH
└─┬─ Calc[total_balance=SUM($1.acctbal)]
  └─┬─ CalcSubCollection
    └─── TableCollection[Customers]\
""",
            id="globalcalc_a",
        ),
        pytest.param(
            CalcInfo(
                [
                    TableCollectionInfo("Customers"),
                    TableCollectionInfo("Suppliers")
                    ** SubCollectionInfo("parts_supplied")
                    ** CalcInfo(
                        [],
                        value=FunctionInfo(
                            "MUL",
                            [
                                ReferenceInfo("ps_availqty"),
                                ReferenceInfo("retail_price"),
                            ],
                        ),
                    ),
                ],
                total_demand=FunctionInfo("SUM", [ChildReferenceInfo("acctbal", 0)]),
                total_supply=FunctionInfo("SUM", [ChildReferenceInfo("value", 1)]),
            ),
            "TPCH(total_demand=SUM(Customers.acctbal), total_supply=SUM(Suppliers.parts_supplied(value=ps_availqty * retail_price).value))",
            """\
┌─── TPCH
└─┬─ Calc[total_demand=SUM($1.acctbal), total_supply=SUM($2.value)]
  ├─┬─ CalcSubCollection
  │ └─── TableCollection[Customers]
  └─┬─ CalcSubCollection
    └─┬─ TableCollection[Suppliers]
      ├─── SubCollection[parts_supplied]
      └─── Calc[value=ps_availqty * retail_price]\
""",
            id="globalcalc_b",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** OrderInfo([], (ReferenceInfo("name"), True, True)),
            "Nations.ORDER_BY(name.ASC(na_pos='last'))",
            """\
──┬─ TPCH
  ├─── TableCollection[Nations]
  └─── OrderBy[name.ASC(na_pos='last')]\
""",
            id="nations_ordering",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** OrderInfo(
                [SubCollectionInfo("suppliers")],
                (
                    FunctionInfo("SUM", [ChildReferenceInfo("account_balance", 0)]),
                    False,
                    True,
                ),
                (ReferenceInfo("name"), True, True),
            ),
            "Nations.ORDER_BY(SUM(suppliers.account_balance).DESC(na_pos='last'), name.ASC(na_pos='last'))",
            """\
──┬─ TPCH
  ├─── TableCollection[Nations]
  └─┬─ OrderBy[SUM($1.account_balance).DESC(na_pos='last'), name.ASC(na_pos='last')]
    └─┬─ CalcSubCollection
      └─── SubCollection[suppliers]\
""",
            id="nations_nested_ordering",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("nations")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** SubCollectionInfo("customers")
            ** OrderInfo([], (ReferenceInfo("acctbal"), True, True)),
            "Regions.ORDER_BY(name.ASC(na_pos='last')).nations.ORDER_BY(key.ASC(na_pos='last')).customers.ORDER_BY(acctbal.ASC(na_pos='last'))",
            """\
──┬─ TPCH
  ├─── TableCollection[Regions]
  └─┬─ OrderBy[name.ASC(na_pos='last')]
    ├─── SubCollection[nations]
    └─┬─ OrderBy[key.ASC(na_pos='last')]
      ├─── SubCollection[customers]
      └─── OrderBy[acctbal.ASC(na_pos='last')]\
""",
            id="regions_nations_customers_order",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("customers")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** WhereInfo(
                [],
                FunctionInfo(
                    "GRT", [ReferenceInfo("acctbal"), LiteralInfo(1000, Int64Type())]
                ),
            )
            ** CalcInfo([], region_name=BackReferenceExpressionInfo("name", 1))
            ** OrderInfo([], (ReferenceInfo("region_name"), True, True))
            ** WhereInfo(
                [],
                FunctionInfo(
                    "NEQ",
                    [ReferenceInfo("region_name"), LiteralInfo("ASIA", StringType())],
                ),
            ),
            "Regions.ORDER_BY(name.ASC(na_pos='last')).customers.ORDER_BY(key.ASC(na_pos='last')).WHERE(acctbal > 1000)(region_name=BACK(1).name).ORDER_BY(region_name.ASC(na_pos='last')).WHERE(region_name != 'ASIA')",
            """\
──┬─ TPCH
  ├─── TableCollection[Regions]
  └─┬─ OrderBy[name.ASC(na_pos='last')]
    ├─── SubCollection[customers]
    ├─── OrderBy[key.ASC(na_pos='last')]
    ├─── Where[acctbal > 1000]
    ├─── Calc[region_name=BACK(1).name]
    ├─── OrderBy[region_name.ASC(na_pos='last')]
    └─── Where[region_name != 'ASIA']\
""",
            id="regions_customers_order_where_calc_order_where",
        ),
    ],
)
def test_collections_to_string(
    calc_pipeline: CollectionTestInfo,
    expected_string: str,
    expected_tree_string: str,
    tpch_node_builder: AstNodeBuilder,
):
    """
    Verifies that various AST collection node structures produce the expected
    non-tree string representation.
    """
    collection: PyDoughCollectionAST = calc_pipeline.build(tpch_node_builder)
    assert (
        collection.to_string() == expected_string
    ), "Mismatch between non-tree string representation and expected value"
    assert (
        collection.to_tree_string() == expected_tree_string
    ), "Mismatch between tree string representation and expected value"


@pytest.mark.parametrize(
    "calc_pipeline, expected_collation_strings",
    [
        pytest.param(
            CalcInfo([], x=LiteralInfo(1, Int64Type()), y=LiteralInfo(3, Int64Type())),
            None,
            id="global_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("suppliers")
            ** CalcInfo(
                [],
                region_name=BackReferenceExpressionInfo("name", 1),
                nation_name=ReferenceInfo("nation_name"),
                supplier_name=ReferenceInfo("name"),
            ),
            None,
            id="regions_suppliers_calc",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** OrderInfo([], (ReferenceInfo("name"), True, True)),
            ["name.ASC(na_pos='last')"],
            id="nations_ordering",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** OrderInfo(
                [SubCollectionInfo("suppliers")],
                (
                    FunctionInfo("SUM", [ChildReferenceInfo("account_balance", 0)]),
                    False,
                    True,
                ),
                (ReferenceInfo("name"), True, True),
            ),
            [
                "SUM(suppliers.account_balance).DESC(na_pos='last')",
                "name.ASC(na_pos='last')",
            ],
            id="nations_nested_ordering",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("nations")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** SubCollectionInfo("customers")
            ** OrderInfo([], (ReferenceInfo("acctbal"), True, True)),
            ["acctbal.ASC(na_pos='last')"],
            id="regions_nations_customers_order",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("nations")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** SubCollectionInfo("customers"),
            None,
            id="regions_nations_customers",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("customers")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** WhereInfo(
                [],
                FunctionInfo(
                    "GRT", [ReferenceInfo("acctbal"), LiteralInfo(1000, Int64Type())]
                ),
            )
            ** CalcInfo([], region_name=BackReferenceExpressionInfo("name", 1))
            ** OrderInfo([], (ReferenceInfo("region_name"), True, True))
            ** WhereInfo(
                [],
                FunctionInfo(
                    "NEQ",
                    [ReferenceInfo("region_name"), LiteralInfo("ASIA", StringType())],
                ),
            ),
            ["region_name.ASC(na_pos='last')"],
            id="regions_customers_order_where_calc_order_where",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("customers")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** WhereInfo(
                [],
                FunctionInfo(
                    "GRT", [ReferenceInfo("acctbal"), LiteralInfo(1000, Int64Type())]
                ),
            )
            ** CalcInfo([], region_name=BackReferenceExpressionInfo("name", 1))
            ** WhereInfo(
                [],
                FunctionInfo(
                    "NEQ",
                    [ReferenceInfo("region_name"), LiteralInfo("ASIA", StringType())],
                ),
            ),
            ["key.ASC(na_pos='last')"],
            id="regions_customers_order_where_calc_where",
        ),
    ],
)
def test_collections_ordering(
    calc_pipeline: CollectionTestInfo,
    expected_collation_strings: list[str] | None,
    tpch_node_builder: AstNodeBuilder,
):
    """
    Verifies that various AST collection node structures have the expected
    collation nodes to order by.
    """
    collection: PyDoughCollectionAST = calc_pipeline.build(tpch_node_builder)
    if expected_collation_strings is None:
        assert (
            collection.ordering is None
        ), "expected collection to not have an ordering, but it did have one"
    else:
        assert (
            collection.ordering is not None
        ), "expected collection to have an ordering, but it did not"
        collation_strings: list[str] = [
            collation.to_string() for collation in collection.ordering
        ]
        assert (
            collation_strings == expected_collation_strings
        ), "Mismatch between string representation of collation keys and expected value"


def test_regions_intra_ratio_string_order(
    region_intra_ratio: tuple[CollectionTestInfo, str, str],
    tpch_node_builder: AstNodeBuilder,
):
    """
    Same as `test_collections_to_string` and `test_collections_ordering`, but
    specifically on the structure from the `region_intra_ratio` fixture.
    """
    calc_pipeline, expected_string, expected_tree_string = region_intra_ratio
    collection: PyDoughCollectionAST = calc_pipeline.build(tpch_node_builder)
    assert collection.to_string() == expected_string
    assert collection.to_tree_string() == expected_tree_string
    assert collection.ordering is None
