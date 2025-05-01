"""
Unit tests for PyDough QDAG nodes for collections.
"""

import pytest
from test_utils import (
    BackReferenceExpressionInfo,
    CalculateInfo,
    ChildReferenceCollectionInfo,
    ChildReferenceExpressionInfo,
    CollectionTestInfo,
    FunctionInfo,
    LiteralInfo,
    OrderInfo,
    PartitionInfo,
    ReferenceInfo,
    SubCollectionInfo,
    TableCollectionInfo,
    TopKInfo,
    WhereInfo,
    WindowInfo,
)

from pydough.qdag import AstNodeBuilder, PyDoughCollectionQDAG
from pydough.types import (
    NumericType,
    StringType,
)


@pytest.fixture
def region_intra_pct() -> tuple[CollectionTestInfo, str, str]:
    """
    The QDAG node info for a query that calculates, for each region, the
    percentage of all part sale values (retail price of the part times the
    total quantity purchased for each time it was purchased) that were sold to
    a customer in the same region vs all part sale values from that region,
    only counting sales where the shipping mode was 'AIR'.

    Equivalent SQL query:
    ```
    SELECT
        R1.name AS region_name
        10.0 * SUM(P.p_retailprice * (R1.region_name == R2.region_name)) /
        SUM(P.p_retailprice * L.quantity) AS intra_pct
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
    part_sales = nations.suppliers.supply_records.CALCULATE(
        retail_price=part.retail_price
    ).lines.WHERE(
        shipmode == 'AIR
    ).CALCULATE(
        is_intra = order.customer.nation.region.name == region_name.name,
        value = retail_price * quantity,
    )
    result = regions.CALCULATE(region_name = name).CALCULATE(
        region_name,
        intra_pct = 100.0 * SUM(part_sales.value * part_sales.is_intra) / SUM(part_sales.value)
    )
    ```
    """
    test_info: CollectionTestInfo = (
        TableCollectionInfo("regions")
        ** CalculateInfo([], region_name=ReferenceInfo("name"))
        ** CalculateInfo(
            [
                SubCollectionInfo("nations")
                ** SubCollectionInfo("suppliers")
                ** SubCollectionInfo("supply_records")
                ** CalculateInfo(
                    [SubCollectionInfo("part")],
                    retail_price=ChildReferenceExpressionInfo("retail_price", 0),
                )
                ** SubCollectionInfo("lines")
                ** WhereInfo(
                    [],
                    FunctionInfo(
                        "EQU",
                        [ReferenceInfo("ship_mode"), LiteralInfo("AIR", StringType())],
                    ),
                )
                ** CalculateInfo(
                    [
                        SubCollectionInfo("order")
                        ** SubCollectionInfo("customer")
                        ** SubCollectionInfo("nation")
                        ** SubCollectionInfo("region")
                    ],
                    is_intra=FunctionInfo(
                        "EQU",
                        [
                            ChildReferenceExpressionInfo("name", 0),
                            BackReferenceExpressionInfo("region_name", 4),
                        ],
                    ),
                    value=FunctionInfo(
                        "MUL",
                        [
                            BackReferenceExpressionInfo("retail_price", 1),
                            ReferenceInfo("quantity"),
                        ],
                    ),
                )
            ],
            region_name=ReferenceInfo("region_name"),
            intra_pct=FunctionInfo(
                "MUL",
                [
                    LiteralInfo(100.0, NumericType()),
                    FunctionInfo(
                        "DIV",
                        [
                            FunctionInfo(
                                "SUM",
                                [
                                    FunctionInfo(
                                        "MUL",
                                        [
                                            ChildReferenceExpressionInfo("value", 0),
                                            ChildReferenceExpressionInfo("is_intra", 0),
                                        ],
                                    )
                                ],
                            ),
                            FunctionInfo(
                                "SUM", [ChildReferenceExpressionInfo("value", 0)]
                            ),
                        ],
                    ),
                ],
            ),
        )
    )
    is_intra: str = "order.customer.region.name == region_name"
    base_value: str = "retail_price * quantity"
    path_to_lines = "nations.suppliers.supply_records.CALCULATE(retail_price=part.retail_price).lines.WHERE(ship_mode == 'AIR')"
    part_values: str = (
        f"{path_to_lines}.CALCULATE(is_intra={is_intra}, value={base_value})"
    )
    string_representation: str = f"TPCH.regions.CALCULATE(region_name=name).CALCULATE(region_name=region_name, intra_pct=100.0 * (SUM({part_values}.value * {part_values}.is_intra) / SUM({part_values}.value)))"
    tree_string_representation: str = """
──┬─ TPCH
  ├─── TableCollection[regions]
  ├─── Calculate[region_name=name]
  └─┬─ Calculate[region_name=region_name, intra_pct=100.0 * (SUM($1.value * $1.is_intra) / SUM($1.value))]
    └─┬─ AccessChild
      └─┬─ SubCollection[nations]
        └─┬─ SubCollection[suppliers]
          ├─── SubCollection[supply_records]
          └─┬─ Calculate[retail_price=$1.retail_price]
            ├─┬─ AccessChild
            │ └─── SubCollection[part]
            ├─── SubCollection[lines]
            ├─── Where[ship_mode == 'AIR']
            └─┬─ Calculate[is_intra=$1.name == region_name, value=retail_price * quantity]
              └─┬─ AccessChild
                └─┬─ SubCollection[order]
                  └─┬─ SubCollection[customer]
                    └─── SubCollection[region]
"""
    return test_info, string_representation, tree_string_representation


@pytest.mark.parametrize(
    "calc_pipeline, expected_calcs, expected_total_names",
    [
        pytest.param(
            CalculateInfo(
                [], x=LiteralInfo(1, NumericType()), y=LiteralInfo(3, NumericType())
            ),
            {"x": 0, "y": 1},
            {
                "x",
                "y",
                "customers",
                "lines",
                "nations",
                "orders",
                "supply_records",
                "parts",
                "regions",
                "suppliers",
            },
            id="global_calc",
        ),
        pytest.param(
            CalculateInfo(
                [
                    TableCollectionInfo("suppliers"),
                    TableCollectionInfo("parts")
                    ** SubCollectionInfo("lines")
                    ** CalculateInfo(
                        [],
                        value=FunctionInfo(
                            "MUL", [ReferenceInfo("quantity"), ReferenceInfo("tax")]
                        ),
                    ),
                ],
                n_balance=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                ),
                t_value=FunctionInfo("SUM", [ChildReferenceExpressionInfo("value", 1)]),
            ),
            {"n_balance": 0, "t_value": 1},
            {
                "n_balance",
                "t_value",
                "customers",
                "lines",
                "nations",
                "orders",
                "supply_records",
                "parts",
                "regions",
                "suppliers",
            },
            id="global_nested_calc",
        ),
        pytest.param(
            TableCollectionInfo("regions"),
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
            TableCollectionInfo("regions") ** SubCollectionInfo("nations"),
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
            TableCollectionInfo("regions")
            ** CalculateInfo(
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
                TableCollectionInfo("regions")
                ** CalculateInfo(
                    [], foo=LiteralInfo(42, NumericType()), bar=ReferenceInfo("name")
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
                TableCollectionInfo("regions")
                ** CalculateInfo(
                    [], foo=LiteralInfo(42, NumericType()), bar=ReferenceInfo("name")
                )
                ** SubCollectionInfo("nations")
            ),
            {"key": 0, "name": 1, "region_key": 2, "comment": 3},
            {
                "foo",
                "bar",
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
                TableCollectionInfo("regions")
                ** SubCollectionInfo("nations")
                ** CalculateInfo(
                    [], foo=LiteralInfo(42, NumericType()), bar=ReferenceInfo("name")
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
                TableCollectionInfo("regions")
                ** CalculateInfo(
                    [], foo=LiteralInfo(42, NumericType()), bar=ReferenceInfo("name")
                )
                ** CalculateInfo(
                    [],
                    fizz=FunctionInfo(
                        "ADD", [ReferenceInfo("foo"), LiteralInfo(1, NumericType())]
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
            TableCollectionInfo("parts") ** SubCollectionInfo("suppliers_of_part"),
            {
                "key": 0,
                "name": 1,
                "address": 2,
                "nation_key": 3,
                "phone": 4,
                "account_balance": 5,
                "comment": 6,
                "ps_available_quantity": 7,
                "ps_supply_cost": 8,
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
                "ps_available_quantity",
                "ps_comment",
                "ps_supply_cost",
                "ps_lines",
            },
            id="parts_suppliers",
        ),
        pytest.param(
            (
                TableCollectionInfo("parts")
                ** SubCollectionInfo("suppliers_of_part")
                ** CalculateInfo(
                    [],
                    good_comment=FunctionInfo(
                        "EQU",
                        [ReferenceInfo("comment"), LiteralInfo("good", StringType())],
                    ),
                    negative_balance=FunctionInfo(
                        "LET",
                        [
                            ReferenceInfo("account_balance"),
                            LiteralInfo(0.0, NumericType()),
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
                "ps_available_quantity",
                "ps_comment",
                "ps_supply_cost",
                "ps_lines",
                "good_comment",
                "negative_balance",
            },
            id="parts_suppliers_calc",
        ),
        pytest.param(
            TableCollectionInfo("regions") ** SubCollectionInfo("suppliers"),
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
            TableCollectionInfo("regions") ** SubCollectionInfo("lines_sourced_from"),
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
                "return_flag": 14,
                "comment": 15,
                "nation_name": 16,
                "supplier_name": 17,
                "supplier_address": 18,
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
                "return_flag",
                "comment",
                "part_and_supplier",
                "order",
                "supplier",
                "part",
                "nation_name",
                "supplier_name",
                "supplier_address",
                "other_parts_supplied",
                "supplier_region",
            },
            id="regions_lines",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
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
                ** CalculateInfo(
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
                TableCollectionInfo("regions")
                ** SubCollectionInfo("nations")
                ** SubCollectionInfo("suppliers")
                ** SubCollectionInfo("supply_records")
                ** SubCollectionInfo("lines")
                ** CalculateInfo(
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
                "return_flag",
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
                TableCollectionInfo("regions")
                ** SubCollectionInfo("lines_sourced_from")
                ** CalculateInfo(
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
                "return_flag",
                "comment",
                "part_and_supplier",
                "order",
                "supplier",
                "part",
                "nation_name",
                "supplier_name",
                "supplier_address",
                "other_parts_supplied",
                "supplier_region",
            },
            id="regions_lines_backcalc",
        ),
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("parts"),
                "containers",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [SubCollectionInfo("parts")],
                container=ReferenceInfo("container"),
                total_price=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                ),
            ),
            {"container": 0, "total_price": 1},
            {"container", "total_price", "parts"},
            id="partition_with_order_part",
        ),
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("parts")
                ** OrderInfo([], (ReferenceInfo("retail_price"), False, True)),
                "containers",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [SubCollectionInfo("parts")],
                container=ReferenceInfo("container"),
                total_price=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                ),
            )
            ** SubCollectionInfo("parts")
            ** CalculateInfo(
                [],
                part_name=ReferenceInfo("name"),
                container=ReferenceInfo("container"),
                ratio=FunctionInfo(
                    "DIV",
                    [
                        ReferenceInfo("retail_price"),
                        BackReferenceExpressionInfo("total_price", 1),
                    ],
                ),
            ),
            {"part_name": 0, "container": 1, "ratio": 2},
            {
                "container",
                "ratio",
                "brand",
                "comment",
                "key",
                "lines",
                "manufacturer",
                "name",
                "part_name",
                "retail_price",
                "size",
                "suppliers_of_part",
                "supply_records",
                "part_type",
                "total_price",
            },
            id="partition_data_with_data_order",
        ),
    ],
)
def test_collections_calc_terms(
    calc_pipeline: CollectionTestInfo,
    expected_calcs: dict[str, int],
    expected_total_names: set[str],
    tpch_node_builder: AstNodeBuilder,
) -> None:
    """
    Tests that a sequence of collection-producing QDAG nodes results in the
    correct calculate terms & total set of available terms.
    """
    collection: PyDoughCollectionQDAG = calc_pipeline.build(tpch_node_builder)
    assert collection.calc_terms == set(expected_calcs), (
        "Mismatch between set of calculate terms and expected value"
    )
    actual_calcs: dict[str, int] = {
        expr: collection.get_expression_position(expr) for expr in collection.calc_terms
    }
    assert actual_calcs == expected_calcs, (
        "Mismatch between positions of calculate terms and expected value"
    )
    assert collection.all_terms == expected_total_names, (
        "Mismatch between set of all terms and expected value"
    )


@pytest.mark.parametrize(
    "calc_pipeline, expected_string, expected_tree_string",
    [
        pytest.param(
            CalculateInfo(
                [], x=LiteralInfo(1, NumericType()), y=LiteralInfo(3, NumericType())
            ),
            "TPCH.CALCULATE(x=1, y=3)",
            """
┌─── TPCH
└─── Calculate[x=1, y=3]
""",
            id="global_calc",
        ),
        pytest.param(
            CalculateInfo(
                [
                    TableCollectionInfo("suppliers"),
                    TableCollectionInfo("parts")
                    ** SubCollectionInfo("lines")
                    ** CalculateInfo(
                        [],
                        value=FunctionInfo(
                            "MUL", [ReferenceInfo("quantity"), ReferenceInfo("tax")]
                        ),
                    ),
                ],
                n_balance=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                ),
                t_value=FunctionInfo("SUM", [ChildReferenceExpressionInfo("value", 1)]),
            ),
            "TPCH.CALCULATE(n_balance=SUM(suppliers.account_balance), t_value=SUM(parts.lines.CALCULATE(value=quantity * tax).value))",
            """
┌─── TPCH
└─┬─ Calculate[n_balance=SUM($1.account_balance), t_value=SUM($2.value)]
  ├─┬─ AccessChild
  │ └─── TableCollection[suppliers]
  └─┬─ AccessChild
    └─┬─ TableCollection[parts]
      ├─── SubCollection[lines]
      └─── Calculate[value=quantity * tax]
""",
            id="global_nested_calc",
        ),
        pytest.param(
            TableCollectionInfo("regions"),
            "TPCH.regions",
            """
──┬─ TPCH
  └─── TableCollection[regions]
""",
            id="regions",
        ),
        pytest.param(
            TableCollectionInfo("regions") ** SubCollectionInfo("nations"),
            "TPCH.regions.nations",
            """
──┬─ TPCH
  └─┬─ TableCollection[regions]
    └─── SubCollection[nations]
""",
            id="regions_nations",
        ),
        pytest.param(
            TableCollectionInfo("regions") ** SubCollectionInfo("nations"),
            "TPCH.regions.nations",
            """
──┬─ TPCH
  └─┬─ TableCollection[regions]
    └─── SubCollection[nations]
""",
            id="regions_filter_nations_where",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** CalculateInfo(
                [],
            ),
            "TPCH.regions.CALCULATE()",
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  └─── Calculate[]
""",
            id="regions_empty_calc",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** CalculateInfo(
                [],
                region_name=ReferenceInfo("name"),
                adjusted_key=FunctionInfo(
                    "MUL",
                    [
                        FunctionInfo(
                            "SUB", [ReferenceInfo("key"), LiteralInfo(1, NumericType())]
                        ),
                        LiteralInfo(2, NumericType()),
                    ],
                ),
            ),
            "TPCH.regions.CALCULATE(region_name=name, adjusted_key=(key - 1) * 2)",
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  └─── Calculate[region_name=name, adjusted_key=(key - 1) * 2]
""",
            id="regions_calc",
        ),
        pytest.param(
            TableCollectionInfo("regions")
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
            ** CalculateInfo(
                [],
                region_name=BackReferenceExpressionInfo("name", 1),
                nation_name=ReferenceInfo("name"),
            ),
            "TPCH.regions.WHERE(name != 'ASIA').nations.WHERE(name != 'USA').CALCULATE(region_name=name, nation_name=name)",
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  └─┬─ Where[name != 'ASIA']
    ├─── SubCollection[nations]
    ├─── Where[name != 'USA']
    └─── Calculate[region_name=name, nation_name=name]
""",
            id="regions_nations_calc",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** CalculateInfo(
                [SubCollectionInfo("suppliers")],
                nation_name=ReferenceInfo("name"),
                total_supplier_balances=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                ),
            ),
            "TPCH.nations.CALCULATE(nation_name=name, total_supplier_balances=SUM(suppliers.account_balance))",
            """
──┬─ TPCH
  ├─── TableCollection[nations]
  └─┬─ Calculate[nation_name=name, total_supplier_balances=SUM($1.account_balance)]
    └─┬─ AccessChild
      └─── SubCollection[suppliers]
""",
            id="nations_childcalc_suppliers",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** CalculateInfo(
                [], adj_name=FunctionInfo("LOWER", [ReferenceInfo("name")])
            )
            ** SubCollectionInfo("nations")
            ** CalculateInfo(
                [],
                region_name=BackReferenceExpressionInfo("adj_name", 1),
                nation_name=ReferenceInfo("name"),
            ),
            "TPCH.regions.CALCULATE(adj_name=LOWER(name)).nations.CALCULATE(region_name=adj_name, nation_name=name)",
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  └─┬─ Calculate[adj_name=LOWER(name)]
    ├─── SubCollection[nations]
    └─── Calculate[region_name=adj_name, nation_name=name]
""",
            id="regions_calc_nations_calc",
        ),
        pytest.param(
            TableCollectionInfo("suppliers")
            ** CalculateInfo(
                [SubCollectionInfo("parts_supplied")],
                supplier_name=ReferenceInfo("name"),
                total_retail_price=FunctionInfo(
                    "SUM",
                    [
                        FunctionInfo(
                            "SUB",
                            [
                                ChildReferenceExpressionInfo("retail_price", 0),
                                LiteralInfo(1.0, NumericType()),
                            ],
                        )
                    ],
                ),
            ),
            "TPCH.suppliers.CALCULATE(supplier_name=name, total_retail_price=SUM(parts_supplied.retail_price - 1.0))",
            """
──┬─ TPCH
  ├─── TableCollection[suppliers]
  └─┬─ Calculate[supplier_name=name, total_retail_price=SUM($1.retail_price - 1.0)]
    └─┬─ AccessChild
      └─── SubCollection[parts_supplied]
""",
            id="suppliers_childcalc_parts_a",
        ),
        pytest.param(
            TableCollectionInfo("suppliers")
            ** CalculateInfo(
                [
                    SubCollectionInfo("parts_supplied")
                    ** CalculateInfo(
                        [],
                        adj_retail_price=FunctionInfo(
                            "SUB",
                            [
                                ReferenceInfo("retail_price"),
                                LiteralInfo(1.0, NumericType()),
                            ],
                        ),
                    )
                ],
                supplier_name=ReferenceInfo("name"),
                total_retail_price=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("adj_retail_price", 0)]
                ),
            ),
            "TPCH.suppliers.CALCULATE(supplier_name=name, total_retail_price=SUM(parts_supplied.CALCULATE(adj_retail_price=retail_price - 1.0).adj_retail_price))",
            """
──┬─ TPCH
  ├─── TableCollection[suppliers]
  └─┬─ Calculate[supplier_name=name, total_retail_price=SUM($1.adj_retail_price)]
    └─┬─ AccessChild
      ├─── SubCollection[parts_supplied]
      └─── Calculate[adj_retail_price=retail_price - 1.0]
""",
            id="suppliers_childcalc_parts_b",
        ),
        pytest.param(
            TableCollectionInfo("suppliers")
            ** SubCollectionInfo("supply_records")
            ** CalculateInfo(
                [SubCollectionInfo("lines")],
                ratio=FunctionInfo(
                    "DIV",
                    [
                        FunctionInfo(
                            "SUM", [ChildReferenceExpressionInfo("quantity", 0)]
                        ),
                        ReferenceInfo("available_quantity"),
                    ],
                ),
            )
            ** SubCollectionInfo("part")
            ** CalculateInfo(
                [],
                supplier_name=BackReferenceExpressionInfo("name", 2),
                part_name=ReferenceInfo("name"),
                ratio=BackReferenceExpressionInfo("ratio", 1),
            ),
            "TPCH.suppliers.supply_records.CALCULATE(ratio=SUM(lines.quantity) / available_quantity).part.CALCULATE(supplier_name=name, part_name=name, ratio=ratio)",
            """
──┬─ TPCH
  └─┬─ TableCollection[suppliers]
    ├─── SubCollection[supply_records]
    └─┬─ Calculate[ratio=SUM($1.quantity) / available_quantity]
      ├─┬─ AccessChild
      │ └─── SubCollection[lines]
      ├─── SubCollection[part]
      └─── Calculate[supplier_name=name, part_name=name, ratio=ratio]
""",
            id="suppliers_parts_childcalc",
        ),
        pytest.param(
            (
                CalculateInfo(
                    [TableCollectionInfo("customers")],
                    total_balance=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                )
            ),
            "TPCH.CALCULATE(total_balance=SUM(customers.account_balance))",
            """
┌─── TPCH
└─┬─ Calculate[total_balance=SUM($1.account_balance)]
  └─┬─ AccessChild
    └─── TableCollection[customers]
""",
            id="globalcalc_a",
        ),
        pytest.param(
            CalculateInfo(
                [
                    TableCollectionInfo("customers"),
                    TableCollectionInfo("suppliers")
                    ** SubCollectionInfo("parts_supplied")
                    ** CalculateInfo(
                        [],
                        value=FunctionInfo(
                            "MUL",
                            [
                                ReferenceInfo("ps_available_quantity"),
                                ReferenceInfo("retail_price"),
                            ],
                        ),
                    ),
                ],
                total_demand=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                ),
                total_supply=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("value", 1)]
                ),
            ),
            "TPCH.CALCULATE(total_demand=SUM(customers.account_balance), total_supply=SUM(suppliers.parts_supplied.CALCULATE(value=ps_available_quantity * retail_price).value))",
            """
┌─── TPCH
└─┬─ Calculate[total_demand=SUM($1.account_balance), total_supply=SUM($2.value)]
  ├─┬─ AccessChild
  │ └─── TableCollection[customers]
  └─┬─ AccessChild
    └─┬─ TableCollection[suppliers]
      ├─── SubCollection[parts_supplied]
      └─── Calculate[value=ps_available_quantity * retail_price]
""",
            id="globalcalc_b",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** OrderInfo([], (ReferenceInfo("name"), True, True)),
            "TPCH.nations.ORDER_BY(name.ASC(na_pos='last'))",
            """
──┬─ TPCH
  ├─── TableCollection[nations]
  └─── OrderBy[name.ASC(na_pos='last')]
""",
            id="nations_ordering",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** CalculateInfo(
                [SubCollectionInfo("customers")],
                nation_name=ReferenceInfo("name"),
                n_customers=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
            ),
            "TPCH.nations.CALCULATE(nation_name=name, n_customers=COUNT(customers))",
            """
──┬─ TPCH
  ├─── TableCollection[nations]
  └─┬─ Calculate[nation_name=name, n_customers=COUNT($1)]
    └─┬─ AccessChild
      └─── SubCollection[customers]
""",
            id="count_subcollection",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** CalculateInfo(
                [
                    SubCollectionInfo("customers"),
                    SubCollectionInfo("customers")
                    ** WhereInfo(
                        [SubCollectionInfo("orders")],
                        FunctionInfo(
                            "EQU",
                            [
                                FunctionInfo(
                                    "COUNT", [ChildReferenceCollectionInfo(0)]
                                ),
                                LiteralInfo(0, NumericType()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("customers")
                    ** SubCollectionInfo("orders")
                    ** SubCollectionInfo("lines"),
                    SubCollectionInfo("customers")
                    ** SubCollectionInfo("orders")
                    ** SubCollectionInfo("lines")
                    ** SubCollectionInfo("part"),
                ],
                nation_name=ReferenceInfo("name"),
                n_customers=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
                n_customers_without_orders=FunctionInfo(
                    "COUNT", [ChildReferenceCollectionInfo(1)]
                ),
                n_lines_with_tax=FunctionInfo(
                    "COUNT", [ChildReferenceExpressionInfo("tax", 2)]
                ),
                n_part_orders=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(3)]),
                n_unique_parts_ordered=FunctionInfo(
                    "NDISTINCT", [ChildReferenceCollectionInfo(3)]
                ),
            ),
            "TPCH.nations.CALCULATE(nation_name=name, n_customers=COUNT(customers), n_customers_without_orders=COUNT(customers.WHERE(COUNT(orders) == 0)), n_lines_with_tax=COUNT(customers.orders.lines.tax), n_part_orders=COUNT(customers.orders.lines.part), n_unique_parts_ordered=NDISTINCT(customers.orders.lines.part))",
            """
──┬─ TPCH
  ├─── TableCollection[nations]
  └─┬─ Calculate[nation_name=name, n_customers=COUNT($1), n_customers_without_orders=COUNT($2), n_lines_with_tax=COUNT($3.tax), n_part_orders=COUNT($4), n_unique_parts_ordered=NDISTINCT($4)]
    ├─┬─ AccessChild
    │ └─── SubCollection[customers]
    ├─┬─ AccessChild
    │ ├─── SubCollection[customers]
    │ └─┬─ Where[COUNT($1) == 0]
    │   └─┬─ AccessChild
    │     └─── SubCollection[orders]
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[customers]
    │   └─┬─ SubCollection[orders]
    │     └─── SubCollection[lines]
    └─┬─ AccessChild
      └─┬─ SubCollection[customers]
        └─┬─ SubCollection[orders]
          └─┬─ SubCollection[lines]
            └─── SubCollection[part]
""",
            id="agg_subcollection_multiple",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** OrderInfo(
                [SubCollectionInfo("suppliers")],
                (
                    FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    False,
                    True,
                ),
                (ReferenceInfo("name"), True, True),
            ),
            "TPCH.nations.ORDER_BY(SUM(suppliers.account_balance).DESC(na_pos='last'), name.ASC(na_pos='last'))",
            """
──┬─ TPCH
  ├─── TableCollection[nations]
  └─┬─ OrderBy[SUM($1.account_balance).DESC(na_pos='last'), name.ASC(na_pos='last')]
    └─┬─ AccessChild
      └─── SubCollection[suppliers]
""",
            id="nations_nested_ordering",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("nations")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** SubCollectionInfo("customers")
            ** OrderInfo([], (ReferenceInfo("account_balance"), True, True)),
            "TPCH.regions.ORDER_BY(name.ASC(na_pos='last')).nations.ORDER_BY(key.ASC(na_pos='last')).customers.ORDER_BY(account_balance.ASC(na_pos='last'))",
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  └─┬─ OrderBy[name.ASC(na_pos='last')]
    ├─── SubCollection[nations]
    └─┬─ OrderBy[key.ASC(na_pos='last')]
      ├─── SubCollection[customers]
      └─── OrderBy[account_balance.ASC(na_pos='last')]
""",
            id="regions_nations_customers_order",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("customers")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** WhereInfo(
                [],
                FunctionInfo(
                    "GRT",
                    [
                        ReferenceInfo("account_balance"),
                        LiteralInfo(1000, NumericType()),
                    ],
                ),
            )
            ** CalculateInfo([], region_name=BackReferenceExpressionInfo("name", 1))
            ** OrderInfo([], (ReferenceInfo("region_name"), True, True))
            ** WhereInfo(
                [],
                FunctionInfo(
                    "NEQ",
                    [ReferenceInfo("region_name"), LiteralInfo("ASIA", StringType())],
                ),
            ),
            "TPCH.regions.ORDER_BY(name.ASC(na_pos='last')).customers.ORDER_BY(key.ASC(na_pos='last')).WHERE(account_balance > 1000).CALCULATE(region_name=name).ORDER_BY(region_name.ASC(na_pos='last')).WHERE(region_name != 'ASIA')",
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  └─┬─ OrderBy[name.ASC(na_pos='last')]
    ├─── SubCollection[customers]
    ├─── OrderBy[key.ASC(na_pos='last')]
    ├─── Where[account_balance > 1000]
    ├─── Calculate[region_name=name]
    ├─── OrderBy[region_name.ASC(na_pos='last')]
    └─── Where[region_name != 'ASIA']
""",
            id="regions_customers_order_where_calc_order_where",
        ),
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("parts"),
                "containers",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [SubCollectionInfo("parts")],
                container=ReferenceInfo("container"),
                total_price=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                ),
            ),
            "TPCH.Partition(parts, name='containers', by=container).CALCULATE(container=container, total_price=SUM(parts.retail_price))",
            """
──┬─ TPCH
  ├─┬─ Partition[name='containers', by=container]
  │ └─┬─ AccessChild
  │   └─── TableCollection[parts]
  └─┬─ Calculate[container=container, total_price=SUM($1.retail_price)]
    └─┬─ AccessChild
      └─── PartitionChild[parts]
""",
            id="partition_part",
        ),
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("lines")
                ** WhereInfo(
                    [],
                    FunctionInfo(
                        "EQU", [ReferenceInfo("tax"), LiteralInfo(0, NumericType())]
                    ),
                )
                ** CalculateInfo(
                    [
                        SubCollectionInfo("order")
                        ** SubCollectionInfo("shipping_region"),
                        SubCollectionInfo("part"),
                    ],
                    region_name=ChildReferenceExpressionInfo("name", 0),
                    part_type=ChildReferenceExpressionInfo("part_type", 1),
                ),
                "groups",
                [
                    ChildReferenceExpressionInfo("region_name", 0),
                    ChildReferenceExpressionInfo("part_type", 0),
                ],
            )
            ** CalculateInfo(
                [SubCollectionInfo("lines")],
                region_name=ReferenceInfo("region_name"),
                part_type=ReferenceInfo("part_type"),
                total_price=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("extended_price", 0)]
                ),
            ),
            "TPCH.Partition(lines.WHERE(tax == 0).CALCULATE(region_name=order.shipping_region.name, part_type=part.part_type), name='groups', by=('region_name', 'part_type')).CALCULATE(region_name=region_name, part_type=part_type, total_price=SUM(lines.extended_price))",
            """
──┬─ TPCH
  ├─┬─ Partition[name='groups', by=(region_name, part_type)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[lines]
  │   ├─── Where[tax == 0]
  │   └─┬─ Calculate[region_name=$1.name, part_type=$2.part_type]
  │     ├─┬─ AccessChild
  │     │ └─┬─ SubCollection[order]
  │     │   └─── SubCollection[shipping_region]
  │     └─┬─ AccessChild
  │       └─── SubCollection[part]
  └─┬─ Calculate[region_name=region_name, part_type=part_type, total_price=SUM($1.extended_price)]
    └─┬─ AccessChild
      └─── PartitionChild[lines]
""",
            id="partition_nested",
        ),
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("parts"),
                "containers",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [SubCollectionInfo("parts")],
                container=ReferenceInfo("container"),
                total_price=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                ),
            )
            ** OrderInfo([], (ReferenceInfo("total_price"), False, True)),
            "TPCH.Partition(parts, name='containers', by=container).CALCULATE(container=container, total_price=SUM(parts.retail_price)).ORDER_BY(total_price.DESC(na_pos='last'))",
            """
──┬─ TPCH
  ├─┬─ Partition[name='containers', by=container]
  │ └─┬─ AccessChild
  │   └─── TableCollection[parts]
  ├─┬─ Calculate[container=container, total_price=SUM($1.retail_price)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[parts]
  └─── OrderBy[total_price.DESC(na_pos='last')]
""",
            id="partition_with_order_part",
        ),
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("parts")
                ** OrderInfo([], (ReferenceInfo("retail_price"), False, True)),
                "containers",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [SubCollectionInfo("parts")],
                container=ReferenceInfo("container"),
                total_price=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                ),
            )
            ** SubCollectionInfo("parts")
            ** CalculateInfo(
                [],
                part_name=ReferenceInfo("name"),
                container=ReferenceInfo("container"),
                ratio=FunctionInfo(
                    "DIV",
                    [
                        ReferenceInfo("retail_price"),
                        BackReferenceExpressionInfo("total_price", 1),
                    ],
                ),
            ),
            "TPCH.Partition(parts.ORDER_BY(retail_price.DESC(na_pos='last')), name='containers', by=container).CALCULATE(container=container, total_price=SUM(parts.retail_price)).parts.CALCULATE(part_name=name, container=container, ratio=retail_price / total_price)",
            """
──┬─ TPCH
  ├─┬─ Partition[name='containers', by=container]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[parts]
  │   └─── OrderBy[retail_price.DESC(na_pos='last')]
  └─┬─ Calculate[container=container, total_price=SUM($1.retail_price)]
    ├─┬─ AccessChild
    │ └─── PartitionChild[parts]
    ├─── PartitionChild[parts]
    └─── Calculate[part_name=name, container=container, ratio=retail_price / total_price]
""",
            id="partition_data_with_data_order",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** CalculateInfo(
                [SubCollectionInfo("suppliers")],
                total_sum=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                ),
            )
            ** TopKInfo([], 5, (ReferenceInfo("total_sum"), False, True)),
            "TPCH.nations.CALCULATE(total_sum=SUM(suppliers.account_balance)).TOP_K(5, total_sum.DESC(na_pos='last'))",
            """
──┬─ TPCH
  ├─── TableCollection[nations]
  ├─┬─ Calculate[total_sum=SUM($1.account_balance)]
  │ └─┬─ AccessChild
  │   └─── SubCollection[suppliers]
  └─── TopK[5, total_sum.DESC(na_pos='last')]
""",
            id="nations_topk",
        ),
        pytest.param(
            TableCollectionInfo("parts")
            ** WhereInfo(
                [
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("GERMANY", StringType()),
                            ],
                        ),
                    )
                ],
                FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
            ),
            "TPCH.parts.WHERE(HAS(suppliers_of_part.nation.WHERE(name == 'GERMANY')))",
            """
──┬─ TPCH
  ├─── TableCollection[parts]
  └─┬─ Where[HAS($1)]
    └─┬─ AccessChild
      └─┬─ SubCollection[suppliers_of_part]
        ├─── SubCollection[nation]
        └─── Where[name == 'GERMANY']
""",
            id="simple_has",
        ),
        pytest.param(
            TableCollectionInfo("customers")
            ** CalculateInfo(
                [SubCollectionInfo("nation")],
                cust_nation_name=ChildReferenceExpressionInfo("name", 0),
            )
            ** WhereInfo(
                [
                    SubCollectionInfo("orders")
                    ** SubCollectionInfo("lines")
                    ** WhereInfo(
                        [SubCollectionInfo("supplier") ** SubCollectionInfo("nation")],
                        FunctionInfo(
                            "NEQ",
                            [
                                ChildReferenceExpressionInfo("name", 0),
                                BackReferenceExpressionInfo("cust_nation_name", 2),
                            ],
                        ),
                    )
                ],
                FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(0)]),
            ),
            "TPCH.customers.CALCULATE(cust_nation_name=nation.name).WHERE(HASNOT(orders.lines.WHERE(supplier.nation.name != cust_nation_name)))",
            """
──┬─ TPCH
  ├─── TableCollection[customers]
  ├─┬─ Calculate[cust_nation_name=$1.name]
  │ └─┬─ AccessChild
  │   └─── SubCollection[nation]
  └─┬─ Where[HASNOT($1)]
    └─┬─ AccessChild
      └─┬─ SubCollection[orders]
        ├─── SubCollection[lines]
        └─┬─ Where[$1.name != cust_nation_name]
          └─┬─ AccessChild
            └─┬─ SubCollection[supplier]
              └─── SubCollection[nation]
""",
            id="simple_hasnot",
        ),
        pytest.param(
            TableCollectionInfo("suppliers")
            ** CalculateInfo(
                [
                    SubCollectionInfo("parts_supplied")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("size"), LiteralInfo(24, NumericType())],
                        ),
                    ),
                    SubCollectionInfo("parts_supplied")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("size"), LiteralInfo(25, NumericType())],
                        ),
                    ),
                ],
                has_part_24=FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
                hasnot_part_25=FunctionInfo(
                    "HASNOT", [ChildReferenceCollectionInfo(1)]
                ),
            )
            ** WhereInfo(
                [
                    SubCollectionInfo("parts_supplied")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("size"), LiteralInfo(26, NumericType())],
                        ),
                    ),
                    SubCollectionInfo("parts_supplied")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("size"), LiteralInfo(27, NumericType())],
                        ),
                    ),
                ],
                FunctionInfo(
                    "BAN",
                    [
                        FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(0)]),
                        FunctionInfo("HAS", [ChildReferenceCollectionInfo(1)]),
                    ],
                ),
            )
            ** TopKInfo(
                [
                    SubCollectionInfo("parts_supplied")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("size"), LiteralInfo(28, NumericType())],
                        ),
                    ),
                    SubCollectionInfo("parts_supplied")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("size"), LiteralInfo(29, NumericType())],
                        ),
                    ),
                ],
                100,
                (FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]), True, True),
                (FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(1)]), True, True),
                (ReferenceInfo("key"), True, True),
            )
            ** OrderInfo(
                [
                    SubCollectionInfo("parts_supplied")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("size"), LiteralInfo(30, NumericType())],
                        ),
                    ),
                    SubCollectionInfo("parts_supplied")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("size"), LiteralInfo(31, NumericType())],
                        ),
                    ),
                ],
                (FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(0)]), True, True),
                (FunctionInfo("HAS", [ChildReferenceCollectionInfo(1)]), True, True),
                (ReferenceInfo("key"), True, True),
            ),
            "TPCH.suppliers.CALCULATE(has_part_24=COUNT(parts_supplied.WHERE(size == 24)) > 0, hasnot_part_25=COUNT(parts_supplied.WHERE(size == 25)) == 0).WHERE(HASNOT(parts_supplied.WHERE(size == 26)) & HAS(parts_supplied.WHERE(size == 27))).TOP_K(100, (COUNT(parts_supplied.WHERE(size == 28)) > 0).ASC(na_pos='last'), (COUNT(parts_supplied.WHERE(size == 29)) == 0).ASC(na_pos='last'), key.ASC(na_pos='last')).ORDER_BY((COUNT(parts_supplied.WHERE(size == 30)) == 0).ASC(na_pos='last'), (COUNT(parts_supplied.WHERE(size == 31)) > 0).ASC(na_pos='last'), key.ASC(na_pos='last'))",
            """
──┬─ TPCH
  ├─── TableCollection[suppliers]
  ├─┬─ Calculate[has_part_24=COUNT($1) > 0, hasnot_part_25=COUNT($2) == 0]
  │ ├─┬─ AccessChild
  │ │ ├─── SubCollection[parts_supplied]
  │ │ └─── Where[size == 24]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[parts_supplied]
  │   └─── Where[size == 25]
  ├─┬─ Where[HASNOT($1) & HAS($2)]
  │ ├─┬─ AccessChild
  │ │ ├─── SubCollection[parts_supplied]
  │ │ └─── Where[size == 26]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[parts_supplied]
  │   └─── Where[size == 27]
  ├─┬─ TopK[100, (COUNT($1) > 0).ASC(na_pos='last'), (COUNT($2) == 0).ASC(na_pos='last'), key.ASC(na_pos='last')]
  │ ├─┬─ AccessChild
  │ │ ├─── SubCollection[parts_supplied]
  │ │ └─── Where[size == 28]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[parts_supplied]
  │   └─── Where[size == 29]
  └─┬─ OrderBy[(COUNT($1) == 0).ASC(na_pos='last'), (COUNT($2) > 0).ASC(na_pos='last'), key.ASC(na_pos='last')]
    ├─┬─ AccessChild
    │ ├─── SubCollection[parts_supplied]
    │ └─── Where[size == 30]
    └─┬─ AccessChild
      ├─── SubCollection[parts_supplied]
      └─── Where[size == 31]
""",
            id="multiple_locations_has_hasnot",
        ),
        pytest.param(
            TableCollectionInfo("parts")
            ** WhereInfo(
                [
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("GERMANY", StringType()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("BRAZIL", StringType()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("name"), LiteralInfo("USA", StringType())],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("FRANCE", StringType()),
                            ],
                        ),
                    ),
                ],
                FunctionInfo(
                    "BAN",
                    [
                        FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
                        FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(1)]),
                        FunctionInfo("HAS", [ChildReferenceCollectionInfo(2)]),
                        FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(3)]),
                        FunctionInfo(
                            "CONTAINS",
                            [
                                ReferenceInfo("part_type"),
                                LiteralInfo("ECONOMY", StringType()),
                            ],
                        ),
                    ],
                ),
            ),
            "TPCH.parts.WHERE(HAS(suppliers_of_part.nation.WHERE(name == 'GERMANY')) & HASNOT(suppliers_of_part.nation.WHERE(name == 'BRAZIL')) & HAS(suppliers_of_part.nation.WHERE(name == 'USA')) & HASNOT(suppliers_of_part.nation.WHERE(name == 'FRANCE')) & CONTAINS(part_type, 'ECONOMY'))",
            """
──┬─ TPCH
  ├─── TableCollection[parts]
  └─┬─ Where[HAS($1) & HASNOT($2) & HAS($3) & HASNOT($4) & CONTAINS(part_type, 'ECONOMY')]
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'GERMANY']
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'BRAZIL']
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'USA']
    └─┬─ AccessChild
      └─┬─ SubCollection[suppliers_of_part]
        ├─── SubCollection[nation]
        └─── Where[name == 'FRANCE']
""",
            id="conjunction_has_hasnot",
        ),
        pytest.param(
            TableCollectionInfo("parts")
            ** WhereInfo(
                [
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("GERMANY", StringType()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("BRAZIL", StringType()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("name"), LiteralInfo("USA", StringType())],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("FRANCE", StringType()),
                            ],
                        ),
                    ),
                ],
                FunctionInfo(
                    "BOR",
                    [
                        FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
                        FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(1)]),
                        FunctionInfo("HAS", [ChildReferenceCollectionInfo(2)]),
                        FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(3)]),
                        FunctionInfo(
                            "CONTAINS",
                            [
                                ReferenceInfo("part_type"),
                                LiteralInfo("ECONOMY", StringType()),
                            ],
                        ),
                    ],
                ),
            ),
            "TPCH.parts.WHERE((COUNT(suppliers_of_part.nation.WHERE(name == 'GERMANY')) > 0) | (COUNT(suppliers_of_part.nation.WHERE(name == 'BRAZIL')) == 0) | (COUNT(suppliers_of_part.nation.WHERE(name == 'USA')) > 0) | (COUNT(suppliers_of_part.nation.WHERE(name == 'FRANCE')) == 0) | CONTAINS(part_type, 'ECONOMY'))",
            """
──┬─ TPCH
  ├─── TableCollection[parts]
  └─┬─ Where[(COUNT($1) > 0) | (COUNT($2) == 0) | (COUNT($3) > 0) | (COUNT($4) == 0) | CONTAINS(part_type, 'ECONOMY')]
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'GERMANY']
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'BRAZIL']
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'USA']
    └─┬─ AccessChild
      └─┬─ SubCollection[suppliers_of_part]
        ├─── SubCollection[nation]
        └─── Where[name == 'FRANCE']
""",
            id="disjunction_has_hasnot",
        ),
        pytest.param(
            TableCollectionInfo("parts")
            ** WhereInfo(
                [
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("GERMANY", StringType()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("BRAZIL", StringType()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("name"), LiteralInfo("USA", StringType())],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("FRANCE", StringType()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("ARGENTINA", StringType()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [
                                ReferenceInfo("name"),
                                LiteralInfo("CANADA", StringType()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers_of_part")
                    ** SubCollectionInfo("nation")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "EQU",
                            [ReferenceInfo("name"), LiteralInfo("INDIA", StringType())],
                        ),
                    ),
                ],
                FunctionInfo(
                    "BAN",
                    [
                        FunctionInfo(
                            "BOR",
                            [
                                FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
                                FunctionInfo("HAS", [ChildReferenceCollectionInfo(1)]),
                            ],
                        ),
                        FunctionInfo(
                            "BOR",
                            [
                                FunctionInfo("HAS", [ChildReferenceCollectionInfo(2)]),
                                FunctionInfo("HAS", [ChildReferenceCollectionInfo(3)]),
                            ],
                        ),
                        FunctionInfo("HAS", [ChildReferenceCollectionInfo(4)]),
                        FunctionInfo(
                            "BAN",
                            [
                                FunctionInfo("HAS", [ChildReferenceCollectionInfo(5)]),
                                FunctionInfo("HAS", [ChildReferenceCollectionInfo(6)]),
                                FunctionInfo(
                                    "CONTAINS",
                                    [
                                        ReferenceInfo("part_type"),
                                        LiteralInfo("ECONOMY", StringType()),
                                    ],
                                ),
                                FunctionInfo(
                                    "BOR",
                                    [
                                        FunctionInfo(
                                            "HAS", [ChildReferenceCollectionInfo(0)]
                                        ),
                                        FunctionInfo(
                                            "HAS", [ChildReferenceCollectionInfo(3)]
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ),
            "TPCH.parts.WHERE(((COUNT(suppliers_of_part.nation.WHERE(name == 'GERMANY')) > 0) | (COUNT(suppliers_of_part.nation.WHERE(name == 'BRAZIL')) > 0)) & ((COUNT(suppliers_of_part.nation.WHERE(name == 'USA')) > 0) | (COUNT(suppliers_of_part.nation.WHERE(name == 'FRANCE')) > 0)) & HAS(suppliers_of_part.nation.WHERE(name == 'ARGENTINA')) & HAS(suppliers_of_part.nation.WHERE(name == 'CANADA')) & HAS(suppliers_of_part.nation.WHERE(name == 'INDIA')) & CONTAINS(part_type, 'ECONOMY') & ((COUNT(suppliers_of_part.nation.WHERE(name == 'GERMANY')) > 0) | (COUNT(suppliers_of_part.nation.WHERE(name == 'FRANCE')) > 0)))",
            """
──┬─ TPCH
  ├─── TableCollection[parts]
  └─┬─ Where[((COUNT($1) > 0) | (COUNT($2) > 0)) & ((COUNT($3) > 0) | (COUNT($4) > 0)) & HAS($5) & HAS($6) & HAS($7) & CONTAINS(part_type, 'ECONOMY') & ((COUNT($1) > 0) | (COUNT($4) > 0))]
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'GERMANY']
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'BRAZIL']
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'USA']
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'FRANCE']
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'ARGENTINA']
    ├─┬─ AccessChild
    │ └─┬─ SubCollection[suppliers_of_part]
    │   ├─── SubCollection[nation]
    │   └─── Where[name == 'CANADA']
    └─┬─ AccessChild
      └─┬─ SubCollection[suppliers_of_part]
        ├─── SubCollection[nation]
        └─── Where[name == 'INDIA']
""",
            id="hybrid_has_hasnot",
        ),
        pytest.param(
            TableCollectionInfo("customers")
            ** CalculateInfo(
                [],
                name=ReferenceInfo("name"),
                cust_rank=WindowInfo(
                    "RANKING", (ReferenceInfo("account_balance"), False, True)
                ),
            ),
            "TPCH.customers.CALCULATE(name=name, cust_rank=RANKING(by=(account_balance.DESC(na_pos='last'))))",
            """
──┬─ TPCH
  ├─── TableCollection[customers]
  └─── Calculate[name=name, cust_rank=RANKING(by=(account_balance.DESC(na_pos='last')))]
""",
            id="rank_customers_a",
        ),
        pytest.param(
            TableCollectionInfo("customers")
            ** CalculateInfo(
                [],
                name=ReferenceInfo("name"),
                cust_rank=WindowInfo(
                    "RANKING",
                    (ReferenceInfo("account_balance"), False, True),
                    allow_ties=True,
                ),
            ),
            "TPCH.customers.CALCULATE(name=name, cust_rank=RANKING(by=(account_balance.DESC(na_pos='last')), allow_ties=True))",
            """
──┬─ TPCH
  ├─── TableCollection[customers]
  └─── Calculate[name=name, cust_rank=RANKING(by=(account_balance.DESC(na_pos='last')), allow_ties=True)]
""",
            id="rank_customers_b",
        ),
        pytest.param(
            TableCollectionInfo("customers")
            ** CalculateInfo(
                [],
                name=ReferenceInfo("name"),
                cust_rank=WindowInfo(
                    "RANKING",
                    (ReferenceInfo("account_balance"), False, True),
                    allow_ties=True,
                    dense=True,
                ),
            ),
            "TPCH.customers.CALCULATE(name=name, cust_rank=RANKING(by=(account_balance.DESC(na_pos='last')), allow_ties=True, dense=True))",
            """
──┬─ TPCH
  ├─── TableCollection[customers]
  └─── Calculate[name=name, cust_rank=RANKING(by=(account_balance.DESC(na_pos='last')), allow_ties=True, dense=True)]
""",
            id="rank_customers_c",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** SubCollectionInfo("customers")
            ** CalculateInfo(
                [],
                nation_name=BackReferenceExpressionInfo("name", 1),
                name=ReferenceInfo("name"),
                cust_rank=WindowInfo(
                    "RANKING", (ReferenceInfo("account_balance"), False, True), levels=1
                ),
            ),
            "TPCH.nations.customers.CALCULATE(nation_name=name, name=name, cust_rank=RANKING(by=(account_balance.DESC(na_pos='last')), levels=1))",
            """
──┬─ TPCH
  └─┬─ TableCollection[nations]
    ├─── SubCollection[customers]
    └─── Calculate[nation_name=name, name=name, cust_rank=RANKING(by=(account_balance.DESC(na_pos='last')), levels=1)]
""",
            id="rank_customers_per_nation",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** SubCollectionInfo("nations")
            ** SubCollectionInfo("customers")
            ** CalculateInfo(
                [],
                region_name=BackReferenceExpressionInfo("name", 2),
                nation_name=BackReferenceExpressionInfo("name", 1),
                name=ReferenceInfo("name"),
                cust_rank=WindowInfo(
                    "RANKING",
                    (ReferenceInfo("account_balance"), False, True),
                    levels=2,
                    allow_ties=True,
                    dense=True,
                ),
            ),
            "TPCH.regions.nations.customers.CALCULATE(region_name=name, nation_name=name, name=name, cust_rank=RANKING(by=(account_balance.DESC(na_pos='last')), levels=2, allow_ties=True, dense=True))",
            """
──┬─ TPCH
  └─┬─ TableCollection[regions]
    └─┬─ SubCollection[nations]
      ├─── SubCollection[customers]
      └─── Calculate[region_name=name, nation_name=name, name=name, cust_rank=RANKING(by=(account_balance.DESC(na_pos='last')), levels=2, allow_ties=True, dense=True)]
""",
            id="rank_customers_per_region",
        ),
    ],
)
def test_collections_to_string(
    calc_pipeline: CollectionTestInfo,
    expected_string: str,
    expected_tree_string: str,
    tpch_node_builder: AstNodeBuilder,
) -> None:
    """
    Verifies that various QDAG collection node structures produce the expected
    non-tree string representation.
    """
    collection: PyDoughCollectionQDAG = calc_pipeline.build(tpch_node_builder)
    assert collection.to_string() == expected_string, (
        "Mismatch between non-tree string representation and expected value"
    )
    assert collection.to_tree_string() == expected_tree_string.strip(), (
        "Mismatch between tree string representation and expected value"
    )


@pytest.mark.parametrize(
    "calc_pipeline, expected_collation_strings",
    [
        pytest.param(
            CalculateInfo(
                [], x=LiteralInfo(1, NumericType()), y=LiteralInfo(3, NumericType())
            ),
            None,
            id="global_calc",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** SubCollectionInfo("suppliers")
            ** CalculateInfo(
                [],
                region_name=BackReferenceExpressionInfo("name", 1),
                nation_name=ReferenceInfo("nation_name"),
                supplier_name=ReferenceInfo("name"),
            ),
            None,
            id="regions_suppliers_calc",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** OrderInfo([], (ReferenceInfo("name"), True, True)),
            ["name.ASC(na_pos='last')"],
            id="nations_ordering",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** OrderInfo(
                [SubCollectionInfo("suppliers")],
                (
                    FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
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
            TableCollectionInfo("regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("nations")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** SubCollectionInfo("customers")
            ** OrderInfo([], (ReferenceInfo("account_balance"), True, True)),
            ["account_balance.ASC(na_pos='last')"],
            id="regions_nations_customers_order",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("nations")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** SubCollectionInfo("customers"),
            None,
            id="regions_nations_customers",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("customers")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** WhereInfo(
                [],
                FunctionInfo(
                    "GRT",
                    [
                        ReferenceInfo("account_balance"),
                        LiteralInfo(1000, NumericType()),
                    ],
                ),
            )
            ** CalculateInfo([], region_name=BackReferenceExpressionInfo("name", 1))
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
            TableCollectionInfo("regions")
            ** OrderInfo([], (ReferenceInfo("name"), True, True))
            ** SubCollectionInfo("customers")
            ** OrderInfo([], (ReferenceInfo("key"), True, True))
            ** WhereInfo(
                [],
                FunctionInfo(
                    "GRT",
                    [
                        ReferenceInfo("account_balance"),
                        LiteralInfo(1000, NumericType()),
                    ],
                ),
            )
            ** CalculateInfo([], region_name=BackReferenceExpressionInfo("name", 1))
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
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("parts"),
                "containers",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [SubCollectionInfo("parts")],
                container=ReferenceInfo("container"),
                total_price=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                ),
            ),
            None,
            id="partition_without_order",
        ),
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("parts"),
                "containers",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [SubCollectionInfo("parts")],
                container=ReferenceInfo("container"),
                total_price=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                ),
            )
            ** OrderInfo([], (ReferenceInfo("total_price"), False, True)),
            ["total_price.DESC(na_pos='last')"],
            id="partition_with_order_part",
        ),
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("parts")
                ** OrderInfo([], (ReferenceInfo("retail_price"), False, True)),
                "containers",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [SubCollectionInfo("parts")],
                container=ReferenceInfo("container"),
                total_price=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                ),
            ),
            None,
            id="partition_with_data_order",
        ),
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("parts")
                ** OrderInfo([], (ReferenceInfo("retail_price"), False, True)),
                "containers",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [SubCollectionInfo("parts")],
                container=ReferenceInfo("container"),
                total_price=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                ),
            )
            ** SubCollectionInfo("parts")
            ** CalculateInfo(
                [],
                part_name=ReferenceInfo("name"),
                container=ReferenceInfo("container"),
                ratio=FunctionInfo(
                    "DIV",
                    [
                        ReferenceInfo("retail_price"),
                        BackReferenceExpressionInfo("total_price", 1),
                    ],
                ),
            ),
            ["retail_price.DESC(na_pos='last')"],
            id="partition_data_with_data_order",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** CalculateInfo(
                [SubCollectionInfo("suppliers")],
                total_sum=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                ),
            )
            ** TopKInfo([], 5, (ReferenceInfo("total_sum"), False, True)),
            ["total_sum.DESC(na_pos='last')"],
            id="nations_topk",
        ),
    ],
)
def test_collections_ordering(
    calc_pipeline: CollectionTestInfo,
    expected_collation_strings: list[str] | None,
    tpch_node_builder: AstNodeBuilder,
) -> None:
    """
    Verifies that various QDAG collection node structures have the expected
    collation nodes to order by.
    """
    collection: PyDoughCollectionQDAG = calc_pipeline.build(tpch_node_builder)
    if expected_collation_strings is None:
        assert collection.ordering is None, (
            "expected collection to not have an ordering, but it did have one"
        )
    else:
        assert collection.ordering is not None, (
            "expected collection to have an ordering, but it did not"
        )
        collation_strings: list[str] = [
            collation.to_string() for collation in collection.ordering
        ]
        assert collation_strings == expected_collation_strings, (
            "Mismatch between string representation of collation keys and expected value"
        )


def test_regions_intra_pct_string_order(
    region_intra_pct: tuple[CollectionTestInfo, str, str],
    tpch_node_builder: AstNodeBuilder,
) -> None:
    """
    Same as `test_collections_to_string` and `test_collections_ordering`, but
    specifically on the structure from the `region_intra_pct` fixture.
    """
    calc_pipeline, expected_string, expected_tree_string = region_intra_pct
    collection: PyDoughCollectionQDAG = calc_pipeline.build(tpch_node_builder)
    assert collection.to_string() == expected_string
    assert collection.to_tree_string() == expected_tree_string.strip()
    assert collection.ordering is None
