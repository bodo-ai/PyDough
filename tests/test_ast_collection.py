"""
TODO: add file-level docstring.
"""

from typing import Set, Dict, Tuple
from pydough.types import (
    StringType,
    Float64Type,
    Int64Type,
)
from pydough.pydough_ast import AstNodeBuilder, PyDoughCollectionAST
from test_utils import (
    AstNodeTestInfo,
    LiteralInfo,
    FunctionInfo,
    ReferenceInfo,
    BackReferenceExpressionInfo,
    TableCollectionInfo,
    SubCollectionInfo,
    CalcInfo,
    ChildReferenceInfo,
    BackReferenceCollectionInfo,
)
import pytest


@pytest.fixture
def region_intra_ratio() -> Tuple[AstNodeTestInfo, str]:
    """
    The AST node info for a query that calculates the ratio for each region
    between the of all part sale values (retail price of the part times the
    quantity purchased for each time it was purchased) that were sold to a
    customer in the same region vs all part sale values from that region.

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
    GROUP BY region_name
    ```

    Equivalent PyDough code:
    ```
    is_intra_line = IFF(order.customer.region.name == BACK(3).name, 1, 0)
    part_sales = suppliers.parts_supplied(
        intra_value = retail_price * ps_lines(adj_quantity = quantity * is_intra_line).adj_quantity,
        total_value = retail_price * ps_lines.quantity,
    )
    Regions(
        region_name = name
        intra_sales = SUM(part_sales.intra_value) / SUM(part_sales.total_value)
    )
    ```
    """
    test_info: AstNodeTestInfo = TableCollectionInfo("Regions") ** CalcInfo(
        [
            SubCollectionInfo("suppliers")
            ** SubCollectionInfo("parts_supplied")
            ** CalcInfo(
                [
                    SubCollectionInfo("ps_lines")
                    ** CalcInfo(
                        [
                            SubCollectionInfo("order")
                            ** SubCollectionInfo("customer")
                            ** SubCollectionInfo("region")
                        ],
                        adj_quantity=FunctionInfo(
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
                    )
                ],
                value=FunctionInfo(
                    "MUL",
                    [
                        ReferenceInfo("retail_price"),
                        ChildReferenceInfo("adj_quantity", 0),
                    ],
                ),
                adj_value=FunctionInfo(
                    "MUL",
                    [
                        ReferenceInfo("retail_price"),
                        ChildReferenceInfo("quantity", 0),
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
    adjusted_lines: str = "ps_lines(adj_quantity=quantity * IFF(BACK(3).name == order.customer.region.name, 1, 0))"
    part_values: str = f"suppliers.parts_supplied(value=retail_price * {adjusted_lines}.adj_quantity, adj_value=retail_price * {adjusted_lines}.quantity)"
    string_representation: str = f"Regions(region_name=name, intra_ratio=SUM({part_values}.adj_value) / SUM({part_values}.value))"
    return test_info, string_representation


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
                    TableCollectionInfo("Parts") ** SubCollectionInfo("lines"),
                ],
                n_balance=FunctionInfo(
                    "SUM", [ChildReferenceInfo("account_balance", 0)]
                ),
                t_tax=FunctionInfo("SUM", [ChildReferenceInfo("tax", 1)]),
            ),
            {"n_balance": 0, "t_tax": 1},
            {
                "n_balance",
                "t_tax",
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
                "supplier_name": 16,
                "supplier_address": 17,
                "ps_availqty": 18,
                "ps_supplycost": 19,
                "ps_comment": 20,
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
                ** SubCollectionInfo("nations")
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
    calc_pipeline: AstNodeTestInfo,
    expected_calcs: Dict[str, int],
    expected_total_names: Set[str],
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
    actual_calcs: Dict[str, int] = {
        expr: collection.get_expression_position(expr) for expr in collection.calc_terms
    }
    assert (
        actual_calcs == expected_calcs
    ), "Mismatch between positions of calc terms and expected value"
    assert (
        collection.all_terms == expected_total_names
    ), "Mismatch between set of all terms and expected value"


@pytest.mark.parametrize(
    "calc_pipeline, expected_string",
    [
        pytest.param(
            CalcInfo([], x=LiteralInfo(1, Int64Type()), y=LiteralInfo(3, Int64Type())),
            "TPCH(x=1, y=3)",
            id="global_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions"),
            "Regions",
            id="regions",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("nations"),
            "Regions.nations",
            id="regions_nations",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo(
                [],
            ),
            "Regions()",
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
            id="regions_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** CalcInfo(
                [],
                region_name=BackReferenceExpressionInfo("name", 1),
                nation_name=ReferenceInfo("name"),
            ),
            "Regions.nations(region_name=BACK(1).name, nation_name=name)",
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
            id="regions_suppliers_calc",
        ),
        pytest.param(
            TableCollectionInfo("Parts")
            ** SubCollectionInfo("suppliers_of_part")
            ** SubCollectionInfo("ps_lines"),
            "Parts.suppliers_of_part.ps_lines",
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
            id="nations_childcalc_suppliers",
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
            id="suppliers_childcalc_parts_b",
        ),
        pytest.param(
            TableCollectionInfo("Suppliers")
            ** SubCollectionInfo("parts_supplied")
            ** CalcInfo(
                [
                    SubCollectionInfo("ps_lines"),
                    BackReferenceCollectionInfo("nation", 1)
                    ** CalcInfo([], name=ReferenceInfo("name")),
                ],
                nation_name=ChildReferenceInfo("name", 1),
                supplier_name=BackReferenceExpressionInfo("name", 1),
                part_name=ReferenceInfo("name"),
                ratio=FunctionInfo(
                    "DIV",
                    [ChildReferenceInfo("quantity", 0), ReferenceInfo("ps_availqty")],
                ),
            ),
            "Suppliers.parts_supplied(nation_name=nation(name=name).name, supplier_name=BACK(1).name, part_name=name, ratio=ps_lines.quantity / ps_availqty)",
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
                    BackReferenceCollectionInfo("nation", 1)
                    ** CalcInfo([], name=ReferenceInfo("name")),
                ],
                nation_name=ChildReferenceInfo("name", 1),
                supplier_name=BackReferenceExpressionInfo("name", 1),
                part_name=ReferenceInfo("name"),
                ratio=ChildReferenceInfo("ratio", 0),
            ),
            "Suppliers.parts_supplied(nation_name=nation(name=name).name, supplier_name=BACK(1).name, part_name=name, ratio=ps_lines(ratio=quantity / BACK(1).ps_availqty).ratio)",
            id="suppliers_parts_childcalc_b",
        ),
    ],
)
def test_collections_to_string(
    calc_pipeline: AstNodeTestInfo,
    expected_string: str,
    tpch_node_builder: AstNodeBuilder,
):
    """
    Verifies that various AST collection node structures produce the expected
    non-tree string representation.
    """
    collection: PyDoughCollectionAST = calc_pipeline.build(tpch_node_builder)
    assert collection.to_string() == expected_string


def test_regions_intra_ratio_to_string(
    region_intra_ratio: Tuple[AstNodeTestInfo, str],
    tpch_node_builder: AstNodeBuilder,
):
    """
    Same as `test_collections_to_string` but specifically on the structure from
    the `region_intra_ratio` fixture.
    """
    calc_pipeline, expected_string = region_intra_ratio
    collection: PyDoughCollectionAST = calc_pipeline.build(tpch_node_builder)
    assert collection.to_string() == expected_string
