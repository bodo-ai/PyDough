"""
TODO: add file-level docstring.
"""

from typing import Set, Dict
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
)
import pytest


@pytest.mark.parametrize(
    "calc_pipeline, expected_calcs, expected_total_names",
    [
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
            TableCollectionInfo("Regions") ** CalcInfo(),
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
                ** CalcInfo(foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name"))
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
                ** CalcInfo(foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name"))
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
                ** CalcInfo(foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name"))
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
                ** CalcInfo(foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name"))
                ** CalcInfo(
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
