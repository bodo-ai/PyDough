"""
TODO: add file-level docstring.
"""

from typing import List, Set, Dict
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
    TableCollectionInfo,
    SubCollectionInfo,
    CalcInfo,
    pipeline_test_info,
)
import pytest


@pytest.mark.parametrize(
    "calc_pipeline, expected_calcs, expected_total_names",
    [
        pytest.param(
            [TableCollectionInfo("Regions")],
            {"comment": 0, "key": 1, "name": 2},
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
            [
                TableCollectionInfo("Regions"),
                SubCollectionInfo("nations"),
            ],
            {"comment": 0, "key": 1, "name": 2, "region_key": 3},
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
            [
                TableCollectionInfo("Regions"),
                CalcInfo(),
            ],
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
            [
                TableCollectionInfo("Regions"),
                CalcInfo(foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name")),
            ],
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
            [
                TableCollectionInfo("Regions"),
                CalcInfo(foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name")),
                SubCollectionInfo("nations"),
            ],
            {"comment": 0, "key": 1, "name": 2, "region_key": 3},
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
            [
                TableCollectionInfo("Regions"),
                SubCollectionInfo("nations"),
                CalcInfo(foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name")),
            ],
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
            [
                TableCollectionInfo("Regions"),
                CalcInfo(foo=LiteralInfo(42, Int64Type()), bar=ReferenceInfo("name")),
                CalcInfo(
                    fizz=FunctionInfo(
                        "ADD", [ReferenceInfo("foo"), LiteralInfo(1, Int64Type())]
                    ),
                    buzz=FunctionInfo(
                        "SUB", [ReferenceInfo("foo"), LiteralInfo(1, Int64Type())]
                    ),
                ),
            ],
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
            [
                TableCollectionInfo("Parts"),
                SubCollectionInfo("suppliers_of_part"),
            ],
            {
                "account_balance": 0,
                "address": 1,
                "comment": 2,
                "key": 3,
                "name": 4,
                "nation_key": 5,
                "phone": 6,
                "ps_availqty": 7,
                "ps_comment": 8,
                "ps_supplycost": 9,
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
            [
                TableCollectionInfo("Parts"),
                SubCollectionInfo("suppliers_of_part"),
                CalcInfo(
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
                ),
            ],
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
            [
                TableCollectionInfo("Regions"),
                SubCollectionInfo("suppliers"),
            ],
            {
                "account_balance": 0,
                "address": 1,
                "comment": 2,
                "key": 3,
                "name": 4,
                "nation_key": 5,
                "phone": 6,
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
            [
                TableCollectionInfo("Regions"),
                SubCollectionInfo("lines_sourced_from"),
            ],
            {},
            {},
            id="regions_lines",
        ),
    ],
)
def test_collections_calc_terms(
    calc_pipeline: List[AstNodeTestInfo],
    expected_calcs: Dict[str, int],
    expected_total_names: Set[str],
    tpch_node_builder: AstNodeBuilder,
):
    """
    Tests that column properties have the correct return type.
    """
    collection: PyDoughCollectionAST = pipeline_test_info(
        tpch_node_builder, calc_pipeline
    )
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
