"""
TODO: add file-level docstring.
"""

from typing import Set, Dict
from pydough.types import (
    Int64Type,
)
from pydough.pydough_ast import AstNodeBuilder, PyDoughCollectionAST
from test_utils import (
    AstNodeTestInfo,
    LiteralInfo,
    ReferenceInfo,
    TableCollectionInfo,
    CalcInfo,
)
import pytest


@pytest.mark.parametrize(
    "calc_pipeline, expected_calcs, expected_total_names",
    [
        pytest.param(
            CalcInfo(x=LiteralInfo(1, Int64Type()), y=LiteralInfo(3, Int64Type())),
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
            TableCollectionInfo("Regions"),
            {"key": 0, "name": 1, "comment": 2},
            {
                "name",
                "key",
                "comment",
            },
            id="regions",
        ),
        pytest.param(
            (TableCollectionInfo("Regions") ** CalcInfo()),
            {},
            {
                "name",
                "key",
                "comment",
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
                "foo",
                "bar",
            },
            id="regions_calc",
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
    Tests that column properties have the correct return type.
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
