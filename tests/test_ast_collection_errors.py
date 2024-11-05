"""
TODO: add file-level docstring.
"""

import re

import pytest
from test_utils import (
    AstNodeTestInfo,
    BackReferenceExpressionInfo,
    CalcInfo,
    ChildReferenceInfo,
    ReferenceInfo,
    SubCollectionInfo,
    TableCollectionInfo,
)

from pydough.pydough_ast import AstNodeBuilder


@pytest.mark.parametrize(
    "calc_pipeline, error_message",
    [
        pytest.param(
            TableCollectionInfo("Rainbows"),
            "Unrecognized term of graph 'TPCH': 'Rainbows'",
            id="table_dne",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("postage_stamps"),
            "Unrecognized term of simple table collection 'Regions' in graph 'TPCH': 'postage_stamps'",
            id="subcollection_dne",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** CalcInfo([], foo=ReferenceInfo("bar")),
            "Unrecognized term of simple table collection 'Regions' in graph 'TPCH': 'bar'",
            id="reference_dne",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** SubCollectionInfo("suppliers")
            ** CalcInfo([], foo=ReferenceInfo("region_key")),
            "Unrecognized term of simple table collection 'Suppliers' in graph 'TPCH': 'region_key'",
            id="reference_bad_ancestry",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo([], foo=BackReferenceExpressionInfo("foo", 0)),
            "Expected number of levels in BACK to be a positive integer, received 0",
            id="back_zero",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo([], foo=BackReferenceExpressionInfo("foo", 1)),
            "Unrecognized term of graph 'TPCH': 'foo'",
            id="back_on_root",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** CalcInfo([], foo=BackReferenceExpressionInfo("foo", 3)),
            "Cannot reference back 3 levels above Regions.nations",
            id="back_too_far",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** CalcInfo([], foo=BackReferenceExpressionInfo("foo", 1)),
            "Unrecognized term of simple table collection 'Regions' in graph 'TPCH': 'foo'",
            id="back_dne",
        ),
        pytest.param(
            CalcInfo([], foo=ChildReferenceInfo("foo", 0)),
            "Invalid child reference index 0 with 0 children",
            id="child_dne",
        ),
        pytest.param(
            CalcInfo(
                [TableCollectionInfo("Regions")], foo=ChildReferenceInfo("bar", 0)
            ),
            "Unrecognized term of simple table collection 'Regions' in graph 'TPCH': 'bar'",
            id="child_expr_dne",
        ),
    ],
)
def test_malformed_collection_sequences(
    calc_pipeline: AstNodeTestInfo,
    error_message: str,
    tpch_node_builder: AstNodeBuilder,
):
    """
    Tests that building a malformed sequence of collections produces the
    expected error message.
    """
    with pytest.raises(Exception, match=re.escape(error_message)):
        calc_pipeline.build(tpch_node_builder)
