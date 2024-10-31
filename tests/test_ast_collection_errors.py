"""
TODO: add file-level docstring.
"""

import re
from pydough.pydough_ast import AstNodeBuilder
from test_utils import (
    AstNodeTestInfo,
    ReferenceInfo,
    TableCollectionInfo,
    SubCollectionInfo,
    CalcInfo,
    BackReferenceExpressionInfo,
)
import pytest


@pytest.mark.parametrize(
    "calc_pipeline, error_message",
    [
        pytest.param(
            TableCollectionInfo("Rainbows"),
            "graph 'TPCH' does not have a collection named 'Rainbows'",
            id="table_dne",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("postage_stamps"),
            "Unrecognized term of simple table collection 'Regions' in graph 'TPCH': 'postage_stamps'",
            id="subcollection_dne",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** CalcInfo(foo=ReferenceInfo("bar")),
            "Unrecognized term of simple table collection 'Regions' in graph 'TPCH': 'bar'",
            id="reference_dne",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** SubCollectionInfo("suppliers")
            ** CalcInfo(foo=ReferenceInfo("region_key")),
            "Unrecognized term of simple table collection 'Suppliers' in graph 'TPCH': 'region_key'",
            id="reference_bad_ancestry",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo(foo=BackReferenceExpressionInfo("foo", 0)),
            "Expected number of levels in BACK to be a positive integer, received 0",
            id="back_zero",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo(foo=BackReferenceExpressionInfo("foo", 1)),
            "Cannot reference back 1 level above Regions",
            id="back_on_root",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** CalcInfo(foo=BackReferenceExpressionInfo("foo", 2)),
            "Cannot reference back 2 levels above Regions.nations",
            id="back_too_far",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** CalcInfo(foo=BackReferenceExpressionInfo("foo", 1)),
            "Unrecognized term of simple table collection 'Regions' in graph 'TPCH': 'foo'",
            id="back_dne",
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
