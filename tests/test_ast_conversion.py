"""
TODO: add file-level docstring.
"""

import pytest
from test_utils import (
    CollectionTestInfo,
    TableCollectionInfo,
)

from pydough.conversion import convert_ast_to_relational
from pydough.pydough_ast import AstNodeBuilder, PyDoughCollectionAST
from pydough.relational.relational_expressions import (
    ColumnReference,
    ColumnSortInfo,
    RelationalExpression,
)
from pydough.relational.relational_nodes import (
    Relational,
    RelationalRoot,
    Scan,
)
from pydough.types import (
    Int64Type,
    StringType,
)


def rootwrap(
    node: Relational,
    column_names: list[str],
    order_info: list[tuple[str, bool, bool]] | None = None,
) -> RelationalRoot:
    """
    TODO
    """
    ordered_columns: list[tuple[str, RelationalExpression]] = []
    orderings: list[ColumnSortInfo] | None = None
    for name in column_names:
        ref: ColumnReference = ColumnReference(name, node.columns[name].data_type)
        ordered_columns.append((name, ref))
    if order_info is not None:
        orderings = []
        for name, asc, na_first in order_info:
            order_ref: ColumnReference = ColumnReference(
                name, node.columns[name].data_type
            )
            orderings.append(ColumnSortInfo(order_ref, asc, na_first))
    return RelationalRoot(node, ordered_columns, orderings)


@pytest.mark.parametrize(
    "calc_pipeline, expected_relational",
    [
        pytest.param(
            TableCollectionInfo("Regions"),
            rootwrap(
                Scan(
                    "tpch.REGION",
                    {
                        "key": ColumnReference("r_regionkey", Int64Type()),
                        "name": ColumnReference("r_name", StringType()),
                        "comment": ColumnReference("r_comment", StringType()),
                    },
                ),
                ["key", "name", "comment"],
            ),
            id="scan_regions",
        ),
        pytest.param(
            TableCollectionInfo("Nations"),
            rootwrap(
                Scan(
                    "tpch.NATION",
                    {
                        "key": ColumnReference("n_nationkey", Int64Type()),
                        "name": ColumnReference("n_name", StringType()),
                        "region_key": ColumnReference("n_regionkey", Int64Type()),
                        "comment": ColumnReference("n_comment", StringType()),
                    },
                ),
                ["key", "name", "region_key", "comment"],
            ),
            id="scan_nations",
        ),
    ],
)
def test_ast_to_relational(
    calc_pipeline: CollectionTestInfo,
    expected_relational: Relational,
    tpch_node_builder: AstNodeBuilder,
):
    """
    Same as `test_collections_to_string` and `test_collections_ordering`, but
    specifically on the structure from the `region_intra_ratio` fixture.
    """
    collection: PyDoughCollectionAST = calc_pipeline.build(tpch_node_builder)
    relational: Relational = convert_ast_to_relational(collection)
    assert relational == expected_relational
