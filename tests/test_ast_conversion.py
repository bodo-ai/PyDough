"""
TODO: add file-level docstring.
"""

import pytest
from test_utils import (
    ChildReferenceExpressionInfo,
    CollectionTestInfo,
    FunctionInfo,
    LiteralInfo,
    ReferenceInfo,
    SubCollectionInfo,
    TableCollectionInfo,
    WhereInfo,
)

from pydough.conversion import convert_ast_to_relational
from pydough.pydough_ast import AstNodeBuilder, PyDoughCollectionAST
from pydough.relational.relational_nodes import Relational
from pydough.types import (
    StringType,
)


@pytest.mark.parametrize(
    "calc_pipeline, expected_relational_string",
    [
        pytest.param(
            TableCollectionInfo("Regions"),
            """
ROOT(columns=[('key', key), ('name', name), ('comment', comment)], orderings=[])
 SCAN(table=tpch.REGION, columns={'comment': r_comment, 'key': r_regionkey, 'name': r_name})
""",
            id="scan_regions",
        ),
        pytest.param(
            TableCollectionInfo("Nations"),
            """
ROOT(columns=[('key', key), ('name', name), ('region_key', region_key), ('comment', comment)], orderings=[])
 SCAN(table=tpch.NATION, columns={'comment': n_comment, 'key': n_nationkey, 'name': n_name, 'region_key': n_regionkey})
""",
            id="scan_nations",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("nations"),
            """
ROOT(columns=[('key', key_0), ('name', name_0), ('region_key', region_key), ('comment', comment_0)], orderings=[])
 JOIN(cond=left.key == right.region_key, type=inner, columns={'comment': left.comment, 'comment_0': right.comment, 'key': left.key, 'key_0': right.key, 'name': left.name, 'name_0': right.name, 'region_key': right.region_key})
  SCAN(table=tpch.REGION, columns={'comment': r_comment, 'key': r_regionkey, 'name': r_name})
  SCAN(table=tpch.NATION, columns={'comment': n_comment, 'key': n_nationkey, 'name': n_name, 'region_key': n_regionkey})
""",
            id="join_region_nations",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** SubCollectionInfo("customers"),
            """
ROOT(columns=[('key', key_1), ('name', name_1), ('address', address), ('nation_key', nation_key), ('phone', phone), ('acctbal', acctbal), ('mktsegment', mktsegment), ('comment', comment_1)], orderings=[])
 JOIN(cond=left.key_0 == right.nation_key, type=inner, columns={'acctbal': right.acctbal, 'address': right.address, 'comment': left.comment, 'comment_0': left.comment_0, 'comment_1': right.comment, 'key': left.key, 'key_0': left.key_0, 'key_1': right.key, 'mktsegment': right.mktsegment, 'name': left.name, 'name_0': left.name_0, 'name_1': right.name, 'nation_key': right.nation_key, 'phone': right.phone, 'region_key': left.region_key})
  JOIN(cond=left.key == right.region_key, type=inner, columns={'comment': left.comment, 'comment_0': right.comment, 'key': left.key, 'key_0': right.key, 'name': left.name, 'name_0': right.name, 'region_key': right.region_key})
   SCAN(table=tpch.REGION, columns={'comment': r_comment, 'key': r_regionkey, 'name': r_name})
   SCAN(table=tpch.NATION, columns={'comment': n_comment, 'key': n_nationkey, 'name': n_name, 'region_key': n_regionkey})
  SCAN(table=tpch.CUSTOMER, columns={'acctbal': c_acctbal, 'address': c_address, 'comment': c_comment, 'key': c_custkey, 'mktsegment': c_mktsegment, 'name': c_name, 'nation_key': c_nationkey, 'phone': c_phone})
""",
            id="join_region_nations_customers",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** WhereInfo(
                [],
                FunctionInfo(
                    "EQU", [ReferenceInfo("name"), LiteralInfo("ASIA", StringType())]
                ),
            )
            ** SubCollectionInfo("nations"),
            """\
\
""",
            id="join_asia_region_nations",
            marks=pytest.mark.skip("TODO"),
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** WhereInfo(
                [SubCollectionInfo("region")],
                FunctionInfo(
                    "EQU",
                    [
                        ChildReferenceExpressionInfo("name", 0),
                        LiteralInfo("ASIA", StringType()),
                    ],
                ),
            ),
            """\
\
""",
            id="asian_regions",
            marks=pytest.mark.skip("TODO"),
        ),
        pytest.param(
            TableCollectionInfo("Lineitems")
            ** WhereInfo(
                [
                    SubCollectionInfo("supplier") ** SubCollectionInfo("nation"),
                    SubCollectionInfo("part"),
                ],
                FunctionInfo(
                    "BAN",
                    [
                        FunctionInfo(
                            "EQU",
                            [
                                ChildReferenceExpressionInfo("name", 0),
                                LiteralInfo("GERMANY", StringType()),
                            ],
                        ),
                        FunctionInfo(
                            "STARTSWITH",
                            [
                                ChildReferenceExpressionInfo("part_type", 1),
                                LiteralInfo("ECONOMY", StringType()),
                            ],
                        ),
                    ],
                ),
            ),
            """

""",
            id="lines_german_supplier_economy_part",
            marks=pytest.mark.skip("TODO"),
        ),
    ],
)
def test_ast_to_relational(
    calc_pipeline: CollectionTestInfo,
    expected_relational_string: str,
    tpch_node_builder: AstNodeBuilder,
):
    """
    Tests whether the AST nodes are correctly translated into Relational nodes
    with the expected string representation.
    """
    collection: PyDoughCollectionAST = calc_pipeline.build(tpch_node_builder)
    relational: Relational = convert_ast_to_relational(collection)
    assert (
        relational.to_tree_string() == expected_relational_string.strip()
    ), "Mismatch between full string representation of output Relational node versus expected string"
