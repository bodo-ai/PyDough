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
 JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'comment': t0.comment, 'comment_0': t1.comment, 'key': t0.key, 'key_0': t1.key, 'name': t0.name, 'name_0': t1.name, 'region_key': t1.region_key})
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
 JOIN(conditions=[t0.key_0 == t1.nation_key], types=['inner'], columns={'acctbal': t1.acctbal, 'address': t1.address, 'comment': t0.comment, 'comment_0': t0.comment_0, 'comment_1': t1.comment, 'key': t0.key, 'key_0': t0.key_0, 'key_1': t1.key, 'mktsegment': t1.mktsegment, 'name': t0.name, 'name_0': t0.name_0, 'name_1': t1.name, 'nation_key': t1.nation_key, 'phone': t1.phone, 'region_key': t0.region_key})
  JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'comment': t0.comment, 'comment_0': t1.comment, 'key': t0.key, 'key_0': t1.key, 'name': t0.name, 'name_0': t1.name, 'region_key': t1.region_key})
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
