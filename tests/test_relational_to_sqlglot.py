"""
Unit tests for converting our Relational nodes to the equivalent
SQLGlot expressions. This is just testing the conversion and is not
testing the actual runtime or converting entire complex trees.
"""

import pytest
from conftest import make_relational_column
from sqlglot.expressions import Expression, From, Identifier, Select, Table

from pydough.relational.scan import Scan


@pytest.mark.parametrize(
    "scan, sqlglot_expr",
    [
        pytest.param(
            Scan(
                table_name="simple_scan",
                columns=[make_relational_column("a"), make_relational_column("b")],
                orderings=None,
            ),
            Select(
                **{
                    "expressions": [
                        Identifier(this="a"),
                        Identifier(this="b"),
                    ],
                    "from": From(this=Table(this=Identifier(this="simple_scan"))),
                }
            ),
            id="simple_scan",
        )
    ],
)
def test_scan_to_sqlglot(scan: Scan, sqlglot_expr: Expression):
    assert scan.to_sqlglot() == sqlglot_expr
