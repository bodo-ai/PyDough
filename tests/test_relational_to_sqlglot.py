"""
Unit tests for converting our Relational nodes to the equivalent
SQLGlot expressions. This is just testing the conversion and is not
testing the actual runtime or converting entire complex trees.
"""

import pytest
from conftest import build_simple_scan, make_literal_column, make_relational_column
from sqlglot.expressions import Expression, From, Identifier, Literal, Select, Table

from pydough.relational.project import Project
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


@pytest.mark.parametrize(
    "project, sqlglot_expr",
    [
        pytest.param(
            Project(
                input=build_simple_scan(),
                columns=[make_relational_column("a"), make_relational_column("b")],
                orderings=None,
            ),
            Select(
                **{
                    "expressions": [
                        Identifier(this="a"),
                        Identifier(this="b"),
                    ],
                    "from": From(this=Table(this=Identifier(this="table"))),
                }
            ),
            id="simple_project",
        ),
        pytest.param(
            Project(
                input=build_simple_scan(),
                columns=[make_relational_column("b")],
                orderings=None,
            ),
            Select(
                **{
                    "expressions": [
                        Identifier(this="b"),
                    ],
                    "from": From(this=Table(this=Identifier(this="table"))),
                }
            ),
            id="column_pruning",
        ),
        pytest.param(
            Project(
                input=build_simple_scan(),
                columns=[make_relational_column("b"), make_relational_column("a")],
                orderings=None,
            ),
            Select(
                **{
                    "expressions": [
                        Identifier(this="b"),
                        Identifier(this="a"),
                    ],
                    "from": From(this=Table(this=Identifier(this="table"))),
                }
            ),
            id="column_reordering",
        ),
        pytest.param(
            Project(
                input=build_simple_scan(),
                columns=[make_relational_column("c", "a"), make_relational_column("b")],
                orderings=None,
            ),
            Select(
                **{
                    "expressions": [
                        Identifier(this="a", alias="c"),
                        Identifier(this="b"),
                    ],
                    "from": From(this=Table(this=Identifier(this="table"))),
                }
            ),
            id="column_renaming",
        ),
        pytest.param(
            Project(
                input=build_simple_scan(),
                columns=[
                    make_relational_column("a"),
                    make_relational_column("b"),
                    make_literal_column("c", 1),
                ],
                orderings=None,
            ),
            Select(
                **{
                    "expressions": [
                        Identifier(this="a"),
                        Identifier(this="b"),
                        Literal(value=1, alias="c"),
                    ],
                    "from": From(this=Table(this=Identifier(this="table"))),
                }
            ),
            id="literal_addition",
        ),
        # TODO: Add dependency tests. Requires ensuring functions are merged.
    ],
)
def test_project_to_sqlglot(project: Project, sqlglot_expr: Expression):
    assert project.to_sqlglot() == sqlglot_expr
