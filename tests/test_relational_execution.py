"""
Simple tests to verify that SQL queries can be executed on a SQLite database.
"""

from typing import Any

from test_utils import make_relational_column_reference, make_relational_ordering

from pydough.database_connectors import DatabaseContext
from pydough.pydough_ast.pydough_operators import (
    EQU,
    SUM,
)
from pydough.relational import (
    Aggregate,
    CallExpression,
    Join,
    JoinType,
    Relational,
    RelationalRoot,
    Scan,
)
from pydough.sqlglot import execute
from pydough.types import BooleanType, UnknownType


def test_person_total_salary(sqlite_people_jobs_context: DatabaseContext):
    """
    Tests a simple join and aggregate to compute the total salary for each
    person in the PEOPLE table.
    """
    people: Relational = Scan(
        table_name="PEOPLE",
        columns={
            "person_id": make_relational_column_reference("person_id"),
            "name": make_relational_column_reference("name"),
        },
    )
    jobs: Relational = Aggregate(
        keys={"person_id": make_relational_column_reference("person_id")},
        aggregations={
            "total_salary": CallExpression(
                SUM,
                UnknownType(),
                [make_relational_column_reference("salary")],
            )
        },
        input=Scan(
            table_name="JOBS",
            columns={
                "person_id": make_relational_column_reference("person_id"),
                "salary": make_relational_column_reference("salary"),
            },
        ),
    )
    result = RelationalRoot(
        ordered_columns=[
            ("name", make_relational_column_reference("name")),
            ("total_salary", make_relational_column_reference("total_salary")),
        ],
        orderings=[
            make_relational_ordering(make_relational_column_reference("name"), True)
        ],
        input=Join(
            columns={
                "name": make_relational_column_reference("name", input_name="left"),
                "total_salary": make_relational_column_reference(
                    "total_salary", input_name="right"
                ),
            },
            left=people,
            right=jobs,
            join_type=JoinType.LEFT,
            condition=CallExpression(
                EQU,
                BooleanType(),
                [
                    make_relational_column_reference("person_id", input_name="left"),
                    make_relational_column_reference("person_id", input_name="right"),
                ],
            ),
        ),
    )
    output: list[Any] = execute(result, sqlite_people_jobs_context)
    people_results: list[str] = [f"Person {i}" for i in range(10)]
    salary_results: list[float] = [
        sum((i + j + 5.7) * 1000 for j in range(2)) for i in range(10)
    ]
    expected_output = list(zip(people_results, salary_results))
    assert output == expected_output
