"""
Simple tests to verify that SQL queries can be executed on a SQLite database.
"""

from typing import Any

import pandas as pd
import pytest

from pydough.configs import PyDoughSession
from pydough.pydough_operators import (
    EQU,
    SUM,
)
from pydough.relational import (
    Aggregate,
    CallExpression,
    Join,
    JoinType,
    RelationalNode,
    RelationalRoot,
    Scan,
)
from pydough.sqlglot import execute_df
from pydough.types import BooleanType, UnknownType
from tests.testing_utilities import (
    make_relational_column_reference,
    make_relational_ordering,
)

pytestmark = [pytest.mark.execute]


def test_person_total_salary(sqlite_people_jobs_session: PyDoughSession) -> None:
    """
    Tests a simple join and aggregate to compute the total salary for each
    person in the PEOPLE table.
    """
    people: RelationalNode = Scan(
        table_name="PEOPLE",
        columns={
            "person_id": make_relational_column_reference("person_id"),
            "name": make_relational_column_reference("name"),
        },
    )
    jobs: RelationalNode = Aggregate(
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
                "name": make_relational_column_reference("name", input_name="t0"),
                "total_salary": make_relational_column_reference(
                    "total_salary", input_name="t1"
                ),
            },
            inputs=[people, jobs],
            condition=CallExpression(
                EQU,
                BooleanType(),
                [
                    make_relational_column_reference("person_id", input_name="t0"),
                    make_relational_column_reference("person_id", input_name="t1"),
                ],
            ),
            join_type=JoinType.LEFT,
        ),
    )
    output: list[Any] = execute_df(result, sqlite_people_jobs_session)
    people_results: list[str] = [f"Person {i}" for i in range(10)]
    salary_results: list[float] = [
        sum((i + j + 5.7) * 1000 for j in range(2)) for i in range(10)
    ]
    expected_output = pd.DataFrame(
        list(zip(people_results, salary_results)), columns=["name", "total_salary"]
    )
    pd.testing.assert_frame_equal(output, expected_output)
