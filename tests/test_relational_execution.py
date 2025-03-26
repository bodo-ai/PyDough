"""
Simple tests to verify that SQL queries can be executed on a SQLite database.
"""

from typing import Any

import pandas as pd
import pytest
from test_utils import make_relational_column_reference, make_relational_ordering

from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseContext
from pydough.pydough_operators import (
    AVG,
    EQU,
    GRT,
    MAX,
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

pytestmark = [pytest.mark.execute]


def test_person_total_salary(
    sqlite_people_jobs_context: DatabaseContext,
    default_config: PyDoughConfigs,
) -> None:
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
            conditions=[
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("person_id", input_name="t0"),
                        make_relational_column_reference("person_id", input_name="t1"),
                    ],
                )
            ],
            join_types=[JoinType.LEFT],
        ),
    )
    output: list[Any] = execute_df(result, sqlite_people_jobs_context, default_config)
    people_results: list[str] = [f"Person {i}" for i in range(10)]
    salary_results: list[float] = [
        sum((i + j + 5.7) * 1000 for j in range(2)) for i in range(10)
    ]
    expected_output = pd.DataFrame(
        list(zip(people_results, salary_results)), columns=["name", "total_salary"]
    )
    pd.testing.assert_frame_equal(output, expected_output)


def test_person_jobs_multi_join(
    sqlite_people_jobs_context: DatabaseContext,
    default_config: PyDoughConfigs,
) -> None:
    """
    Tests an example of a query that uses a join relational node to
    represent multiple joins. It should be noted that this may not be optimal
    way to represent this query, but it is a valid way to represent it.
    """
    people: RelationalNode = Scan(
        table_name="PEOPLE",
        columns={
            "person_id": make_relational_column_reference("person_id"),
            "name": make_relational_column_reference("name"),
        },
    )
    # Select each person's highest salary
    jobs: RelationalNode = Aggregate(
        keys={"person_id": make_relational_column_reference("person_id")},
        aggregations={
            "max_salary": CallExpression(
                MAX,
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
    # Select the average salary across all jobs ever recorded
    average_salary: RelationalNode = Aggregate(
        keys={},
        aggregations={
            "average_salary": CallExpression(
                AVG,
                UnknownType(),
                [make_relational_column_reference("salary")],
            )
        },
        input=Scan(
            table_name="JOBS",
            columns={"salary": make_relational_column_reference("salary")},
        ),
    )
    # Select all people with a max salary higher than the average salary
    result = RelationalRoot(
        ordered_columns=[
            ("name", make_relational_column_reference("name")),
            ("max_salary", make_relational_column_reference("max_salary")),
        ],
        orderings=[
            make_relational_ordering(make_relational_column_reference("name"), True)
        ],
        input=Join(
            columns={
                "name": make_relational_column_reference("name", input_name="t0"),
                "max_salary": make_relational_column_reference(
                    "max_salary", input_name="t1"
                ),
            },
            inputs=[
                people,
                jobs,
                average_salary,
            ],
            conditions=[
                CallExpression(
                    EQU,
                    BooleanType(),
                    [
                        make_relational_column_reference("person_id", input_name="t0"),
                        make_relational_column_reference("person_id", input_name="t1"),
                    ],
                ),
                CallExpression(
                    GRT,
                    BooleanType(),
                    [
                        make_relational_column_reference("max_salary", input_name="t1"),
                        make_relational_column_reference(
                            "average_salary", input_name="t2"
                        ),
                    ],
                ),
            ],
            join_types=[JoinType.INNER, JoinType.INNER],
        ),
    )
    output: pd.DataFrame = execute_df(
        result, sqlite_people_jobs_context, default_config
    )
    # By construction salaries are increasing with person_id so we
    # select the top half of the people.
    people_results: list[str] = [f"Person {i}" for i in range(5, 10)]
    salary_results: list[float] = [(i + 1 + 5.7) * 1000 for i in range(5, 10)]
    expected_output = pd.DataFrame(
        list(zip(people_results, salary_results)), columns=["name", "max_salary"]
    )
    pd.testing.assert_frame_equal(output, expected_output)
