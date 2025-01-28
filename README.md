# PyDough

PyDough is an alternative DSL that can be used to solve analytical problems by phrasing questions in terms of a logical document model instead of translating to relational SQL logic.

## What Is PyDough

PyDough allows expressing analytical questions with hierarchical thinking, as seen in models such as Mongo, since that mental model is closer to human linguistics than a relational model.

Consider the following information represented by a database:
- There are people; each person has a name, ssn, birth date, records of jobs they have had, and records of schools they attended
- There are employment records; each job record has the ssn of the person being employed, the name of the company, and the total income they made from the job
- There are education records have; each education record has the ssn of the person attending the school, the name of the school, and the total tuition they paid from the job

Suppose I want to know for every person their name & the total income they've made from all jobs minus the total tuition paid to all schools, including people who have never had a job or never attended any schools, and accounting for people who could have had multiple jobs or schools attended. The following PyDough snippet solves this problem:

```py
People(
    name,
    net_income = SUM(jobs.income_earned) - SUM(schools.tuition_paid)
)
```

However, if answering the question with SQL, the following less-intuitive code would need to be written instead:

```sql
SELECT
    P.name AS name,
    COALESCE(T1.total_income_earned, 0) - COALESCE(T2.total_tuition_paid, 0) AS net_income
FROM PEOPLE AS P
LEFT JOIN (
    SELECT person_ssn, SUM(income_earned) AS total_income_earned
    FROM JOBS
    GROUP BY person_ssn
) AS J
ON P.ssn = J.person_ssn
LEFT JOIN (
    SELECT person_ssn, SUM(tuition_paid) AS total_tuition_paid
    FROM SCHOOLS
    GROUP BY person_ssn
) AS S
ON P.ssn = S.person_ssn
```

## Learning About PyDough

Refer to these documents to learn how to use PyDough:

- [Spec for the PyDough DSL](documentation/dsl.md)
- [Spec for the PyDough metadata](documentation/metadata.md)
- [List of builtin PyDough functions](documentation/functions.md)
- [Usage guide for PyDough](documentation/usage.md)

## Developing PyDough
PyDough uses `uv` as a package manager. Please refer to their docs for
[installation](https://docs.astral.sh/uv/getting-started/). To run testing
commands after installing `uv`, run the following command:

```bash
uv run pytest <pytest_arguments>
```

If you want to skip tests that execute runtime results because they are slower, make sure to include `-m "not slow"` in the pytest arguments.

Note: That some tests may require an additional setup to run successfully.
Please refer to the TPC-H demo directory for more information on how to setup
a default database for testing.

## Running CI Tests
To run our CI tests on your PR, you must include the flag `[run CI]` in latest
commit message.

## Runtime Dependencies

PyDough requires having the following Python modules installed to use
the library:

- pytz, pandas, sqlglot

The full list of dependencies can be found in the `pyproject.toml` file.

## Demo Notebooks

The `demo` folder contains a series of example Jupyter Notebooks
that can be used to understand PyDough's capabilities. We recommend any new user start
with the [demo readme](demos/README.md) and then walk through the example Juypter notebooks.
