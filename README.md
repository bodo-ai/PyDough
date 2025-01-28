# PyDough

PyDough is an alternative DSL that can be used to solve analytical problems by phrasing questions in terms of a logical document model instead of translating to relational SQL logic.

## What Is PyDough

PyDough allows expressing analytical questions with hierarchical thinking, as seen in models such as [MongoDB](https://www.mongodb.com/docs/manual/data-modeling/), since that mental model is closer to human linguistics than a relational model.
Unlike MongoDB, PyDough only uses the a logical document model for abstractly explaining & interacting with data, rather than a physical document model to store the data.
PyDough code can be written in and interleaved with Python code, and practices a lazy evaluation scheme that does not qualify or execute any logic until requested.
PyDough executes by translating its logic into SQL which it can directly executing in a arbitrary database.

Consider the following information represented by the tables in a database:
- There are people; each person has a name, ssn, birth date, records of jobs they have had, and records of schools they have attended.
- There are employment records; each job record has the ssn of the person being employed, the name of the company, and the total income they made from the job.
- There are education records; each education record has the ssn of the person attending the school, the name of the school, and the total tuition they paid to that school.

Suppose I want to know for every person their name & the total income they've made from all jobs minus the total tuition paid to all schools. However, I want to include people who have never had a job or never attended any schools, and I need to account for people who could have had multiple jobs or attended multiple schools attended.
The following PyDough snippet solves this problem:

```py
result = People(
    name,
    net_income = SUM(jobs.income_earned) - SUM(schools.tuition_paid)
)
pydough.to_df(result)
```

However, if answering the question with SQL, I would need to write the following less-intuitive SQL query:

```sql
SELECT
    P.name AS name,
    COALESCE(T1.total_income_earned, 0) - COALESCE(T2.total_tuition_paid, 0) AS net_income
FROM PEOPLE AS P
LEFT JOIN (
    SELECT person_ssn, SUM(income_earned) AS total_income_earned
    FROM EMPLOYMENT_RECORDS
    GROUP BY person_ssn
) AS J
ON P.ssn = J.person_ssn
LEFT JOIN (
    SELECT person_ssn, SUM(tuition_paid) AS total_tuition_paid
    FROM EDUCATION_RECORDS
) AS S
ON P.ssn = S.person_ssn
```

Internally, PyDough solves the question by translating the much simpler logical document model logic into SQL, which can be directly executed on a database. Even if the same SQL is generated by PyDough as the example above, all a user needs to worry about is writing the much smaller PyDough code snippet in Python.

## Why Build PyDough?

PyDough as a DSL has several benefits over other solutions, both for human use and LLM generation:
- ORMs still require understanding & writing SQL, including dealing directly with joins. If a human or AI is bad at writing SQL, they will be just as bad at writing ORM-based code. PyDough, on the other hand, abstracts away joins in favor of thinking about logical relationships between collections & sub-collections.
- The complex semantics of aggregation keys, different types of joins, and aggregating before vs after joining are all abstracted away by PyDough. These details require much deeper understanding of SQL semantics than most have time to learn learn to do correctly, meaning that PyDough can have a lower learning curve to write correct code for complex questions.
- When a question is being asked, the PyDough code to answer it will look more similar to the text of the question than the SQL text would. This makes LLM generation of PyDough code simpler since there is a stronger correlation between a question asked and the PyDough code to answer it.
- Often, PyDough code will be significantly more compact than equivalent SQL text, and therefore easier for a human to verify for logical correctness.
- PyDough is portable between various database execution solutions, so you are not locked into one data storage solution while using PyDough.

## Learning About PyDough

Refer to these documents to learn how to use PyDough:

- [Spec for the PyDough DSL](https://github.com/bodo-ai/PyDough/blob/main/documentation/dsl.md)
- [Spec for the PyDough metadata](https://github.com/bodo-ai/PyDough/blob/main/documentation/metadata.md)
- [List of builtin PyDough functions](https://github.com/bodo-ai/PyDough/blob/main/documentation/functions.md)
- [Usage guide for PyDough](https://github.com/bodo-ai/PyDough/blob/main/documentation/usage.md)

## Installing or Developing PyDough

PyDough releases are [available on PyPI](https://pypi.org/project/pydough/) and can be installed via pip:

```
pip install pydough
```

For local development, PyDough uses `uv` as a package manager.
Please refer to their docs for [installation](https://docs.astral.sh/uv/getting-started/).


To run testing commands after installing `uv`, run the following command:

```bash
uv run pytest <pytest_arguments>
```

If you want to skip tests that execute runtime results because they are slower,
make sure to include `-m "not execute"` in the pytest arguments.

Note: some tests may require an additional setup to run successfully.
The [demos](https://github.com/bodo-ai/PyDough/blob/main/demos/README.md) directory 
contains for more information on how to setup the TPC-H sqlite database. For
testing, the `tpch.db` file must be located in the `tests` directory.
Additionally, the [`setup_defog.sh`](https://github.com/bodo-ai/PyDough/blob/main/tests/setup_defog.sh)
script must be run so that the `defog.db` file is located in the `tests` directory.

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
with the [demo readme](https://github.com/bodo-ai/PyDough/blob/main/demos/README.md) and then walk through the example Juypter notebooks.
