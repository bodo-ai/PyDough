# PyDough

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

The test PyDough the `demo` folder contains a series of example Jupyter Notebooks
that can be used to understand PyDough's capabilities. We recommend any new user start
with the [demo readme](demos/README.md) and then walk through the example Juypter notebooks.
