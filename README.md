# PyDough

## Developing PyDough
PyDough uses `uv` as a package manager. Please refer to their docs for
[installation](https://docs.astral.sh/uv/getting-started/). To run testing
commands after installing `uv`, run the following command:

```bash
uv run pytest <pytest_arguments>
```

Note: That some tests may require an additional setup to run successfully.
Please refer to the TPC-H demo directory for more information on how to setup
a default database for testing.

## Running CI Tests
To run our CI tests on your PR, you must include the flag `[run CI]` in latest
commit message.

## Runtime Dependencies

PyDough requires having the following Python modules installed to use
the library:

- pytz
