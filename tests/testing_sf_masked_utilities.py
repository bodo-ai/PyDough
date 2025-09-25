import os
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import cache

import pandas as pd
import pytest

import pydough
from pydough.database_connectors import (
    DatabaseContext,
    load_database_context,
)
from pydough.metadata import GraphMetadata
from tests.testing_utilities import (
    PyDoughPandasTest,
    graph_fetcher,
)


@dataclass
class PyDoughSnowflakeMaskedTest(PyDoughPandasTest):
    """
    The data packet encapsulating the information to run a masked PyDough e2e test
    that compares the result against the given reference answer based on the
    type of Snowflake account used to connect and execute the PyDough-generated
    SQL query.
    """

    answers: dict[str, pd.DataFrame | None] = field(default_factory=dict)
    """
    A mapping of Snowflake account types to their expected query results.
    """

    account_type: str = "none"
    """
    The type of Snowflake account to use for the test.
    Either "none", "partial", or "full".
    """

    # field(init=False) is used to indicate that this field is not
    # initialized via the constructor in this class, but rather is set later.
    pd_function: Callable[[], pd.DataFrame] = field(init=False)

    def __post_init__(self):
        # Override pd_function to look up from answers
        def _lookup() -> pd.DataFrame:
            df = self.answers.get(self.account_type)
            if df is None:
                raise ValueError(f"No DataFrame for account_type={self.account_type}")
            return df

        self.pd_function = _lookup


def is_sf_masked_env(account_type: str = "FULL") -> bool:
    """Check if the environment variables for Snowflake masked tests are set."""
    required_vars: list[str] = [
        "SF_MASKED_ACCOUNT",
    ]
    required_vars += [
        f"SF_{account_type}_USERNAME",
        f"SF_{account_type}_PASSWORD",
    ]
    return all(os.getenv(var) is not None for var in required_vars)


@pytest.fixture
def sf_masked_context() -> Callable[[str, str, str], DatabaseContext]:
    """Fixture for creating a Snowflake DatabaseContext with masked columns.

    Arguments to the returned function:
        `database_name`: The name of the Snowflake database.
        `schema_name`: The name of the Snowflake schema.
        `account_type`: The type of Snowflake account to use ("full", "partial", "none").

    Returns:
        A function that takes the above arguments and returns a DatabaseContext.
    """

    def _impl(
        database_name: str, schema_name: str, account_type: str
    ) -> DatabaseContext:
        account_type = account_type.upper()
        account_lists: list[str] = ["FULL", "PARTIAL", "NONE"]
        if account_type not in account_lists:
            pytest.skip(
                f"Skipping Snowflake masked tests: Unknown account_type {account_type}"
            )
        if not is_sf_masked_env(account_type):
            pytest.skip(
                f"Skipping Snowflake masked tests: {account_type} environment variables not set."
            )
        import snowflake.connector as sf_connector

        env_var_prefix: str = f"SF_{account_type}"
        warehouse: str = "BODO_WH"
        password: str | None = os.getenv(f"{env_var_prefix}_PASSWORD")
        username: str | None = os.getenv(f"{env_var_prefix}_USERNAME")
        account: str | None = os.getenv("SF_MASKED_ACCOUNT")
        connection: sf_connector.connection.SnowflakeConnection = sf_connector.connect(
            user=username,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database_name,
            schema=schema_name,
        )
        return load_database_context("snowflake", connection=connection)

    return _impl


@pytest.fixture(scope="session")
def get_sf_masked_graphs() -> graph_fetcher:
    """
    Returns the graphs for the masked database in Snowflake.
    """

    @cache
    def impl(name: str) -> GraphMetadata:
        path: str = f"{os.path.dirname(__file__)}/test_metadata/sf_masked_examples.json"
        return pydough.parse_json_metadata_from_file(file_path=path, graph_name=name)

    return impl
