"""
Tests for the PyDough workflow integrated with BodoSQL
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

from collections.abc import Callable
import pandas as pd
import pytest
import datetime
from pydough.database_connectors.database_connector import (
    DatabaseDialect,
    DatabaseConnection,
)
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode
from pydough.configs import DayOfWeek, PyDoughConfigs
from pydough.database_connectors import DatabaseContext

import os
import pydough
from functools import cache
from tests.test_pydough_functions.simple_pydough_functions import week_offset

from tests.testing_utilities import graph_fetcher
import numpy as np
import numpy.typing as npt
from .conftest import tpch_custom_test_data_dialect_replacements

from .testing_utilities import PyDoughPandasTest
from pydough import init_pydough_context, to_df, to_sql
from bodosql import BodoSQLContext


@pytest.fixture(scope="session")
def bodosql_sample_ctx() -> BodoSQLContext:
    """
    BodoSQL Context context for the sample BodoSQL database.
    """
    color_df: pd.DataFrame = pd.read_csv(
        f"{os.path.dirname(__file__)}/gen_data/colors.csv",
        names=["identname", "colorname", "chex", "r", "g", "b"],
    )
    customers_df: pd.DataFrame = pd.DataFrame(
        {
            "cstid": range(1, 21),
            "cstfname": [
                "Alice",
                "Bob",
                "Charlie",
                "David",
                "Eve",
                "Frank",
                "Grace",
                "Heidi",
                "Ivan",
                "Janet",
                "Karl",
                "Leo",
                "Marcel",
                "Nina",
                "Oscar",
                "Peggy",
                "Quentin",
                "Rahul",
                "Sybil",
                "Trent",
            ],
            "cstlname": [
                "Smith",
                "Johnson",
                "Williams",
                "Jones",
                "Brown",
                "Davis",
                "Miller",
                "Wilson",
                "Moore",
                "Taylor",
                "Anderson",
                "Thomas",
                "Jackson",
                "White",
                "Harris",
                "Martin",
                "Sharma",
                "Garcia",
                "Martinez",
                "Robinson",
            ],
        }
    )
    suppliers_df: pd.DataFrame = pd.DataFrame(
        {
            "supid": range(1, 6),
            "supname": [
                "Pallette Emporium",
                "Rainbow Inc.",
                "Hue Depot",
                "Chroma Co.",
                "Tint Traders",
            ],
        }
    )
    rng: np.random.Generator = np.random.default_rng(seed=42)
    n_shipments: int = 200000
    color_indices: npt.NDArray[np.int_] = np.minimum(
        rng.integers(len(color_df), size=n_shipments),
        rng.integers(len(color_df), size=n_shipments),
    )
    customer_ids: npt.NDArray[np.int_] = np.minimum(
        rng.integers(len(customers_df), size=n_shipments),
        rng.integers(2 * len(customers_df), size=n_shipments),
    )
    supplier_ids: npt.NDArray[np.int_] = np.minimum(
        rng.integers(len(suppliers_df), size=n_shipments),
        rng.integers(round(1.5 * len(suppliers_df)), size=n_shipments),
    )
    dates_of_shipment: list[datetime.date] = sorted(
        [
            datetime.date.fromordinal(738886 + i)
            for i in rng.integers(770, size=n_shipments)
        ]
    )
    volumes: npt.NDArray[np.float64] = rng.choice(
        [0.5, 1.0, 2.0, 4.5, 10.0], p=[0.1, 0.4, 0.3, 0.15, 0.05], size=n_shipments
    )
    prices: npt.NDArray[np.float64] = np.round(
        volumes * (12 + (((1 + color_indices) * (1 + supplier_ids)) % 11.11)), 2
    )
    shipments_df: pd.DataFrame = pd.DataFrame(
        {
            "sid": range(n_shipments),
            "colid": color_df.loc[color_indices, "identname"].values,
            "cusid": customer_ids,
            "comid": supplier_ids,
            "dos": dates_of_shipment,
            "vol": volumes,
            "price": prices,
        }
    )
    bc: BodoSQLContext = BodoSQLContext(
        {
            "CLRS": color_df,
            "SHPMNTS": shipments_df,
            "CUSTOMERS": customers_df,
            "SUPLS": suppliers_df,
        }
    )
    assert bc.estimated_row_counts == [865, 200000, 20, 5]
    bc.estimated_ndvs = [
        {
            "identname": 865,
            "colorname": 865,
            "chex": 765,
            "r": 221,
            "g": 234,
            "b": 340,
        },
        {
            "sid": 20000,
            "colid": 763,
            "cusid": 20,
            "comid": 5,
            "dos": 770,
            "vol": 5,
            "price": 4951,
        },
        {
            "cstid": 20,
            "cstfname": 20,
            "cstlname": 20,
        },
        {
            "supid": 5,
            "supname": 5,
        },
    ]
    return bc


@pytest.fixture(scope="session")
def bodosql_sample_db_ctx(bodosql_sample_ctx: BodoSQLContext) -> DatabaseContext:
    """
    Database context for the sample BodoSQL database.
    """
    return DatabaseContext(bodosql_sample_ctx, dialect=DatabaseDialect.SNOWFLAKE)


@pytest.fixture(scope="session")
def bodosql_graphs() -> graph_fetcher:
    """
    A function that takes in the name of a graph from the supported sample
    BodoSQL graph names and returns the metadata for that PyDough graph.
    """

    @cache
    def impl(name: str) -> GraphMetadata:
        path: str = f"{os.path.dirname(__file__)}/test_metadata/bodosql_graphs.json"
        return pydough.parse_json_metadata_from_file(file_path=path, graph_name=name)

    return impl


@pytest.fixture(
    params=[
        pytest.param(
            # Which three red colors (r is at least 50 larger than g and b) had
            # the largest volume shipped?
            PyDoughPandasTest(
                """
result = (
    colors
    .WHERE(HAS(shipments) & (red >= (green + 50)) & (red >= (blue + 50)))
    .CALCULATE(
        color=name,
        hex_code=hex_code,
        vol_shipped=SUM(shipments.volume),
    )
    .TOP_K(3, by=(vol_shipped.DESC(), hex_code.ASC()))
            )
                """,
                "COLORSHOP",
                lambda: pd.DataFrame(
                    {
                        "color": ["Alabama Crimson", "Auburn", "Alizarin Crimson"],
                        "hex_code": ["#a32638", "#a52a2a", "#e32636"],
                        "vol_shipped": [1211.5, 1138.0, 1114.0],
                    }
                ),
                "color_q01",
                order_sensitive=True,
            ),
            id="color_q01",
        ),
        pytest.param(
            # For each company, what was the first gray color purchased from
            # them? (gray = r, g, and b values are all within 10 of each other
            # and all between 100 and 200 inclusive)
            PyDoughPandasTest(
                """
first_grey = (
    shipments
    .WHERE(
        (SMALLEST(color.red, color.green, color.blue) >= 100)
        & (LARGEST(color.red, color.green, color.blue) <= 200)
        & (LARGEST(color.red, color.green, color.blue) <= (SMALLEST(color.red, color.green, color.blue) + 10))
    )
    .CALCULATE(color_name=color.name, color_hex=color.hex_code)
    .BEST(by=(ship_date.ASC(), key.ASC()), per='companies')
)
result = (
    companies
    .CALCULATE(
        company_name=name,
        color_name=first_grey.color_name,
        color_hex=first_grey.color_hex,
        ship_date=first_grey.ship_date,
    )
    .ORDER_BY(company_name.ASC())
)
                """,
                "COLORSHOP",
                lambda: pd.DataFrame(
                    {
                        "company_name": [
                            "Chroma Co.",
                            "Hue Depot",
                            "Pallette Emporium",
                            "Rainbow Inc.",
                            "Tint Traders",
                        ],
                        "color_name": [
                            "Dim Gray",
                            "Silver",
                            "Gray (X11 Gray)",
                            "Battleship Grey",
                            None,
                        ],
                        "color_hex": ["#696969", "#c0c0c0", "#bebebe", "#848482", None],
                        "vol_shipped": [
                            datetime.date(2024, 1, 2),
                            datetime.date(2024, 1, 1),
                            datetime.date(2024, 1, 2),
                            datetime.date(2024, 1, 1),
                            None,
                        ],
                    }
                ),
                "color_q02",
                order_sensitive=True,
            ),
            id="color_q02",
        ),
    ],
)
def bodosql_e2e_tests(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the BodoSQL tests.
    """
    return request.param


@pytest.mark.bodosql
def test_pipeline_sql_bodosql(
    bodosql_e2e_tests: PyDoughPandasTest,
    bodosql_graphs: graph_fetcher,
    bodosql_sample_db_ctx: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Test transforming queries to SQL for BodoSQL, comparing against expected
    results.
    """
    file_path: str = get_sql_test_filename(
        bodosql_e2e_tests.test_name, bodosql_sample_db_ctx.dialect
    )
    bodosql_e2e_tests.run_sql_test(
        bodosql_graphs,
        file_path,
        update_tests,
        bodosql_sample_db_ctx,
    )


@pytest.mark.bodosql
@pytest.mark.execute
def test_pipeline_e2e_bodosql(
    bodosql_e2e_tests: PyDoughPandasTest,
    bodosql_graphs: graph_fetcher,
    bodosql_sample_db_ctx: DatabaseContext,
):
    """
    Test executing tests end-to-end with BodoSQL, comparing against expected
    results.
    """
    bodosql_e2e_tests.run_e2e_test(
        bodosql_graphs,
        bodosql_sample_db_ctx,
        coerce_types=True,
    )
