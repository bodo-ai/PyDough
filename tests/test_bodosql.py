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
        names=["IDENTNAME", "COLORNAME", "CHEX", "R", "G", "B"],
    )
    customers_df: pd.DataFrame = pd.DataFrame(
        {
            "CSTID": range(1, 21),
            "CSTFNAME": [
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
            "SUPID": range(1, 6),
            "SUPNAME": [
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
        rng.integers(1, len(customers_df) + 1, size=n_shipments),
        rng.integers(1, 2 * len(customers_df), size=n_shipments),
    )
    supplier_ids: npt.NDArray[np.int_] = np.minimum(
        rng.integers(1, len(suppliers_df) + 1, size=n_shipments),
        rng.integers(1, round(1.5 * len(suppliers_df)), size=n_shipments),
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
            "SID": range(n_shipments),
            "COLID": color_df.loc[color_indices, "IDENTNAME"].values,
            "CUSID": customer_ids,
            "COMID": supplier_ids,
            "DOS": dates_of_shipment,
            "VOL": volumes,
            "PRC": prices,
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
            "IDENTNAME": 865,
            "COLORNAME": 865,
            "CHEX": 765,
            "R": 221,
            "G": 234,
            "B": 340,
        },
        {
            "SID": 20000,
            "COLID": 763,
            "CUSID": 20,
            "COMID": 5,
            "DOS": 770,
            "VOL": 5,
            "PRC": 4951,
        },
        {
            "CSTID": 20,
            "CSTFNAME": 20,
            "CSTLNAME": 20,
        },
        {
            "SUPID": 5,
            "SUPNAME": 5,
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
                            "Silver",
                            "Silver",
                            "Gray (X11 Gray)",
                            "Battleship Grey",
                            "Dim Grey",
                        ],
                        "color_hex": [
                            "#c0c0c0",
                            "#c0c0c0",
                            "#bebebe",
                            "#848482",
                            "#696969",
                        ],
                        "vol_shipped": [
                            datetime.date(2024, 1, 1),
                            datetime.date(2024, 1, 4),
                            datetime.date(2024, 1, 2),
                            datetime.date(2024, 1, 1),
                            datetime.date(2024, 1, 2),
                        ],
                    }
                ),
                "color_q02",
                order_sensitive=True,
            ),
            id="color_q02",
        ),
        pytest.param(
            # For each company, which blue color (blue is at least 50 larger
            # than red and green) made the most total profit?
            PyDoughPandasTest(
                """
blue_data = (
    colors
    .WHERE(blue >= (LARGEST(red, green) + 50))
    .CALCULATE(color_name=name, color_hex=hex_code)
    .shipments
    .PARTITION(name="comp_color", by=(company_key, color_name))
    .CALCULATE(color_hex=ANYTHING(shipments.color_hex), total_profit=SUM(shipments.price))
)
best_blue = CROSS(blue_data).WHERE(company_key == selected_company_key).BEST(by=(total_profit.DESC(), color_name.ASC()), per='companies')
result = (
    companies
    .CALCULATE(selected_company_key=key)
    .CALCULATE(
        company_name=name,
        color_name=best_blue.color_name,
        color_hex=best_blue.color_hex,
        total_profit=best_blue.total_profit,
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
                            "Blue (Ryb)",
                            "Amethyst",
                            "Celestial Blue",
                            "Blue Gray",
                            "Cerulean Blue",
                        ],
                        "color_hex": ["#0247fe", "#96c", "#4997d0", "#69c", "#2a52be"],
                        "total_profit": [
                            3388.59,
                            4567.26,
                            7442.77,
                            7089.28,
                            2543.37,
                        ],
                    }
                ),
                "color_q03",
                order_sensitive=True,
            ),
            id="color_q03",
        ),
        pytest.param(
            # How many shipments were there of colors whose name starts with
            # Yellow in the year 2025?
            PyDoughPandasTest(
                "result = COLORSHOP.CALCULATE(n=COUNT(shipments.WHERE(STARTSWITH(color.name, 'Yellow') & (YEAR(ship_date) == 2025))))",
                "COLORSHOP",
                lambda: pd.DataFrame({"n": [16]}),
                "color_q04",
            ),
            id="color_q04",
        ),
        pytest.param(
            # For each color of the rainbow, how many colors have that color
            # as their first word?
            PyDoughPandasTest(
                """
result = (
    colors
    .CALCULATE(first_word=LOWER(GETPART(name, ' ', 1)))
    .WHERE(ISIN(first_word, rainbow_colors))
    .PARTITION(name="rainbow_color", by=first_word)
    .CALCULATE(rainbow_color=first_word, n=COUNT(colors))
)
                """,
                "COLORSHOP",
                lambda: pd.DataFrame(
                    {
                        "rainbow_color": [
                            "red",
                            "orange",
                            "yellow",
                            "green",
                            "blue",
                            "indigo",
                            "violet",
                        ],
                        "n": [6, 4, 6, 7, 9, 3, 4],
                    }
                ),
                "color_q05",
                kwargs={
                    "rainbow_colors": [
                        "red",
                        "orange",
                        "yellow",
                        "green",
                        "blue",
                        "indigo",
                        "violet",
                    ]
                },
            ),
            id="color_q05",
        ),
        pytest.param(
            # For every day in a specified period, what is the cumulative
            # number of orders made in that period?
            PyDoughPandasTest(
                """
result = (
    shipments
    .WHERE(MONOTONIC(start_date, ship_date, end_date))
    .PARTITION(name="days", by=ship_date)
    .CALCULATE(day=ship_date, cum_ships=RELSUM(COUNT(shipments), by=ship_date.ASC(), cumulative=True))
    .ORDER_BY(day.ASC())
)
                """,
                "COLORSHOP",
                lambda: pd.DataFrame(
                    {
                        "day": [
                            datetime.date(2024, 1, 15),
                            datetime.date(2024, 1, 16),
                            datetime.date(2024, 1, 17),
                            datetime.date(2024, 1, 18),
                            datetime.date(2024, 1, 19),
                            datetime.date(2024, 1, 20),
                        ],
                        "cum_ships": [273, 525, 810, 1064, 1324, 1560],
                    }
                ),
                "color_q06",
                kwargs={
                    "start_date": datetime.date(2024, 1, 15),
                    "end_date": datetime.date(2024, 1, 20),
                },
            ),
            id="color_q06",
        ),
        pytest.param(
            # For every day in a specified period, how many orders were made
            # that day, and what is the change from the previous day within
            # that window, broken down by company?
            PyDoughPandasTest(
                """
result = (
    shipments
    .WHERE(MONOTONIC(start_date, ship_date, end_date))
    .CALCULATE(company_name=company.name)
    .PARTITION(name="day_comps", by=(ship_date, company_name))
    .CALCULATE(n_orders=COUNT(shipments))
    .PARTITION(name="company", by=company_name)
    .day_comps
    .CALCULATE(company_name, day=ship_date, n_orders=n_orders, delta=n_orders - PREV(n_orders, by=ship_date.ASC(), per='company'))
    .ORDER_BY(company_name.ASC(), day.ASC())
)
                """,
                "COLORSHOP",
                lambda: pd.DataFrame(
                    {
                        "company_name": ["Chroma Co."] * 4
                        + ["Hue Depot"] * 4
                        + ["Pallette Emporium"] * 4
                        + ["Rainbow Inc."] * 4
                        + ["Tint Traders"] * 4,
                        "day": [
                            datetime.date(2025, 7, 1),
                            datetime.date(2025, 7, 2),
                            datetime.date(2025, 7, 3),
                            datetime.date(2025, 7, 4),
                        ]
                        * 5,
                        "n_orders": [
                            30,
                            44,
                            33,
                            26,
                            57,
                            52,
                            51,
                            48,
                            83,
                            77,
                            86,
                            73,
                            70,
                            66,
                            65,
                            81,
                            21,
                            19,
                            22,
                            17,
                        ],
                        "delta": [
                            None,
                            14,
                            -11,
                            -7,
                            None,
                            -5,
                            -1,
                            -3,
                            None,
                            -6,
                            9,
                            -13,
                            None,
                            -4,
                            -1,
                            16,
                            None,
                            -2,
                            3,
                            -5,
                        ],
                    }
                ),
                "color_q07",
                kwargs={
                    "start_date": datetime.date(2025, 7, 1),
                    "end_date": datetime.date(2025, 7, 4),
                },
                order_sensitive=True,
            ),
            id="color_q07",
        ),
        pytest.param(
            # List every color key that has never been ordered.
            PyDoughPandasTest(
                """
result = (
    colors
    .WHERE(HASNOT(shipments))
    .CALCULATE(color=key)
    .ORDER_BY(color.ASC())
)
                """,
                "COLORSHOP",
                lambda: pd.DataFrame(
                    {
                        "color": ["zaffre", "zinnwaldite_brown"],
                    }
                ),
                "color_q08",
                order_sensitive=True,
            ),
            id="color_q08",
        ),
        pytest.param(
            # List every color key that has not been ordered since the
            # specified date.
            PyDoughPandasTest(
                """
result = (
    colors
    .WHERE(HASNOT(shipments.WHERE(ship_date >= cutoff_date)))
    .CALCULATE(color=key)
    .ORDER_BY(color.ASC())
)
                """,
                "COLORSHOP",
                lambda: pd.DataFrame(
                    {
                        "color": [
                            "wheat",
                            "wine_dregs",
                            "wisteria",
                            "yale_blue",
                            "yellow_munsell",
                            "yellow_orange",
                            "yellow_process",
                            "yellow_ryb",
                            "zaffre",
                            "zinnwaldite_brown",
                        ],
                    }
                ),
                "color_q09",
                kwargs={"cutoff_date": datetime.date(2025, 9, 15)},
                order_sensitive=True,
            ),
            id="color_q09",
        ),
        pytest.param(
            # How many colors were ordered at least once on the specified date?
            PyDoughPandasTest(
                """
result = COLORSHOP.CALCULATE(n=COUNT(colors.WHERE(HAS(shipments.WHERE(ship_date == chosen_date)))))
                """,
                "COLORSHOP",
                lambda: pd.DataFrame({"n": [194]}),
                "color_q10",
                kwargs={"chosen_date": datetime.date(2024, 7, 4)},
                order_sensitive=True,
            ),
            id="color_q10",
        ),
        pytest.param(
            # Different way of writing colors_q10.
            PyDoughPandasTest(
                """
result = COLORSHOP.CALCULATE(n=NDISTINCT((shipments.WHERE(ship_date == chosen_date).color_key)))
                """,
                "COLORSHOP",
                lambda: pd.DataFrame({"n": [194]}),
                "color_q11",
                kwargs={"chosen_date": datetime.date(2024, 7, 4)},
                order_sensitive=True,
            ),
            id="color_q11",
        ),
    ],
)
def bodosql_e2e_tests(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the BodoSQL tests.
    """
    return request.param


@pytest.mark.bodosql
def test_pipeline_until_relational_masked_sf(
    bodosql_e2e_tests: PyDoughPandasTest,
    bodosql_graphs: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Test transforming queries to the relational plan for the BodoSQL tests.
    """
    file_path: str = get_plan_test_filename(bodosql_e2e_tests.test_name)
    bodosql_e2e_tests.run_relational_test(
        bodosql_graphs,
        file_path,
        update_tests,
    )


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
