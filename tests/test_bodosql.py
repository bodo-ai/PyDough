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
from pydough.database_connectors.database_connector import DatabaseDialect
from pydough.metadata import GraphMetadata
from pydough.database_connectors import DatabaseContext

import os
import pydough
from functools import cache
from pyarrow import Table as PyArrowTable
import shutil
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.partitioning import PartitionSpec, PartitionField

from tests.testing_utilities import graph_fetcher
import numpy as np
import numpy.typing as npt

from .testing_utilities import PyDoughPandasTest
from bodosql import BodoSQLContext, FileSystemCatalog
from bodo.io.iceberg.catalog.dir import DirCatalog

from pyiceberg.catalog import WAREHOUSE_LOCATION
from pyiceberg.partitioning import (
    PartitionSpec,
    PartitionField,
    IdentityTransform,
    TruncateTransform,
)
from pyiceberg.table import Table as IcebergTable

from pyarrow.csv import read_csv as pyarrow_read_csv


@pytest.fixture(scope="session")
def bodosql_color_ctx() -> BodoSQLContext:
    """
    BodoSQL Context definition for the COLORSHOP dataset, which is a locally
    defined series of tables:
    - `CLRS`: Each registered paint color in the dataset, which has a name and
      hex code + the corresponding RGB values.
    - `CUST`: The customers registered in the dataset.
    - `SUPLS`: The companies registered in the dataset.
    - `SHPMNTS`: The shipments of colors that have been made, which includes the
       color shipped, the customer and company involved, the date of shipment,
       volume of paint shipped, and the price of the shipment.
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
            "CSTLNAME": [
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
    # Generate the shipments table with 200,000 rows using random generation
    # while maintaining referential integrity with the other three tables and
    # creating some correlations/trends in the data to make the queries more
    # interesting. The random seed is fixed to ensure test reproducibility.
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
            "CUST": customers_df,
            "SUPLS": suppliers_df,
        }
    )
    # Confirm the row counts are as expected
    assert bc.estimated_row_counts == [865, 200000, 20, 5]

    # Manually hack in the NDV values for the various columns, since BodoSQL
    # does not natively support estimated NDVs for locally defined DataFrames.
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
def bodosql_corpus_ctx() -> BodoSQLContext:
    """
    BodoSQL Context definition for the CORPUS dataset, which is defined via
    a filesystem Iceberg catalog and has the following tables:
    - `DICT`: A subset of the english dictionary including various words and
      their definitions, with a word possibly appearing more than once if it has
      multiple parts of speech.
    - `SHAKE`: A subset of the corpus of Shakespeare's works, where each row
      corresponds to a word from a line in his plays, accompanied by the
      act/scene/line/player that it is from.
    """
    # Check if the folder already exists. If so, skip the setup steps.
    # Note: if you need to regenerate the folder, either delete it or
    # temporarily override the boolean below. The regeneration code will then be
    # invoked the first time a test that depends on this fixture is run.
    warehouse_loc: str = f"{os.path.dirname(__file__)}/gen_data/corpus_iceberg"
    regenerate_iceberg: bool = not os.path.exists(warehouse_loc)
    if regenerate_iceberg:
        # Create the directory that will serve as the Iceberg catalog.
        try:
            os.mkdir(warehouse_loc)
            dircat: DirCatalog = DirCatalog(
                "CORPUS_DB", **{WAREHOUSE_LOCATION: warehouse_loc}
            )

            # Read the CSV files as PyArrow tables
            dict_pyarrow: PyArrowTable = pyarrow_read_csv(
                f"{os.path.dirname(__file__)}/gen_data/sampled_dictionary.csv.gz"
            )
            shake_pyarrow: PyArrowTable = pyarrow_read_csv(
                f"{os.path.dirname(__file__)}/gen_data/shakespeare_words.csv.gz"
            )

            # Create the Iceberg table definition for the Dictionary table.
            dict_table: IcebergTable = dircat.create_table(
                "DICT",
                schema=dict_pyarrow.schema,
            )

            # Evolve the table spec to partition on the first letter of the word, the
            # part of speech, and the number of characters.
            (
                dict_table.update_spec()
                .add_field("WORD", "truncate[1]")
                .add_field("POS", "identity")
                .add_field("CCOUNT", "identity")
                .commit()
            )

            # Load the data to the Iceberg table
            dict_table.append(dict_pyarrow)

            # Create the Iceberg table definition for the Shakespeare table, and load
            # the data into it.
            shake_table: IcebergTable = dircat.create_table(
                "SHAKE",
                schema=shake_pyarrow.schema,
            )

            # Evolve the table spec to partition on play, act, scene and player
            (
                shake_table.update_spec()
                .add_field("PLAY", "identity")
                .add_field("ACT", "identity")
                .add_field("SCENE", "identity")
                .add_field("PLAYER", "identity")
                .commit()
            )

            # Load the data to the Iceberg table
            shake_table.append(shake_pyarrow)

        except Exception as e:
            # If any error occurs during the setup, clean up by deleting the
            # warehouse directory if it was created.
            if os.path.exists(warehouse_loc):
                shutil.rmtree(warehouse_loc)
            raise e

    # Create the catalog/context pointing to the location of the database.
    catalog = FileSystemCatalog(warehouse_loc)
    bc: BodoSQLContext = BodoSQLContext(catalog=catalog)

    # If regenerating the database, use BodoSQL to replace each table with
    # itself. Because BodoSQL is being used to write the tables, it will also
    # update the metadata to include puffin files with theta sketches containing
    # the approximate NDV statistics for various columns.
    if regenerate_iceberg:
        # Get all the table names from a DDL command given to the BodoSQL
        # context.
        table_names: list[str] = bc.sql('SHOW TABLES IN "."')["NAME"].tolist()
        for table_name in table_names:
            bc.sql(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {table_name}"
            )

    return bc


@pytest.fixture(scope="session")
def bodosql_sample_contexts(
    bodosql_color_ctx: BodoSQLContext, bodosql_corpus_ctx: BodoSQLContext
) -> dict[str, BodoSQLContext]:
    """
    Mapping of graph names to the various BodoSQL Contexts for the sample
    BodoSQL databases. The supported graph names are:

    * `COLORSHOP`
    * `CORPUS`
    """
    return {
        "COLORSHOP": bodosql_color_ctx,
        "CORPUS": bodosql_corpus_ctx,
    }


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
                            "Dim Gray",
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
        pytest.param(
            # For each color that is pure red, green, or blue (one of r, g, or b
            # is 255 and the other two are 0), who was the first customer to
            # purchase that color, and when did they do so, and from which
            # company? Ignore colors without any such shipment.
            PyDoughPandasTest(
                """
first_order = shipments.BEST(per='colors', by=(ship_date.ASC(), key.ASC()))
result = (
    colors
    .WHERE(((SMALLEST(red, green, blue) == 0) & (LARGEST(red, green, blue) == 255) & ((red + green + blue) == 255)) & HAS(first_order))
    .CALCULATE(
        key,
        hex_code,
        first_customer=JOIN_STRINGS(' ', first_order.customer.first_name, first_order.customer.last_name),
        first_order_date=first_order.ship_date,
        first_order_company=first_order.company.name,
    )
    .ORDER_BY(key.ASC())
)
                """,
                "COLORSHOP",
                lambda: pd.DataFrame(
                    {
                        "key": [
                            "blue",
                            "electric_green",
                            "green_color_wheel_x11_green",
                            "lime_web_x11_green",
                            "red",
                        ],
                        "hex_code": ["#00f", "#0f0", "#0f0", "#0f0", "#f00"],
                        "first_customer": [
                            "Janet Taylor",
                            "Frank Davis",
                            "Grace Miller",
                            "Frank Davis",
                            "Marcel Jackson",
                        ],
                        "first_order_date": [
                            datetime.date(2024, 1, 2),
                            datetime.date(2024, 1, 3),
                            datetime.date(2024, 1, 3),
                            datetime.date(2024, 1, 5),
                            datetime.date(2024, 1, 3),
                        ],
                        "first_order_company": [
                            "Rainbow Inc.",
                            "Hue Depot",
                            "Tint Traders",
                            "Pallette Emporium",
                            "Tint Traders",
                        ],
                    }
                ),
                "color_q12",
                order_sensitive=True,
            ),
            id="color_q12",
        ),
        pytest.param(
            # For each of the 5 selected colors, how many shipments were there
            # of that color? Break down as 1 column per color in a single row.
            PyDoughPandasTest(
                """
count_args = {}
for color_name in color_names:
    count_args[f'n_{color_name}'] = COUNT(shipments.WHERE((color_key == color_name)))
result = COLORSHOP.CALCULATE(**count_args)
                """,
                "COLORSHOP",
                lambda: pd.DataFrame(
                    {
                        "n_avocado": [458],
                        "n_blue_bell": [446],
                        "n_carmine": [377],
                        "n_deep_fuchsia": [314],
                        "n_mode_beige": [181],
                    }
                ),
                "color_q13",
                kwargs={
                    "color_names": [
                        "avocado",
                        "blue_bell",
                        "carmine",
                        "deep_fuchsia",
                        "mode_beige",
                    ]
                },
            ),
            id="color_q13",
        ),
        pytest.param(
            # For each of the 5 selected colors, which company has the cheapest
            # price-per-volume for that color?
            PyDoughPandasTest(
                """
analysis_args = {}
for color_name in color_names:
    best_price_per_volume = (
        shipments.WHERE(color_key == color_name)
        .CALCULATE(price_per_volume=ROUND(price / volume, 2))
        .TOP_K(1, by=(price_per_volume.ASC(), company.name.ASC()))
        .SINGULAR()
    )
    analysis_args[f'cheapest_{color_name}_price'] = best_price_per_volume.price_per_volume
    analysis_args[f'cheapest_{color_name}_company'] = best_price_per_volume.company.name
result = COLORSHOP.CALCULATE(**analysis_args)
                """,
                "COLORSHOP",
                lambda: pd.DataFrame(
                    {
                        "cheapest_brandeis_blue_price": [13.81],
                        "cheapest_brandeis_blue_company": ["Hue Depot"],
                        "cheapest_china_pink_price": [17.08],
                        "cheapest_china_pink_company": ["Chroma Co."],
                        "cheapest_french_raspberry_price": [15.97],
                        "cheapest_french_raspberry_company": ["Tint Traders"],
                        "cheapest_puce_price": [12.97],
                        "cheapest_puce_company": ["Rainbow Inc."],
                    }
                ),
                "color_q14",
                kwargs={
                    "color_names": [
                        "brandeis_blue",
                        "china_pink",
                        "french_raspberry",
                        "puce",
                    ]
                },
            ),
            id="color_q14",
        ),
        pytest.param(
            # How many nouns start with N?
            PyDoughPandasTest(
                "result = CORPUS.CALCULATE(n=COUNT(dictionary.WHERE((part_of_speech == 'n.') & STARTSWITH(word, 'n'))))",
                "CORPUS",
                lambda: pd.DataFrame({"n": [654]}),
                "corpus_q01",
            ),
            id="corpus_q01",
        ),
        pytest.param(
            # Which words starting with "aba" appear at least once in the
            # Shakespeare dialogue sample? List the word, part of speech,
            # and definition. If a word has multiple definitions more than once,
            # pick the one with the longest definition, breaking ties
            # alphabetically and ordering the final result alphabetically.
            PyDoughPandasTest(
                """
result = (
    dictionary
    .WHERE(STARTSWITH(word, 'aba') & HAS(shakespeare_uses))
    .PARTITION(name="words", by=word)
    .dictionary
    .BEST(by=(LENGTH(definition).DESC(), definition.ASC()), per='words')
    .CALCULATE(word, part_of_speech, definition)
    .ORDER_BY(word.ASC())
)
                """,
                "CORPUS",
                lambda: pd.DataFrame(
                    {
                        "word": ["abandoned", "abate"],
                        "part_of_speech": ["a.", "v. t."],
                        "definition": [
                            "Forsaken  deserted.",
                            "To beat down; to overthrow.",
                        ],
                    }
                ),
                "corpus_q02",
            ),
            id="corpus_q02",
        ),
        pytest.param(
            # How many words appear in the Shakespeare dialogue sample of act
            # 5 of Macbeth?
            PyDoughPandasTest(
                "result = CORPUS.CALCULATE(n=COUNT(shakespeare.WHERE((play == 'macbeth') & (act == 5))))",
                "CORPUS",
                lambda: pd.DataFrame({"n": [62]}),
                "corpus_q03",
            ),
            id="corpus_q03",
        ),
        pytest.param(
            # How many words appear in the Shakespeare dialogue sample of act
            # 5 of Macbeth?
            PyDoughPandasTest(
                """
result = (
    shakespeare
    .PARTITION(name="plays", by=play)
    .CALCULATE(play, n_words=COUNT(shakespeare))
    .TOP_K(5, by=n_words.DESC())
)
                """,
                "CORPUS",
                lambda: pd.DataFrame(
                    {
                        "play": [
                            "loves labours lost",
                            "julius caesar",
                            "much ado about nothing",
                            "king john",
                            "troilus and cressida",
                        ],
                        "n_words": [8535, 8150, 7705, 5667, 4695],
                    }
                ),
                "corpus_q04",
            ),
            id="corpus_q04",
        ),
        pytest.param(
            # Which five parts of speech show up the most in the Shakespeare
            # dialogue sample?
            PyDoughPandasTest(
                """
result = (
    dictionary
    .CALCULATE(part_of_speech)
    .shakespeare_uses
    .PARTITION(name="pos", by=part_of_speech)
    .CALCULATE(part_of_speech, n_uses=COUNT(shakespeare_uses))
    .TOP_K(5, by=n_uses.DESC())
)
                """,
                "CORPUS",
                lambda: pd.DataFrame(
                    {
                        "part_of_speech": ["n.", "v. t.", "adv.", "a.", "v. i."],
                        "n_uses": [22081, 11540, 8832, 8807, 8070],
                    }
                ),
                "corpus_q05",
                order_sensitive=True,
            ),
            id="corpus_q05",
        ),
        pytest.param(
            # List each adjective with 10 characters whose first character is
            # 'a' or 'b' that appears in one of the selected plays from the
            # Shakespeare dialogue sample. For each occurrence, note the
            # play, act, scene, line, and player where it is uttered.
            PyDoughPandasTest(
                """
result = (
    dictionary
    .WHERE((word < 'c') & (LENGTH(word) == 10) & (part_of_speech == 'a.'))
    .shakespeare_uses
    .WHERE(ISIN(play, selected_plays))
    .CALCULATE(word, play, act, scene, line, player)
)
                """,
                "CORPUS",
                lambda: pd.DataFrame(
                    {
                        "word": [
                            "acquainted",
                            "acquainted",
                            "beforehand",
                            "benedictus",
                            "benedictus",
                            "benedictus",
                            "beneficial",
                        ],
                        "play": [
                            "merry wives of windsor",
                            "pericles",
                            "king john",
                            "much ado about nothing",
                            "much ado about nothing",
                            "much ado about nothing",
                            "henry viii",
                        ],
                        "act": [2, 4, 5, 3, 3, 3, 1],
                        "scene": [2, 6, 7, 4, 4, 4, 1],
                        "line": [139, 185, 116, 66, 66, 67, 64],
                        "player": [
                            "bardolph",
                            "boult",
                            "bastard",
                            "beatrice",
                            "beatrice",
                            "beatrice",
                            "buckingham",
                        ],
                    }
                ),
                "corpus_q06",
                kwargs={
                    "selected_plays": [
                        "much ado about nothing",
                        "henry viii",
                        "merry wives of windsor",
                        "romeo and juliet",
                        "pericles",
                        "king john",
                        "othello",
                    ]
                },
            ),
            id="corpus_q06",
        ),
        pytest.param(
            # What is the definition of the 20 longest words that appear in
            # the first scene of the first act of any play in the Shakespeare
            # dialogue sample? Include words without a definition, and if they
            # have multiple definitions choose the one with the part of speech
            # that comes first alphabetically. Break ties in word length by
            # alphabetical order of the word.
            PyDoughPandasTest(
                """
first_definition = CROSS(
    dictionary
    .PARTITION(name="words", by=word)
    .dictionary
    .BEST(by=part_of_speech.ASC(), per='words')
).WHERE(word == shakespeare_word).SINGULAR()
result = (
    shakespeare
    .WHERE((act == 1) & (scene == 1))
    .TOP_K(20, by=(LENGTH(word).DESC(), word.ASC()))
    .CALCULATE(shakespeare_word=word)
    .CALCULATE(word, definition=first_definition.definition)
)
                """,
                "CORPUS",
                lambda: pd.DataFrame(
                    {
                        "word": [
                            "northamptonshire",
                            "richardgodamercy",
                            "misinterpreting",
                            "wellexperienced",
                            "magnificencein",
                            "threefarthings",
                            "treasoncharles",
                            "truthbetrothed",
                            "basiliscolike",
                            "communication",
                            "countcardinal",
                            "entertainment",
                            "faulconbridge",
                            "insufficience",
                            "interchanging",
                            "parrotteacher",
                            "procrastinate",
                            "schoolmasters",
                            "understanding",
                            "unintelligent",
                        ],
                        "definition": [
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            "Insufficiency.",
                            "of Interchange",
                            None,
                            None,
                            None,
                            "Knowing; intelligent; skillful; as  he is an understanding man.",
                            None,
                        ],
                    }
                ),
                "corpus_q07",
                order_sensitive=True,
            ),
            id="corpus_q07",
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
    bodosql_sample_contexts: dict[str, DatabaseContext],
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Test transforming queries to SQL for BodoSQL, comparing against expected
    results.
    """

    ctx: DatabaseContext = DatabaseContext(
        bodosql_sample_contexts[bodosql_e2e_tests.graph_name],
        dialect=DatabaseDialect.SNOWFLAKE,
    )
    file_path: str = get_sql_test_filename(bodosql_e2e_tests.test_name, ctx.dialect)
    bodosql_e2e_tests.run_sql_test(
        bodosql_graphs,
        file_path,
        update_tests,
        ctx,
    )


@pytest.mark.bodosql
@pytest.mark.execute
def test_pipeline_e2e_bodosql(
    bodosql_e2e_tests: PyDoughPandasTest,
    bodosql_graphs: graph_fetcher,
    bodosql_sample_contexts: dict[str, DatabaseContext],
):
    """
    Test executing tests end-to-end with BodoSQL, comparing against expected
    results.
    """
    ctx: DatabaseContext = DatabaseContext(
        bodosql_sample_contexts[bodosql_e2e_tests.graph_name],
        dialect=DatabaseDialect.SNOWFLAKE,
    )
    bodosql_e2e_tests.run_e2e_test(
        bodosql_graphs,
        ctx,
        coerce_types=True,
    )
