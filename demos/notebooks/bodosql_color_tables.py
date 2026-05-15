"""
Logic used to generate the dataframes used for the COLORSHOP database for the
purposes of the BodoSQL demo jupyter notebook. The function
`generate_color_tables` generates the four pandas DataFrames used in this
database: colors, customers, suppliers, and shipments.
"""

import datetime

import numpy as np
import numpy.typing as npt
import pandas as pd


def generate_color_tables():
    color_df: pd.DataFrame = pd.read_csv(
        "../../tests/gen_data/colors.csv",
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
    return color_df, customers_df, suppliers_df, shipments_df
