"""
Definitions of various fixtures used in PyDough tests that are automatically
available.
"""

import datetime
import sqlite3

import numpy as np
import pandas as pd


def gen_product_info_records(rng: np.random.Generator) -> dict[int, dict]:
    # Build the product records with each product having a unique ID and
    # building a random name, choosing a brand/type with some correlation,
    # as well as a random release date and a price correlated to the
    # brand/type combination.
    products: dict[int, dict] = {}
    types: list[str] = ["phone", "laptop", "tablet", "accessory"]
    brands: list[str] = ["OrangeRectangle", "Dreadnought", "AstralSea", "Zenith"]
    words_a = [
        "Cardinal",
        "Gold",
        "Sapphire",
        "Emerald",
        "Onyx",
        "Amethyst",
        "Ruby",
        "",
    ]
    words_b = [
        "Carbon",
        "Copper",
        "Beat",
        "Sky",
        "Bolt",
        "Scream",
        "Sun",
        "Void",
        "Flame",
    ]
    words_c = [
        "Vision",
        "Set",
        "Unit",
        "Fox",
        "Dawn",
        "Star",
        "Wave",
        "Flare",
        "Core",
        "I",
        "II",
        "III",
        "IV",
        "V",
    ]
    names = set()
    for _ in range(143):
        pr_id = int(rng.integers(100000, 999999))
        while True:
            wa = rng.choice(words_a)
            wb = rng.choice(words_b)
            wc = rng.choice(words_c)
            pr_name = f"{wa}{wb}-{wc}"
            if pr_name not in names:
                names.add(pr_name)
                break
        brand_idx = rng.choice(
            [rng.integers(0, len(brands)), min(rng.integers(0, len(brands), 2))]
        )
        type_idx = min(rng.integers(0, len(types)), max(1 + brand_idx, 3))
        pr_brand = brands[brand_idx]
        pr_type = types[type_idx]
        pr_price = round(
            rng.integers(
                2 ** (brand_idx - type_idx + 5), 4 ** (brand_idx - type_idx + 5)
            )
            + rng.random(),
            2,
        )
        a1, a2, a3, a4 = rng.integers(735120, 739120, 4)
        pr_release = datetime.date.fromordinal(
            rng.choice([min(a1, a2, a3), max(a2, a3, a4)])
        )
        products[pr_id] = {
            "name": pr_name,
            "type": pr_type,
            "brand": pr_brand,
            "price": pr_price,
            "release": pr_release,
        }
    return products


def gen_devices_inf_records(
    rng: np.random.Generator,
    products: dict[int, dict],
    users: dict[int, dict],
    countries: dict[int, str],
) -> dict[int, dict]:
    country_codes = sorted(countries)
    product_codes = sorted(products)
    user_ids = sorted(users)
    # First, build a matrix for the price of each product when produced in each country.
    # Start this as the price of production times as a scaling factor, and add some random
    # fuzziness to it, also amplify the cost for certain countries over others.
    price_matrix = np.zeros((len(products), len(countries)), dtype=np.float64)
    for product_idx, product_id in enumerate(product_codes):
        base_price: float = products[product_id]["price"]
        price_matrix[product_idx, :] = base_price * 1.5
    for country_idx in range(len(country_codes)):
        price_matrix[:, country_idx] *= 2.0 - (country_idx / len(country_codes)) * 0.5
    price_matrix *= (
        np.clip(
            1,
            np.abs(rng.normal(loc=0, scale=0.8, size=(len(products), len(countries)))),
            None,
        )
        ** 2
    )
    # Build the device records with each device having a unique ID and
    # a product ID, factory country ID, purchase country ID and a user ID.
    devices: dict[int, dict] = {}
    for _ in range(4000):
        de_id: int = 1000001
        while de_id in devices:
            de_id = int(rng.integers(1000000, 9999999))
        i1, i2, i3, i4 = rng.integers(0, len(product_codes), 4)
        pr_idx: int = int(rng.choice([max(min(i1, i2), min(i3, i4)), i1, i2]))
        pr_id: int = product_codes[pr_idx]
        store_co_idx: int = int(rng.integers(0, len(country_codes), 1))
        factory_co_idx: int = int(
            rng.choice([store_co_idx, rng.integers(0, len(country_codes))])
        )
        store_co_id: int = country_codes[store_co_idx]
        factory_co_id: int = country_codes[factory_co_idx]
        us_idx = int(rng.integers(0, len(user_ids), 1))
        purchase_us_id: int = user_ids[us_idx]
        release_date: pd.Timestamp = pd.Timestamp(products[pr_id]["release"])
        delta: int = min(rng.integers(0, (pd.Timestamp("2025") - release_date).days, 3))
        minutes = min(max(0, round(rng.normal(700, 180))), 1440)
        purchase_ts: pd.Timestamp = min(
            [release_date + pd.Timedelta(days=delta), pd.Timestamp("2025")]
        ) + pd.Timedelta(minutes=minutes)
        warranty: str | None = rng.choice(
            [None] * 5 + ["ALPHA"] * 5 + ["svx"] * 3 + ["OMEGA"]
        )
        if products[pr_id]["type"] == "phone":
            warranty = rng.choice([warranty, "ALPHA", "OMEGA"])
        if products[pr_id]["type"] == "accessory":
            warranty = rng.choice([warranty, None])
        expiration: str | None = None
        if warranty is not None:
            delta = rng.integers(100, 1000)
            if warranty == "OMEGA":
                delta = rng.choice([delta, rng.integers(100, 2000)])
            if warranty == "svx":
                delta = min([delta, rng.integers(100, 1000)])
            expiration: pd.Timestamp = purchase_ts + pd.Timedelta(days=delta)
            expiration: datetime.date = datetime.date(
                expiration.year, expiration.month, expiration.day
            )
        tax_rate = np.round(
            rng.choice(
                [
                    0.0,
                    store_co_idx / (2 * len(countries)),
                    (rng.random() ** ((store_co_idx + 1) / 3)),
                ]
            )
            ** 2,
            2,
        )
        raw_cost: float = price_matrix[pr_idx, store_co_idx]
        cost = np.round(raw_cost * (1 + tax_rate), 2)
        tax: float = np.round(raw_cost * tax_rate, 2)
        devices[de_id] = {
            "pr_id": pr_id,
            "store_co_id": store_co_id,
            "factory_co_id": factory_co_id,
            "purchase_us_id": purchase_us_id,
            "purchase_ts": purchase_ts,
            "cost": cost,
            "tax": tax,
            "warranty": warranty,
            "expiration": expiration,
        }
    return devices


def gen_product_country_error_densities(
    rng: np.random.Generator, products: dict[int, dict], countries: dict[int, str]
) -> np.ndarray:
    product_codes = sorted(products)
    # Build up the probability density grids for reports
    product_country_error_densities = np.abs(
        rng.normal(loc=0, scale=0.2, size=(len(products), len(countries)))
    )
    for product_idx in range(len(products)):
        product_country_error_densities[
            product_idx, (product_idx + 2) % len(countries)
        ] **= 2
        year = products[product_codes[product_idx]]["release"].year
        product_country_error_densities[product_idx, :] **= 1 / ((2027 - year) / 3)
        product_country_error_densities[product_idx, :] **= min(2 * rng.random(2) + 1)
        product_country_error_densities[product_idx, :] **= rng.choice([1] * 10 + [0.1])
    for product_idx in rng.integers(0, len(products), 5):
        product_country_error_densities[product_idx, :] **= 0.1
    for product_idx in rng.integers(0, len(products), 10):
        product_country_error_densities[product_idx, :] **= 0.1
    for batch_size in range(10, 70, 10):
        for product_idx in rng.integers(0, len(products), batch_size):
            product_country_error_densities[product_idx, :] **= 2.5
    for country_idx in range(len(countries)):
        for i in range(0, len(products), int(len(products) ** 0.5)):
            product_country_error_densities[
                (country_idx + i) % len(products), country_idx
            ] **= 0.1
            product_country_error_densities[
                ((country_idx + i) ** 2) % len(products), country_idx
            ] **= 0.1
    product_country_error_densities[:, len(countries) - 1] **= 0.5
    product_country_error_densities[:, len(countries) // 2] **= 1.25
    product_country_error_densities[:, 0] **= 1.5
    product_country_error_densities[:, 1] /= 2
    product_country_error_densities[:] **= 1.5
    return product_country_error_densities


def gen_product_country_error_distributions(
    rng: np.random.Generator,
    products: dict[int, dict],
    countries: dict[int, str],
    errors: dict[int, dict],
) -> np.ndarray:
    product_country_error_distribution = rng.normal(
        loc=1, scale=2, size=(len(products), len(countries), len(errors))
    )
    product_country_error_distribution **= 2
    for product_idx in range(len(products)):
        product_country_error_distribution[
            product_idx, product_idx % len(countries), product_idx % len(errors)
        ] += 2
    for country_idx in range(len(countries)):
        product_country_error_distribution[:, country_idx, country_idx] += 3
        product_country_error_distribution[country_idx, :, country_idx] += 3
        product_country_error_distribution[country_idx, country_idx, country_idx] += 3
        product_country_error_distribution[:country_idx] += 3
    product_country_error_distribution *= np.abs(
        rng.normal(loc=0, scale=1, size=(len(products), len(countries), len(errors)))
    )
    product_country_error_distribution **= 3
    for product_idx in range(len(products)):
        for country_idx in range(len(countries)):
            product_country_error_distribution[product_idx, country_idx, :] /= np.sum(
                product_country_error_distribution[product_idx, country_idx, :]
            )
    return product_country_error_distribution


def gen_incident_info_records(
    rng: np.random.Generator,
    products: dict[int, dict],
    countries: dict[int, str],
    errors: dict[int, dict],
    devices: dict[int, dict],
    pce_densities: np.ndarray,
    pce_distributions: np.ndarray,
) -> dict[int, dict]:
    pr_codes = sorted(products)
    co_codes = sorted(countries)
    er_codes = sorted(errors)
    # Build up the incident reports based on the densities
    incidents: dict[int, dict] = {}
    for device_id in devices:
        device_info = devices[device_id]
        pr_id = device_info["pr_id"]
        co_id = device_info["factory_co_id"]
        purchase_ts: pd.Timestamp = device_info["purchase_ts"]
        error_density: float = pce_densities[
            pr_codes.index(pr_id), co_codes.index(co_id)
        ]
        repair_id = int(rng.choice([co_id, co_id, rng.choice(co_codes)]))
        years_since = max(1, (pd.Timestamp("2025") - purchase_ts).days // 365)
        for yr_idx in range(4 * years_since - 1):
            if rng.random() < error_density:
                in_id: int = 100000000
                while in_id in incidents:
                    in_id: int = int(rng.integers(100000000, 999999999, 1))
                delta: int = max(rng.integers(0, 90 * (1 + yr_idx), 2))
                incident_ts = purchase_ts + pd.Timedelta(days=delta)
                er_id: int = int(
                    rng.choice(
                        er_codes,
                        p=pce_distributions[
                            pr_codes.index(pr_id), co_codes.index(co_id), :
                        ],
                    )
                )
                repair_cost: float = products[pr_id]["price"] * (rng.random() * 0.4)
                if errors[er_id]["type"] == "hardware":
                    if errors[er_id]["name"] == "Battery Failure":
                        repair_cost *= 3.0
                    else:
                        repair_cost *= 1.5
                    repair_cost *= 2.0
                if errors[er_id]["type"] == "software":
                    repair_cost *= 0.5
                warranty: str | None = device_info["warranty"]
                expiration: datetime.date | None = device_info["expiration"]
                if (
                    warranty is not None
                    and expiration is not None
                    and incident_ts < pd.Timestamp(expiration)
                ):
                    if warranty == "ALPHA":
                        repair_cost = 0.0
                    elif warranty == "OMEGA":
                        repair_cost *= 0.5
                    else:
                        repair_cost *= 0.07
                repair_cost = np.round(repair_cost, 2)
                incidents[in_id] = {
                    "device": device_id,
                    "pr_id": pr_id,
                    "repair_id": repair_id,
                    "error_id": er_id,
                    "incident_ts": incident_ts,
                    "cost": repair_cost,
                }
    return incidents


def gen_technograph_records(cursor: sqlite3.Cursor) -> None:
    """
    TODO
    """
    rng = np.random.default_rng(42)

    # Build the country records, with each country having a unique ID
    countries: dict[int, str] = {
        int(rng.integers(1000, 9999)): country
        for country in ["US", "CA", "MX", "FR", "JP", "CN"]
    }

    # Build the error records, with each error having a unique ID
    errors: dict[int, dict] = {
        int(rng.integers(100, 999)): {"name": error, "type": typ}
        for error, typ in [
            ("Cracked Screen", "hardware"),
            ("Battery Failure", "hardware"),
            ("Software Bug", "software"),
            ("Network Issue", "network"),
            ("Overheating", "hardware"),
            ("Charging Port Issue", "hardware"),
            ("Camera Malfunction", "hardware"),
            ("Speaker Issue", "hardware"),
            ("Microphone Issue", "hardware"),
            ("Display Issue", "hardware"),
        ]
    }

    products = gen_product_info_records(rng)
    devices = gen_devices_inf_records(rng, products, {0: {}}, countries)
    pce_densities = gen_product_country_error_densities(rng, products, countries)
    pce_distributions = gen_product_country_error_distributions(
        rng, products, countries, errors
    )
    incidents = gen_incident_info_records(
        rng, products, countries, errors, devices, pce_densities, pce_distributions
    )

    # Insert everything into the database
    for co_id in sorted(countries):
        co_name = countries[co_id]
        country_record = (co_id, co_name)
        cursor.execute(
            f"INSERT INTO countries VALUES ({', '.join(['?'] * len(country_record))})",
            country_record,
        )

    for er_id in sorted(errors):
        error_info = errors[er_id]
        error_record = (er_id, error_info["name"], error_info["type"])
        cursor.execute(
            f"INSERT INTO errors VALUES ({', '.join(['?'] * len(error_record))})",
            error_record,
        )

    for pr_id in sorted(products, key=lambda x: products[x]["release"]):
        product_info = products[pr_id]
        product_record = (
            pr_id,
            product_info["name"],
            product_info["type"],
            product_info["brand"],
            product_info["price"],
            product_info["release"],
        )
        cursor.execute(
            f"INSERT INTO products VALUES ({', '.join(['?'] * len(product_record))})",
            product_record,
        )

    for de_id in sorted(devices):
        device_info = devices[de_id]
        device_record = (
            de_id,
            device_info["pr_id"],
            device_info["store_co_id"],
            device_info["factory_co_id"],
            device_info["purchase_us_id"],
            device_info["purchase_ts"].to_pydatetime(),
            device_info["cost"],
            device_info["tax"],
            device_info["warranty"],
            device_info["expiration"],
        )
        cursor.execute(
            f"INSERT INTO devices VALUES ({', '.join(['?'] * len(device_record))})",
            device_record,
        )

    for in_id in sorted(incidents, key=lambda x: incidents[x]["incident_ts"]):
        incident_info = incidents[in_id]
        incident_record = (
            in_id,
            incident_info["device"],
            incident_info["repair_id"],
            incident_info["error_id"],
            incident_info["incident_ts"].to_pydatetime(),
            incident_info["cost"],
        )
        cursor.execute(
            f"INSERT INTO incidents VALUES ({', '.join(['?'] * len(incident_record))})",
            incident_record,
        )
