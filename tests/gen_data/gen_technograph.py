"""
Logic to generate the data for the TechnoGraph database schema
"""

import datetime
import sqlite3

import numpy as np
import pandas as pd


def gen_product_info_records(rng: np.random.Generator) -> dict[int, dict]:
    """
    Builds a dictionary of product records, each with a unique ID and
    associated attributes such as name, type, brand, price, and release date.
    The various attributes are generated with randomness but with some
    correlation to each other and uneven distributions.

    Args:
        rng (np.random.Generator): A random number generator instance.

    Returns:
        dict[int, dict]: A dictionary containing product records, where
        the keys are product IDs and the values are dictionaries with
        product attributes.
    """
    # Build the product records with each product having a unique ID and
    # building a random name, choosing a brand/type with some correlation,
    # as well as a random release date and a price correlated to the
    # brand/type combination.
    products: dict[int, dict] = {}
    types: list[str] = ["phone", "laptop", "tablet", "accessory"]
    brands: list[str] = ["OrangeRectangle", "Dreadnought", "AstralSea", "Zenith"]
    words_a: list[str] = [
        "Cardinal",
        "Gold",
        "Sapphire",
        "Emerald",
        "Onyx",
        "Amethyst",
        "Ruby",
        "",
    ]
    words_b: list[str] = [
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
    words_c: list[str] = [
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
    names: set[str] = set()

    # Repeat the process of generating product records 143 times
    for _ in range(143):
        # Generate a random identifier for the product
        pr_id: int = 999999
        while pr_id in products:
            pr_id = int(rng.integers(100000, 999999))

        # Generate a unique name for the product
        # by combining random words from the lists
        while True:
            wa: str = rng.choice(words_a)
            wb: str = rng.choice(words_b)
            wc: str = rng.choice(words_c)
            pr_name: str = f"{wa}{wb}-{wc}"
            if pr_name not in names:
                names.add(pr_name)
                break

        # Choose a random brand for the product, slightly unevenly distributed
        brand_idx: int = int(
            rng.choice(
                [rng.integers(0, len(brands)), min(rng.integers(0, len(brands), 2))]
            )
        )
        pr_brand: str = brands[brand_idx]

        # Choose a random type for the product, slightly unevenly distributed
        # where the distribution is slightly affected by the brand.
        type_idx: int = int(min(rng.integers(0, len(types)), max(1 + brand_idx, 3)))
        pr_type: str = types[type_idx]

        # Choose a production cost for the product uniformly from within a
        # range that is arbitrarily changed depending on the brand/type combination.
        pr_price: float = round(
            rng.integers(
                2 ** (brand_idx - type_idx + 5), 4 ** (brand_idx - type_idx + 5)
            )
            + rng.random(),
            2,
        )
        # Choose a random release date for the product, slightly unevenly by
        # picking 4 random dates from within the range and either choosing the
        # smallest of the first 3 or the largest of the last 3.
        a1, a2, a3, a4 = rng.integers(735120, 739120, 4)
        pr_release: datetime.date = datetime.date.fromordinal(
            rng.choice([min(a1, a2, a3), max(a2, a3, a4)])
        )

        # Insert the product record into the dictionary
        products[pr_id] = {
            "name": pr_name,
            "type": pr_type,
            "brand": pr_brand,
            "price": pr_price,
            "release": pr_release,
        }

    return products


def gen_user_info_records(
    rng: np.random.Generator, countries: dict[int, str]
) -> dict[int, dict]:
    """
    Randomly generates user records, each with a unique ID and associated
    attributes such as name, country ID, birthdate, and account registration
    date.
    """
    users: dict[int, dict] = {}
    first_names: list[str] = [
        "John",
        "Jane",
        "Alex",
        "Chris",
        "Taylor",
        "Kimberly",
        "Jordan",
        "Riley",
        "Maria",
        "Mary",
        "James",
        "Taylor",
        "Rashid",
    ]
    surnames: list[str] = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Davis",
        "Rodriguez",
        "Lopez",
        "Wilson",
        "Anderson",
        "Lee",
        "Chen",
        "Miller",
    ]
    names: set[str] = set()

    for _ in range(950):
        # Generate a random identifier for the user
        us_id: int = 99999999
        while us_id in users:
            us_id = int(rng.integers(10000000, 99999999))

        # Generate a random name for the user by combining random words
        # from the lists in one of several ways.
        user_name: str
        while True:
            first_name: str = rng.choice(first_names)
            surname: str = rng.choice(surnames)
            user_name = rng.choice(
                [
                    f"{first_name}{surname}",
                    f"{first_name}_{surname}".lower(),
                    f"{first_name}{surname}".upper(),
                    f"{first_name}{rng.integers(10, 100)}",
                    f"{surname}{rng.integers(100, 1000)}".lower(),
                    f"{first_name}{surname}{rng.integers(100, 1000)}".upper(),
                    f"{surname}{first_name}_{rng.integers(10, 100)}".lower(),
                ]
            )
            if user_name not in names:
                names.add(user_name)
                break

        # Choose a random country ID for the user, slightly unevenly
        # distributed
        co_idx: int = int(max(rng.integers(0, len(countries), 2)))
        co_id: int = list(countries.keys())[co_idx]

        # Choose a random birthdate for the user via a normal distribution
        # that gets slightly skewed towards smaller years in the range
        # (late 1960s to late 1990s)
        birthdate: datetime.date = datetime.date.fromordinal(
            int(min(rng.normal(725000, 2500, 2)))
        )

        # Choose a random account registration date between 2014 and 2025,
        # skewed towards earlier years.
        reg_date: datetime.date = datetime.date.fromordinal(
            int(min(rng.integers(735234, 739252, 2)))
        )

        # Insert the user record into the dictionary
        users[us_id] = {
            "name": user_name,
            "country_id": co_id,
            "birthdate": birthdate,
            "registration_date": reg_date,
        }

    return users


def gen_devices_info_records(
    rng: np.random.Generator,
    products: dict[int, dict],
    users: dict[int, dict],
    countries: dict[int, str],
) -> dict[int, dict]:
    """
    Builds a dictionary of device records, each with a unique ID and
    associated attributes such as product ID, store country ID, factory
    country ID, purchase user ID, purchase timestamp, cost, tax, warranty
    type, and warranty expiration date. The various attributes are generated
    with randomness but with some correlation to each other and uneven
    distributions.

    Args:
        rng (np.random.Generator): A random number generator instance.
        products (dict[int, dict]): A dictionary containing product records.
        users (dict[int, dict]): A dictionary containing user records.
        countries (dict[int, str]): A dictionary containing country records.

    Returns:
        dict[int, dict]: A dictionary containing device records, where
        the keys are device IDs and the values are dictionaries with
        device attributes.
    """
    country_codes = sorted(countries)
    product_codes = sorted(products)
    user_ids = sorted(users)

    # First, build a matrix for the price of each product when produced in each country.
    # Start this as the price of production times as a scaling factor, and add some random
    # fuzziness to it, also amplify the cost for certain countries over others.
    price_matrix: np.ndarray = np.zeros(
        (len(products), len(countries)), dtype=np.float64
    )
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

    # Repeat the process of generating device records 3500 times.
    devices: dict[int, dict] = {}
    for _ in range(3500):
        # Generate a random identifier for the device
        de_id: int = 1000001
        while de_id in devices:
            de_id = int(rng.integers(1000000, 9999999))

        # Randomly choose a product ID for the device, slightly unevenly distributed.
        i1, i2, i3, i4 = rng.integers(0, len(product_codes), 4)
        pr_idx: int = int(rng.choice([max(min(i1, i2), min(i3, i4)), i1, i2]))
        pr_id: int = product_codes[pr_idx]

        # Randomly choose a purchase user ID, slightly unevenly distributed.
        us_idx = int(
            rng.choice(
                [
                    min(rng.integers(0, len(user_ids), 2)),
                    rng.integers(0, len(user_ids)),
                    max(rng.integers(0, len(user_ids), 2)),
                ]
            )
        )
        purchase_us_id: int = user_ids[us_idx]
        user_country_id: int = users[purchase_us_id]["country_id"]
        user_country_idx: int = country_codes.index(user_country_id)

        # Randomly choose a store country ID either as the same as the
        # user country id, or another randomly selected one.
        store_co_idx: int = int(
            rng.choice(
                [
                    user_country_idx,
                    user_country_idx,
                    rng.integers(0, len(country_codes)),
                ]
            )
        )
        store_co_id: int = country_codes[store_co_idx]

        # Randomly choose a factory country ID as either the same as the store
        # country or another randomly selected one.
        factory_co_idx: int = int(
            rng.choice(
                [store_co_idx, store_co_idx, rng.integers(0, len(country_codes))]
            )
        )
        factory_co_id: int = country_codes[factory_co_idx]

        # Randomly choose a purchase timestamp by picking the smallest of 3
        # dates uniformly distributed within between the release date and 2025.
        # Choose the time of day of the purchased based on a normal distribution
        # around 700 minutes after midnight, with a standard deviation of 180 minutes.
        release_date: pd.Timestamp = pd.Timestamp(products[pr_id]["release"])
        delta: int = min(rng.integers(0, (pd.Timestamp("2025") - release_date).days, 3))
        minutes = max(0, round(rng.normal(700, 180)))
        purchase_ts: pd.Timestamp = min(
            [release_date + pd.Timedelta(days=delta), pd.Timestamp("2025")]
        ) + pd.Timedelta(minutes=minutes)
        purchase_ts = max(
            purchase_ts, pd.Timestamp(users[purchase_us_id]["registration_date"])
        )

        # Add a 5% chance of the purchase date being being moved to the user's
        # next birthday.
        if rng.random() < 0.05 and purchase_ts.year < 2024:
            birth_date: datetime.date = users[purchase_us_id]["birthdate"]
            purchase_ts = pd.Timestamp(
                f"{purchase_ts.year + 1}-{birth_date.month:02}-01"
            ) + pd.Timedelta(
                days=birth_date.day - 1,
                hours=purchase_ts.hour,
                minutes=purchase_ts.minute,
            )

        # Choose a warranty type for the device, slightly unevenly distributed
        # between no warrant, ALPHA, svx and OMEGA. Further warp the distribution
        # for certain product types.
        warranty: str | None = rng.choice(
            [None] * 5 + ["ALPHA"] * 5 + ["svx"] * 3 + ["OMEGA"]
        )
        if products[pr_id]["type"] == "phone":
            warranty = rng.choice([warranty, "ALPHA", "OMEGA"])
        if products[pr_id]["type"] == "accessory":
            warranty = rng.choice([warranty, None])

        # If there is a warranty, randomly choose an expiration date
        # uniformly after the purchase date, but for certain warranty
        # types further warp the distribution of how many days after the
        # expiration date it is.
        expiration: datetime.date | None = None
        if warranty is not None:
            delta = rng.integers(100, 1000)
            if warranty == "OMEGA":
                delta = rng.choice([delta, rng.integers(100, 2000)])
            if warranty == "svx":
                delta = min([delta, rng.integers(100, 1000)])
            expiration_ts: pd.Timestamp = purchase_ts + pd.Timedelta(days=delta)
            expiration = datetime.date(
                expiration_ts.year, expiration_ts.month, expiration_ts.day
            )

        # Choose a tax rate for the device purchase in a random manner with
        # different modalities that are slightly affected by the store country.
        tax_rate: float = np.round(
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

        # Compute the cost & tax for the device based on the price matrix
        # and the tax rate, rounding to 2 decimal places.
        raw_cost: float = price_matrix[pr_idx, store_co_idx]
        cost = np.round(raw_cost * (1 + tax_rate), 2)
        tax: float = np.round(raw_cost * tax_rate, 2)

        # Insert the device record into the dictionary
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
    """
    Builds a matrix of product-country error densities, where each
    entry represents the probability density of an error occurring for
    a specific product when produced in a specific country. These probability
    densities are generated randomness, but with some abnormalities injected
    to simulate scenarios where certain products and/or producing countries
    are more likely to have errors.

    Args:
        rng (np.random.Generator): A random number generator instance.
        products (dict[int, dict]): A dictionary containing product records.
        countries (dict[int, str]): A dictionary containing country records.

    Returns:
        np.ndarray: A 2D array where each row corresponds to a product and
        each column corresponds to a country. The values in the array
        represent the probability density of an error occurring for that
        product in that country.
    """
    product_codes = sorted(products)

    # Start the density matrix with the absolute value of a normal distribution
    # with a mean of 0 and a standard deviation of 0.2
    product_country_error_densities = np.abs(
        rng.normal(loc=0, scale=0.2, size=(len(products), len(countries)))
    )

    # For each product, warp the density matrix in multiple ways:
    # - Reduce the density for one arbitrary country
    # - Increase the density based on how long ago the product was released
    # - Increase/Decrease the density by some random amounts
    for product_idx in range(len(products)):
        product_country_error_densities[
            product_idx, (product_idx + 2) % len(countries)
        ] **= 2
        year = products[product_codes[product_idx]]["release"].year
        product_country_error_densities[product_idx, :] **= 1 / ((2027 - year) / 3)
        product_country_error_densities[product_idx, :] **= min(2 * rng.random(2) + 1)
        product_country_error_densities[product_idx, :] **= rng.choice([1] * 10 + [0.1])

    # For a batch of 5 and 10 random products, increase the density for each
    # product in the batch.
    for product_idx in rng.integers(0, len(products), 5):
        product_country_error_densities[product_idx, :] **= 0.1
    for product_idx in rng.integers(0, len(products), 10):
        product_country_error_densities[product_idx, :] **= 0.1

    # For random batches of 10, 20, 30, 40, 50, and 60 products, decrease
    # the density for each product in the batch
    for batch_size in range(10, 70, 10):
        for product_idx in rng.integers(0, len(products), batch_size):
            product_country_error_densities[product_idx, :] **= 2.5

    # For each country, increase the density for several arbitrary
    # products.
    for country_idx in range(len(countries)):
        for i in range(0, len(products), int(len(products) ** 0.5)):
            product_country_error_densities[
                (country_idx + i) % len(products), country_idx
            ] **= 0.1
            product_country_error_densities[
                ((country_idx + i) ** 2) % len(products), country_idx
            ] **= 0.1

    # Arbitrarily increase/decrease the density for certain countries
    product_country_error_densities[:, len(countries) - 1] **= 0.5
    product_country_error_densities[:, len(countries) // 2] **= 1.25
    product_country_error_densities[:, 0] **= 1.5
    product_country_error_densities[:, 1] /= 2

    # Decrease the overall density slightly
    product_country_error_densities[:] **= 1.5

    # Return the density matrix
    return product_country_error_densities


def gen_product_country_error_distributions(
    rng: np.random.Generator,
    products: dict[int, dict],
    countries: dict[int, str],
    errors: dict[int, dict],
) -> np.ndarray:
    """
    Build a probability distribution for the different errors that can occur
    based on each product/country combination. The distribution is generated
    with randomness, but with some correlation to the product/country
    combination and some abnormalities injected to simulate scenarios where
    certain products and/or producing countries are more likely to have
    certain errors instead of others.
    """
    # Start the distribution matrix with random normal values as weights,
    # squared to make them positive.
    product_country_error_distribution = rng.normal(
        loc=1, scale=2, size=(len(products), len(countries), len(errors))
    )
    product_country_error_distribution **= 2

    # For each product, increase the weight of an arbitrary error
    # for an arbitrary country.
    for product_idx in range(len(products)):
        product_country_error_distribution[
            product_idx, product_idx % len(countries), product_idx % len(errors)
        ] += 2

    # For each country, increase the weight of some arbitrary combinations
    # of product/country/error.
    for country_idx in range(len(countries)):
        product_country_error_distribution[:, country_idx, country_idx] += 3
        product_country_error_distribution[country_idx, :, country_idx] += 3
        product_country_error_distribution[country_idx, country_idx, country_idx] += 3
        product_country_error_distribution[:country_idx] += 3

    # Multiply the weights by another random normal distribution to add some
    # more fuzziness.
    product_country_error_distribution *= np.abs(
        rng.normal(loc=0, scale=1, size=(len(products), len(countries), len(errors)))
    )

    # Cube all the weights to amplify differences between them.
    product_country_error_distribution **= 3

    # Finally, normalize the weights to create a probability distribution for
    # each product/country combination.
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
) -> dict[int, dict]:
    """
    Generate incident records based on the product/country error densities
    and distributions. Each incident record contains information about the
    device, repair ID, error ID, incident timestamp, and repair cost. The
    records are generated with randomness, but with some correlation to the
    product/country combination and some abnormalities injected to simulate
    scenarios where certain products and/or producing countries are more
    likely to have certain errors instead of others.

    Args:
        rng (np.random.Generator): A random number generator instance.
        products (dict[int, dict]): A dictionary containing product records.
        countries (dict[int, str]): A dictionary containing country records.
        errors (dict[int, dict]): A dictionary containing error records.
        devices (dict[int, dict]): A dictionary containing device records.

    Returns:
        dict[int, dict]: A dictionary containing incident records, where
        the keys are incident IDs and the values are dictionaries with
        incident attributes.
    """
    pr_codes = sorted(products)
    co_codes = sorted(countries)
    er_codes = sorted(errors)

    # Generate the density/distribution matrices to derive the probability that
    # a device from a certain product/country combination will have an error, and
    # of which type of error it will be.
    pce_densities = gen_product_country_error_densities(rng, products, countries)
    pce_distributions = gen_product_country_error_distributions(
        rng, products, countries, errors
    )

    # Iterate across every device and generate incidents for it based on its
    # error density.
    incidents: dict[int, dict] = {}
    for device_id in devices:
        device_info = devices[device_id]
        pr_id = device_info["pr_id"]
        co_id = device_info["factory_co_id"]
        store_co_id = device_info["store_co_id"]
        purchase_ts: pd.Timestamp = device_info["purchase_ts"]
        error_density: float = pce_densities[
            pr_codes.index(pr_id), co_codes.index(co_id)
        ]
        # Amplify the error density if the store/factory country are different
        if co_id != store_co_id:
            error_density **= 0.9
        years_since = max(1, (pd.Timestamp("2025") - purchase_ts).days // 365)

        # Iterate a number of times based on the number of years since the
        # purchase date. For each iteration, randomly decide whether to generate
        # an incident based on the error density and the product/country error
        # distribution.
        for yr_idx in range(4 * years_since - 1):
            if rng.random() < error_density:
                # Generate a random incident ID that is not already in use.
                in_id: int = 100000000
                while in_id in incidents:
                    in_id = int(rng.integers(100000000, 999999999))

                # Choose a random repair ID, either the same as where the device
                # was purchased, made, or another random one.
                repair_id = int(rng.choice([co_id, store_co_id, rng.choice(co_codes)]))

                # Generate a random incident timestamp based on the purchase
                # timestamp, with the randomness distributed further in the
                # future based on which iteration it is.
                delta: int = max(rng.integers(0, 90 * (1 + yr_idx), 2))
                incident_ts = purchase_ts + pd.Timedelta(days=delta)

                # Choose a random error ID with probability weights from the
                # product & production country.
                er_id: int = int(
                    rng.choice(
                        er_codes,
                        p=pce_distributions[
                            pr_codes.index(pr_id), co_codes.index(co_id), :
                        ],
                    )
                )

                # Choose a random repair cost based on the product price, multiplied
                # by a random scale factor between 0.0 and 0.4, and further
                # increased/decreased based on the error type.
                repair_cost: float = products[pr_id]["price"] * (rng.random() * 0.4)
                if errors[er_id]["type"] == "hardware":
                    if errors[er_id]["name"] == "Battery Failure":
                        repair_cost *= 3.0
                    else:
                        repair_cost *= 1.5
                    repair_cost *= 2.0
                if errors[er_id]["type"] == "software":
                    repair_cost *= 0.5

                # If the device has a warranty that is still valid, apply a discount
                # to the repair cost based on the warranty type.
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

                # Insert the incident record into the dictionary
                incidents[in_id] = {
                    "device": device_id,
                    "pr_id": pr_id,
                    "repair_id": repair_id,
                    "error_id": er_id,
                    "incident_ts": incident_ts,
                    "cost": repair_cost,
                }

    # For each incident report, have a 25% chance of reducing the time between
    # the incident and 2026 by 80%.
    for in_id in incidents:
        incident_info = incidents[in_id]
        incident_ts = incident_info["incident_ts"]
        if rng.random() < 0.25:
            incident_ts = (
                pd.Timestamp("2026") - (pd.Timestamp("2026") - incident_ts) * 0.2
            )
            incident_info["incident_ts"] = incident_ts

    return incidents


def gen_technograph_records(cursor: sqlite3.Cursor) -> None:
    """
    Populates the TechnoGraph sqlite database with generated data.

    Args:
        cursor (sqlite3.Cursor): A sqlite3 cursor object to execute SQL commands,
        which should be connected to where the TechnoGraph database is located.
    """

    # Create the random number generator used for the data generation, with
    # a fixed seed for reproducibility.
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

    # Generate the products, devices, users, and incidents records
    products: dict[int, dict] = gen_product_info_records(rng)
    users: dict[int, dict] = gen_user_info_records(rng, countries)
    devices: dict[int, dict] = gen_devices_info_records(rng, products, users, countries)
    incidents: dict[int, dict] = gen_incident_info_records(
        rng, products, countries, errors, devices
    )

    # Register the adaptors for date/datetime
    sqlite3.register_adapter(datetime.date, lambda d: d.isoformat())
    sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat())

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

    for us_id in sorted(users, key=lambda x: (users[x]["registration_date"], x)):
        user_info = users[us_id]
        user_record = (
            us_id,
            user_info["name"],
            user_info["country_id"],
            user_info["birthdate"],
            user_info["registration_date"],
        )
        cursor.execute(
            f"INSERT INTO users VALUES ({', '.join(['?'] * len(user_record))})",
            user_record,
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

    # Synthesize the CALENDAR record with every date from
    # 2010-01-01 to 2025-12-31
    for date_ordinal in range(733773, 739616):
        cursor.execute(
            "INSERT INTO calendar VALUES (?)",
            (datetime.date.fromordinal(date_ordinal),),
        )

    cursor.connection.commit()
