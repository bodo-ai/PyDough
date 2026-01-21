"""
Various functions containing user generated collections as
PyDough code snippets for testing purposes.
"""
# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pydough
import pandas as pd
import numpy as np
from decimal import Decimal

import pytest

# Snowflake only.
# Other dialects does not support range collections yet.
pytestmark = pytest.mark.snowflake


def simple_range_1():
    # Generates a table with column named `value` containing integers from 0 to 9.
    return pydough.range_collection(
        "simple_range",
        "value",
        10,  # end value
    )


def simple_range_2():
    # Generates a table with column named `value` containing integers from 0 to 9,
    # ordered in descending order.
    return pydough.range_collection(
        "simple_range",
        "value",
        10,  # end value
    ).ORDER_BY(value.DESC())


def simple_range_3():
    # Generates a table with column named `foo` containing integers from 15 to
    # 20 exclusive, ordered in ascending order.
    return pydough.range_collection("T1", "foo", 15, 20).ORDER_BY(foo.ASC())


def simple_range_4():
    #  Generate a table with 1 column named `N` counting backwards
    # from 10 to 1 (inclusive)
    return pydough.range_collection("T2", "N", 10, 0, -1).ORDER_BY(N.ASC())


def simple_range_5():
    # Generate a table with 1 column named `x` which is an empty range
    return pydough.range_collection("T3", "x", -1)


def user_range_collection_1():
    # Creates a collection `sizes` with a single property `part_size` whose values are the
    # integers from 1 (inclusive) to 100 (exclusive), skipping by 5s, then for each size value,
    # counts how many turquoise parts have that size.
    sizes = pydough.range_collection("sizes", "part_size", 1, 100, 5)
    turquoise_parts = parts.WHERE(CONTAINS(name, "turquoise"))
    return sizes.CALCULATE(part_size).CALCULATE(
        part_size, n_parts=COUNT(CROSS(turquoise_parts).WHERE(size == part_size))
    )


def user_range_collection_2():
    # Generate two tables with one column: `a` has a column `x` of digits 0-9,
    # `b` has a column `y` of every even number from 0 to 1000 (inclusive), and for
    # every row of `a` count how many rows of `b` have `x` has a prefix of `y`, and
    # how many have `x` as a suffix of `y`
    table_a = pydough.range_collection("a", "x", 10)
    table_b = pydough.range_collection("b", "y", 0, 1001, 2)
    result = (
        table_a.CALCULATE(x)
        .CALCULATE(
            x,
            n_prefix=COUNT(CROSS(table_b).WHERE(STARTSWITH(STRING(y), STRING(x)))),
            n_suffix=COUNT(CROSS(table_b).WHERE(ENDSWITH(STRING(y), STRING(x)))),
        )
        .ORDER_BY(x.ASC())
    )
    return result


def user_range_collection_3():
    # Same as user_range_collection_2 but only includes rows of x that
    # have at least one prefix/suffix max
    table_a = pydough.range_collection("a", "x", 10)
    table_b = pydough.range_collection("b", "y", 0, 1001, 2)
    prefix_b = CROSS(table_b).WHERE(STARTSWITH(STRING(y), STRING(x)))
    suffix_b = CROSS(table_b).WHERE(ENDSWITH(STRING(y), STRING(x)))
    return (
        table_a.CALCULATE(x)
        .CALCULATE(
            x,
            n_prefix=COUNT(prefix_b),
            n_suffix=COUNT(suffix_b),
        )
        .WHERE(HAS(prefix_b) & HAS(suffix_b))
        .ORDER_BY(x.ASC())
    )


def user_range_collection_4():
    # For every part size 1-10, find the name &
    # retail price of the cheapest part of that size that
    # is azure, plated, and has a small drum container
    sizes = pydough.range_collection("sizes", "part_size", 1, 11)
    azure_parts = parts.WHERE(
        CONTAINS(name, "azure")
        & CONTAINS(part_type, "PLATED")
        & CONTAINS(container, "SM DRUM")
    )
    return (
        sizes.CALCULATE(part_size)
        .CROSS(azure_parts)
        .WHERE(size == part_size)
        .BEST(per="sizes", by=retail_price.ASC())
        .CALCULATE(part_size, name, retail_price)
        .ORDER_BY(part_size.ASC())
    )


def user_range_collection_5():
    # Creates a collection `sizes` with a single property `part_size` whose values are the
    # integers from 1 (inclusive) to 60 (exclusive), skipping by 5s, then for each size value,
    # counts how many almond parts are in the interval of 5 sizes starting with that size
    sizes = pydough.range_collection("sizes", "part_size", 1, 60, 5)
    almond_parts = parts.WHERE(CONTAINS(name, "almond"))
    return sizes.CALCULATE(part_size).CALCULATE(
        part_size,
        n_parts=COUNT(
            CROSS(almond_parts).WHERE(MONOTONIC(part_size, size, part_size + 4))
        ),
    )


def user_range_collection_6():
    # For every year from 1990 to 2000, how many orders were made in that year
    # by a Japanese customer in the automobile market segment, processed by clerk 925
    years = pydough.range_collection("years", "year", 1990, 2001)
    selected_orders = orders.WHERE(
        (clerk == "Clerk#000000925")
        & (customer.market_segment == "AUTOMOBILE")
        & (customer.nation.name == "JAPAN")
    ).CALCULATE(order_year=YEAR(order_date))
    order_years = selected_orders.PARTITION(name="yrs", by=(order_year, customer_key))
    return (
        years.CALCULATE(year)
        .CALCULATE(year, n_orders=COUNT(CROSS(order_years).WHERE(order_year == year)))
        .ORDER_BY(year.ASC())
    )


def simple_dataframe_1():
    # Generates a simple dataframe collection
    df = pd.DataFrame(
        {
            "color": [
                "red",
                "orange",
                "yellow",
                "green",
                "blue",
                "indigo",
                "violet",
                None,
            ],
            "idx": range(8),
        }
    )
    return pydough.dataframe_collection(name="rainbow", dataframe=df)


def dataframe_collection_datatypes():
    df = pd.DataFrame(
        {
            "string_col": [
                "red",
                "orange",
                None,
            ],
            "int_col": pd.Series(range(3), dtype="int64"),
            "float_col": [
                1.5,
                2.0,
                np.nan,
            ],
            "nullable_int_col": pd.Series(
                [1, None, 7],
            ),
            "bool_col": pd.Series([True, False, False], dtype="int64"),
            "null_col": [None] * 3,
            "datetime_col": pd.to_datetime(["2024-01-01", "2024-01-02", None]),
        }
    )

    return pydough.dataframe_collection("alldatatypes", df)


def dataframe_collection_strings():
    df_strings = pd.DataFrame(
        {
            "normal_strings": [
                "hello",
                "world",
                "pydough",
                None,
                "test_string",
            ],
            "empty_string": [
                "",
                "not_empty",
                "",
                None,
                " ",
            ],
            "special_characters": [
                "'simple quoted'",
                '"double quoted"',
                "unicode_ß_ç_ü",
                None,
                "tap_space\tnewline_\n_test",
            ],
        }
    )
    return pydough.dataframe_collection("strings", df_strings)


def dataframe_collection_numbers():
    df_numbers = pd.DataFrame(
        {
            "py_float": [
                1.5,
                0.0,
                10.0001,
                -2.25,
                None,
            ],
            "np_float64": np.array(
                [
                    1.5,
                    0.0,
                    4.4444444,
                    -2.25,
                    None,
                ],
                dtype="float64",
            ),
            "np_float32": np.array(
                [
                    1.5,
                    3.33333,
                    0.0,
                    -2.25,
                    None,
                ],
                dtype="float32",
            ),
            "null_vs_nan": [
                None,
                np.nan,
                float("nan"),
                1.0,
                0.0,
            ],
            "decimal_val": [
                Decimal("1.50"),
                Decimal("0.00"),
                Decimal("-2.25"),
                Decimal("NaN"),
                None,
            ],
        }
    )
    return pydough.dataframe_collection("numbers", df_numbers)


def dataframe_collection_inf():
    df_inf = pd.DataFrame(
        {
            "py_float": [
                1.5,
                float("nan"),
                float("inf"),
                float("-inf"),
            ],
            "np_float64": np.array(
                [
                    -2.25,
                    np.nan,
                    np.inf,
                    -np.inf,
                ],
                dtype="float64",
            ),
            "np_float32": np.array(
                [
                    0.0,
                    np.nan,
                    np.inf,
                    -np.inf,
                ],
                dtype="float32",
            ),
        }
    )
    return pydough.dataframe_collection("infinty", df_inf)


def dataframe_collection_cross():
    # Get the users and orders dataframe collections
    users_df = pd.DataFrame(
        {
            "id_": range(1, 6),
            "name": ["John", "Jane", "Bob", "Alice", "Charlie"],
        }
    )
    users = pydough.dataframe_collection(name="users", dataframe=users_df)

    orders_df = pd.DataFrame(
        {
            "order_id": range(101, 106),
            "user_id": [1, 2, 1, 3, 2],
            "amount": [250.0, 150.5, 300.0, 450.75, 200.0],
        }
    )
    orders = pydough.dataframe_collection(name="orders", dataframe=orders_df)

    return (
        users.CALCULATE(id1=id_, name1=name)
        .CROSS(orders)
        .WHERE((id1 == user_id))
        .CALCULATE(id1, name1, order_id, amount)
    )


def dataframe_collection_partition():
    # CHECK THIS TEST
    products_df = pd.DataFrame(
        {
            "product_id": range(1, 5),
            "product_category": ["A", "B", "A", "B"],
            "price": [17.99, 45.65, 15, 10.99],
        }
    )
    pricing_rules_df = pd.DataFrame(
        {
            "rule_id": range(1, 4),
            "rule_category": ["A", "B", "C"],
            "discount": [0.10, 0.15, 0.05],
        }
    )

    products = pydough.dataframe_collection(
        name="products_collection", dataframe=products_df
    )

    pricing_rules = pydough.dataframe_collection(
        name="pricing_collection", dataframe=pricing_rules_df
    )

    pricing_calc = pricing_rules.CALCULATE(rule_category, discount)
    products_calc = products.CALCULATE(product_id, product_category, price)

    return (
        products.CALCULATE(product_id, product_category, price)
        .CROSS(pricing_calc)
        .WHERE(product_category == rule_category)
        .PARTITION(name="category", by=product_category)
        .CALCULATE(
            product_category,
            avg_price=AVG(products.price),
            n_products=COUNT(products),
            min_discount=MIN(pricing_calc.discount),
        )
    )
    # my_regions = regions.CALCULATE(region_id=key, region_name=name)
    # my_nations = nations.CALCULATE(nation=name, n_region_id=region.key)
    # return (
    #     my_regions.CROSS(my_nations)
    #     .WHERE(region_id == n_region_id)
    #     .PARTITION(name="nation_region", by=region_name)
    #     .CALCULATE(
    #         region_name=region_name,
    #         n_nations=COUNT(my_nations)
    #     )
    # )


def dataframe_collection_where():
    # Return all suppliers in that region whose account_balance is greater than
    # the DataFrame value.
    threshold_df = pd.DataFrame(
        {
            "region_name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
            "min_account_balance": [5000.32, 8000, 4600.32, 6400.50, 8999.99],
        }
    )
    thresholds = pydough.dataframe_collection(name="thresholds", dataframe=threshold_df)

    filtered_suppliers = suppliers.CALCULATE(
        sup_region_name=nation.region.name, account_balance=account_balance
    )

    return (
        thresholds.CALCULATE(region_name, min_account_balance)
        .CROSS(filtered_suppliers)
        .WHERE(
            (sup_region_name == region_name) & (account_balance > min_account_balance)
        )
        .PARTITION(name="region", by=sup_region_name)
        .CALCULATE(sup_region_name, n_suppliers=COUNT(suppliers))
    )


def dataframe_collection_where_date():
    # Given a DataFrame defining (clerk_id, start_date, end_date),
    # return how many orders from the clerk falls within that range.
    date_df = pd.DataFrame(
        {
            "clerk_id": ["Clerk#000000456", "Clerk#000000743", "Clerk#000000547"],
            "start_date": pd.to_datetime(["1996-01-01", "1995-06-01", "1995-11-01"]),
            "end_date": pd.to_datetime(["1996-02-01", "1995-07-01", "1995-12-01"]),
        }
    )

    thresholds_dates = pydough.dataframe_collection(name="dates", dataframe=date_df)

    return (
        thresholds_dates.CALCULATE(
            clerk_id,
            start_date,
            end_date,
        )
        .CROSS(orders)
        .WHERE(
            (clerk_id == clerk)
            & (MONOTONIC(start_date, DATETIME(order_date), end_date))
        )
        .PARTITION(name="clerk_orders", by=clerk_id)
        .CALCULATE(clerk_id, n_orders=COUNT(orders))
    )


def dataframe_collection_top_k():
    discounts_df = pd.DataFrame(
        {
            "shipping_type": ["REG AIR", "SHIP", "TRUCK"],
            "added_discount": [0.05, 0.06, 0.05],
        }
    )

    disccount_added = pydough.dataframe_collection(
        name="discounts", dataframe=discounts_df
    )

    return (
        disccount_added.CALCULATE(shipping_type, added_discount)
        .CROSS(lines)
        .WHERE(shipping_type == ship_mode)
        .CALCULATE(
            part.name,
            shipping_type,
            extended_price,
            added_discount=discount + added_discount,
            final_price=extended_price * (1 - (discount + added_discount)),
        )
        .TOP_K(5, by=final_price.ASC())
    )


def dataframe_collection_best():
    priority_tax_df = pd.DataFrame(
        {
            "priority_lvl": ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED"],
            "tax_rate": [0.05, 0.04, 0.03, 0.02],
        }
    )
    priority_taxes_collection = pydough.dataframe_collection(
        name="priority_taxes", dataframe=priority_tax_df
    )
    cheapest_order = (
        orders.CALCULATE(key, order_priority, total_price)
        .CROSS(priority_taxes_collection)
        .WHERE(order_priority == priority_lvl)
        .CALCULATE(
            order_priority, key, tax_price=(total_price + total_price * tax_rate)
        )
        .BEST(by=tax_price.ASC(), per="orders")
        .SINGULAR()
    )
    return customers.CALCULATE(
        name,
        order_key=cheapest_order.key,
        order_priority=cheapest_order.order_priority,
        cheapest_order_price=cheapest_order.tax_price,
    ).TOP_K(5, by=cheapest_order_price.ASC())
