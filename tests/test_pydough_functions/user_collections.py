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


def simple_range_9():
    # Generate a table with 1 column named `name space`
    return pydough.range_collection('"quoted-name"', '"name space"', 5)


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


def simple_dataframe_collection_1():
    # Generates a simple dataframe collection
    df = pd.DataFrame(
        {
            "idx": range(8),
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
        }
    )
    return pydough.dataframe_collection(
        name="rainbow", dataframe=df, unique_column_names=["idx"]
    )


def simple_dataframe_collection_2():
    # Generates a simple dataframe collection with column_subset
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4],
            "country": ["US", "CR", "US", "MX"],
            "age": [25, 30, 22, 30],
            "signup_date": pd.to_datetime(
                [
                    "2024-01-10",
                    "2024-01-12",
                    "2024-02-01",
                    "2024-02-01",
                ]
            ),
        }
    )
    return pydough.dataframe_collection(
        name="users",
        dataframe=df,
        unique_column_names=["user_id"],
        column_subset=["signup_date", "user_id"],
    )


def simple_dataframe_collection_3():
    # Generates a simple dataframe collection with quoted names and reserved words,
    # and only a subset of the columns
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4],
            '"`name""["': ["Alice", "Bob", "Charlie", "David"],
            '"space country"': ["US", "CR", "US", "MX"],
            '"CAST"': [25, 30, 22, 30],
        }
    )

    return pydough.dataframe_collection(
        name="users",
        dataframe=df,
        unique_column_names=["user_id"],
    )


def simple_dataframe_collection_4():
    # Dataframe collection with a dataframe with invalid datatypes but
    # those are not selected to the final dataframe collection because of
    # the column_subset parameter, so it should work without errors
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4],
            "country": ["US", "CR", "US", "MX"],
            "user_list": [[12, 4], [23, 5], [56, 3], [23, 6, 7]],
            "date": [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}],
        }
    )

    return pydough.dataframe_collection(
        name="users",
        dataframe=df,
        unique_column_names=["user_id"],
        column_subset=["user_id", "country"],
    )


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

    return pydough.dataframe_collection("alldatatypes", df, ["int_col"])


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
    return pydough.dataframe_collection("strings", df_strings, ["normal_strings"])


def dataframe_collection_numbers():
    df_numbers = pd.DataFrame(
        {
            "pyd_numbers": [
                10,
                -3,
                3.56,
                np.nan,
                None,
            ],
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
    return pydough.dataframe_collection("numbers", df_numbers, ["pyd_numbers"])


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
    return pydough.dataframe_collection("infinty", df_inf, ["py_float"])


def dataframe_collection_cross():
    # Get the users and orders dataframe collections
    users_df = pd.DataFrame(
        {
            "id_": range(1, 6),
            "name": ["John", "Jane", "Bob", "Alice", "Charlie"],
        }
    )
    users = pydough.dataframe_collection(
        name="users", dataframe=users_df, unique_column_names=["id_", "name"]
    )

    orders_df = pd.DataFrame(
        {
            "order_id": range(101, 106),
            "user_id": [1, 2, 1, 3, 2],
            "amount": [250.0, 150.5, 300.0, 450.75, 200.0],
        }
    )
    orders = pydough.dataframe_collection(
        name="orders",
        dataframe=orders_df,
        unique_column_names=[["order_id", "user_id"]],
    )

    return (
        users.CALCULATE(id1=id_, name1=name)
        .CROSS(orders)
        .WHERE((id1 == user_id))
        .CALCULATE(id1, name1, order_id, amount)
    )


def dataframe_collection_partition():
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
        name="products_collection",
        dataframe=products_df,
        unique_column_names=["product_id"],
    ).CALCULATE(product_id, product_category, price)

    pricing_rules = pydough.dataframe_collection(
        name="pricing_collection",
        dataframe=pricing_rules_df,
        unique_column_names=["rule_id", "rule_category"],
    ).CALCULATE(rule_category, discount)

    return (
        products.CROSS(pricing_rules)
        .WHERE(product_category == rule_category)
        .PARTITION(name="category", by=product_category)
        .CALCULATE(
            product_category,
            avg_price=AVG(pricing_collection.price),
            n_products=COUNT(pricing_collection),
            min_discount=MIN(pricing_collection.discount),
        )
    )


def dataframe_collection_where():
    # Return all suppliers in that region whose account_balance is greater than
    # the DataFrame value.
    threshold_df = pd.DataFrame(
        {
            "region_name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
            "min_account_balance": [5000.32, 8000, 4600.32, 6400.50, 8999.99],
        }
    )
    thresholds = pydough.dataframe_collection(
        name="thresholds_collection",
        dataframe=threshold_df,
        unique_column_names=["region_name"],
    ).CALCULATE(region_name, min_account_balance)

    filtered_suppliers = suppliers.CALCULATE(
        sup_region_name=nation.region.name, account_balance=account_balance
    )

    return (
        thresholds.CROSS(filtered_suppliers)
        .WHERE(
            (sup_region_name == region_name) & (account_balance > min_account_balance)
        )
        .PARTITION(name="region", by=sup_region_name)
        .CALCULATE(sup_region_name, n_suppliers=COUNT(filtered_suppliers))
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

    thresholds_dates = pydough.dataframe_collection(
        name="dates", dataframe=date_df, unique_column_names=["clerk_id"]
    ).CALCULATE(clerk_id, start_date, end_date)

    return (
        thresholds_dates.CROSS(orders)
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
        name="discounts", dataframe=discounts_df, unique_column_names=["shipping_type"]
    ).CALCULATE(shipping_type, added_discount)

    return (
        disccount_added.CROSS(lines)
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
        name="priority_taxes",
        dataframe=priority_tax_df,
        unique_column_names=["priority_lvl"],
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
    return (
        customers.WHERE(HAS(cheapest_order))
        .CALCULATE(
            name,
            order_key=cheapest_order.key,
            order_priority=cheapest_order.order_priority,
            cheapest_order_price=cheapest_order.tax_price,
        )
        .TOP_K(5, by=cheapest_order_price.ASC())
    )


# Windows functions
def dataframe_collection_window_functions():
    customers_filters_df = pd.DataFrame(
        {
            "nation_name": ["UNITED STATES", "JAPAN", "BRAZIL"],
            "mrk_segment": ["BUILDING", "AUTOMOBILE", "MACHINERY"],
        }
    )
    customers_filters = pydough.dataframe_collection(
        name="customers_filters",
        dataframe=customers_filters_df,
        unique_column_names=["nation_name", "mrk_segment"],
    ).CALCULATE(nation_name, mrk_segment)

    order_date_diff = orders.CALCULATE(
        month_diff=DATEDIFF(
            "months", PREV(order_date, by=order_date.ASC(), per="customers"), order_date
        )
    )

    order_price_diff = orders.CALCULATE(
        price_diff=(
            total_price - NEXT(total_price, by=order_date.ASC(), per="customers")
        )
    )

    return (
        customers_filters.CROSS(customers)
        .WHERE(
            (nation.name == nation_name)
            & (market_segment == mrk_segment)
            & (PERCENTILE(by=account_balance.ASC(), n_buckets=1000) > 996)
        )
        .CALCULATE(
            name,
            ranking_balance=RANKING(by=account_balance.DESC(), per="customers_filters"),
            n_orders=COUNT(orders),
            avg_month_orders=AVG(order_date_diff.month_diff),
            avg_price_diff=AVG(order_price_diff.price_diff),
            proportion=account_balance / RELSUM(account_balance),
            above_avg=IFF(account_balance > RELAVG(account_balance), True, False),
            n_poorer=RELCOUNT(
                account_balance, by=(account_balance.ASC()), cumulative=True
            ),
            ratio=account_balance / RELSIZE(),
        )
        .ORDER_BY(name.ASC())
    )


# Common definition of the dataframes used in the following tests
# All the classes being taught
class_df = pd.DataFrame(
    {
        "key": [15112, 15122, 15150, 15210, 15251],
        "class_name": [
            "Programming Fundamentals",
            "Imperative Programming",
            "Functional Programming",
            "Parallel Algorithms",
            "Theoretical CS",
        ],
        "language": ["Python", "C", "SML", "SML", None],
    }
)

# All the teachers
teacher_df = pd.DataFrame(
    {
        "tid": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        "first_name": ["Anil", "Mike", "Ian", "David"] * 3,
        "last_name": ["Lee"] * 3 + ["Smith"] * 3 + ["Taylor"] * 3 + ["Thomas"] * 3,
    }
)

# All the records of a teacher teaching a class for a specific semester
teaching_df = pd.DataFrame(
    {
        "class_key": [15112, 15122, 15150, 15210, 15251] * 6,
        "teacher_id": [1 + (i**3) % 12 for i in range(30)],
        "semester": (
            ["2020-09-01"] * 5
            + ["2021-02-01"] * 5
            + ["2021-09-01"] * 5
            + ["2022-02-01"] * 5
            + ["2022-09-01"] * 5
            + ["2023-02-01"] * 5
        ),
        "rating": [round(((i + 7.2) ** 4) % 12, 2) for i in range(30)],
    }
)


def dataframe_collection_taught_recently():
    # For each class, which teacher has taught it the most recently?

    class_tbl = pydough.dataframe_collection(
        "classes", class_df, ["key", "class_name"]
    ).CALCULATE(key, class_name, language)

    teacher_tbl = pydough.dataframe_collection(
        "teachers", teacher_df, ["tid", ["first_name", "last_name"]]
    ).CALCULATE(tid, first_name, last_name)

    teaching_tbl = pydough.dataframe_collection(
        "teaching", teaching_df, ["class_key", "teacher_id", "semester"]
    ).CALCULATE(class_key, teacher_id, semester, rating)

    teacher = CROSS(teacher_tbl).WHERE(teacher_id == tid).SINGULAR()
    return (
        class_tbl.CROSS(teaching_tbl)
        .WHERE(class_key == key)
        .BEST(by=semester.DESC(), per="classes")
        .CALCULATE(
            class_name,
            last_semester=semester,
            teacher_first_name=teacher.first_name,
            teacher_last_name=teacher.last_name,
        )
    )


def dataframe_collection_highest_rating():
    # For each class, which teacher had the highest rating when teaching that class?

    class_tbl = pydough.dataframe_collection(
        "classes", class_df, ["key", "class_name"]
    ).CALCULATE(key, class_name, language)

    teacher_tbl = pydough.dataframe_collection(
        "teachers", teacher_df, ["tid", ["first_name", "last_name"]]
    ).CALCULATE(tid, first_name, last_name)

    teaching_tbl = pydough.dataframe_collection(
        "teaching", teaching_df, ["class_key", "teacher_id", "semester"]
    ).CALCULATE(class_key, teacher_id, semester, rating)

    teacher = CROSS(teacher_tbl).WHERE(teacher_id == tid).SINGULAR()
    return (
        class_tbl.CROSS(teaching_tbl)
        .WHERE(class_key == key)
        .BEST(by=rating.DESC(), per="classes")
        .CALCULATE(
            class_name,
            last_semester=semester,
            teacher_first_name=teacher.first_name,
            teacher_last_name=teacher.last_name,
        )
    )


def dataframe_collection_teacher_class():
    # For each teacher, what class+semester have they taught most recently?

    class_tbl = pydough.dataframe_collection(
        "classes", class_df, ["key", "class_name"]
    ).CALCULATE(key, class_name, language)

    teacher_tbl = pydough.dataframe_collection(
        "teachers", teacher_df, ["tid", ["first_name", "last_name"]]
    ).CALCULATE(tid, first_name, last_name)

    teaching_tbl = pydough.dataframe_collection(
        "teaching", teaching_df, ["class_key", "teacher_id", "semester"]
    ).CALCULATE(class_key, teacher_id, semester, rating)

    # For each teacher, what class+semester have they taught most recently?
    semester_class = CROSS(class_tbl).WHERE(class_key == key).SINGULAR()
    return (
        teacher_tbl.CROSS(teaching_tbl)
        .WHERE(teacher_id == tid)
        .BEST(by=semester.DESC(), per="teachers")
        .CALCULATE(
            first_name,
            last_name,
            recent_semester=semester,
            class_name=semester_class.class_name,
        )
    )


def dataframe_collection_teacher_lowest_rating():
    # For each teacher, what class+semester were they the lowest rated when
    # teaching?

    class_tbl = pydough.dataframe_collection(
        "classes", class_df, ["key", "class_name"]
    ).CALCULATE(key, class_name, language)

    teacher_tbl = pydough.dataframe_collection(
        "teachers", teacher_df, ["tid", ["first_name", "last_name"]]
    ).CALCULATE(tid, first_name, last_name)

    teaching_tbl = pydough.dataframe_collection(
        "teaching", teaching_df, ["class_key", "teacher_id", "semester"]
    ).CALCULATE(class_key, teacher_id, semester, rating)

    # For each teacher, what class+semester were they the lowest rated when
    # teaching?
    semester_class = CROSS(class_tbl).WHERE(class_key == key).SINGULAR()
    return (
        teacher_tbl.CROSS(teaching_tbl)
        .WHERE(teacher_id == tid)
        .BEST(by=rating.DESC(), per="teachers")
        .CALCULATE(first_name, last_name, rating, class_name=semester_class.class_name)
    )


def dataframe_collection_language_highest_rating():
    # For each programming language, what is the teacher who received the
    # highest rating when teaching a class in that language?

    class_tbl = pydough.dataframe_collection(
        "classes", class_df, ["key", "class_name"]
    ).CALCULATE(key, class_name, language)

    teacher_tbl = pydough.dataframe_collection(
        "teachers", teacher_df, ["tid", ["first_name", "last_name"]]
    ).CALCULATE(tid, first_name, last_name)

    teaching_tbl = pydough.dataframe_collection(
        "teaching", teaching_df, ["class_key", "teacher_id", "semester"]
    ).CALCULATE(class_key, teacher_id, semester, rating)

    # For each programming language, what is the teacher who received the
    # highest rating when teaching a class in that language?
    languages_taught = (
        class_tbl.CROSS(teaching_tbl)
        .WHERE((class_key == key) & PRESENT(language))
        .PARTITION(name="languages", by=language)
    )
    teacher = CROSS(teacher_tbl).WHERE(teacher_id == tid).SINGULAR()
    return (
        languages_taught.teaching.CALCULATE(teacher_id)
        .BEST(by=rating.DESC(), per="languages")
        .CALCULATE(
            language, rating, first_name=teacher.first_name, last_name=teacher.last_name
        )
    )


def dataframe_collection_teacher_count():
    # For each teacher, count how many different teachers have taught the same
    # class as them at some point.

    teacher_tbl = pydough.dataframe_collection(
        "teachers", teacher_df, ["tid", ["first_name", "last_name"]]
    ).CALCULATE(tid, first_name, last_name)

    teaching_tbl = pydough.dataframe_collection(
        "teaching", teaching_df, ["class_key", "teacher_id", "semester"]
    )

    # For each teacher, count how many different teachers have taught the same
    # class as them at some point.
    return (
        teaching_tbl.CALCULATE(class_1=class_key, teacher_1=teacher_id)
        .CROSS(teaching_tbl.CALCULATE(class_key, teacher_id))
        .WHERE((class_1 == class_key) & (teacher_1 != teacher_id))
        .CROSS(teacher_tbl)
        .WHERE(tid == teacher_1)
        .PARTITION(name="classes", by=(first_name, last_name))
        .CALCULATE(first_name, last_name, n_teachers=COUNT(teachers))
    )


def dataframe_collection_unique_partition():
    teacher_tbl = pydough.dataframe_collection(
        "teachers", teacher_df, ["tid", ["first_name", "last_name"]]
    ).CALCULATE(tid, first_name, last_name)

    return teacher_tbl.PARTITION(name="names", by=(first_name, last_name))


def dataframe_collection_correlation():
    # For each class, how many different classes are taught in the same language?

    classes_collection = pydough.dataframe_collection(
        "classes", class_df, ["key", "class_name"]
    )

    other_classes_same_language = CROSS(
        classes_collection.CALCULATE(language, key)
    ).WHERE((language == original_language) & (key != original_key))

    return classes_collection.CALCULATE(
        original_language=language, original_key=key
    ).CALCULATE(
        class_name, language, n_other_classes=COUNT(other_classes_same_language)
    )


# BAD TESTS
def dataframe_collection_bad_1():
    # Different column sizes
    df_bad = pd.DataFrame(
        {
            "col1": [1, 2, 3],
            "col2": ["a", "b"],
        }
    )
    return pydough.dataframe_collection("bad_df_1", df_bad, ["col1"])


def dataframe_collection_bad_2():
    # Different datatypes within same column
    df_bad = pd.DataFrame(
        {
            "col1": [1, "two", 3],
            "col2": ["a", "b", "c"],
            "col3": ["a", None, "c"],
        }
    )
    return pydough.dataframe_collection("bad_df_2", df_bad, ["col1"])


def dataframe_collection_bad_3():
    # Empty column
    df_col_empty = pd.DataFrame(
        {
            "col1": [],
            "col2": [],
        }
    )
    return pydough.dataframe_collection("empty_col_df", df_col_empty, ["col1"])


def dataframe_collection_bad_4():
    # Empty dataframe
    df_empty = pd.DataFrame({})
    return pydough.dataframe_collection("empty_df", df_empty, ["id"])


def dataframe_collection_bad_5():
    # Unsupported datatypes (array, struct, map, etc)
    df_unsupported = pd.DataFrame(
        {
            "col1": [[1, 2], [3, 4], [5, 6]],  # list/array type
        }
    )
    return pydough.dataframe_collection("unsupported_df", df_unsupported, ["col1"])


def dataframe_collection_bad_6():
    # Unsupported datatypes (array, struct, map, etc)
    df_unsupported = pd.DataFrame(
        {
            "col1": [{"a": 1}, {"b": 2}, {"c": 3}],  # dict/map type
        }
    )
    return pydough.dataframe_collection("unsupported_df", df_unsupported, ["col1"])


def dataframe_collection_bad_7():
    # Unexisting column in unique column names list
    df_unsupported = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["John", "Jane", "Mike", "David", "Tom"],
        }
    )
    return pydough.dataframe_collection("unsupported_df", df_unsupported, ["col1"])


def dataframe_collection_bad_8():
    # column_subset not a list
    df_unsupported = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["John", "Jane", "Mike", "David", "Tom"],
        }
    )
    return pydough.dataframe_collection(
        "unsupported_df", df_unsupported, ["id"], "no_list"
    )


def dataframe_collection_bad_9():
    # column_subset not a list of strings
    df_unsupported = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["John", "Jane", "Mike", "David", "Tom"],
        }
    )
    return pydough.dataframe_collection(
        "unsupported_df", df_unsupported, ["id"], ["id", 2, "no_string"]
    )


def dataframe_collection_bad_10():
    # Wrong column_subset (not in the dataframe)
    df_unsupported = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["John", "Jane", "Mike", "David", "Tom"],
        }
    )
    return pydough.dataframe_collection(
        "unsupported_df", df_unsupported, ["id"], ["id", "no_exists"]
    )


def dataframe_collection_bad_11():
    # Wrong unique_column_names (not in column_subset)
    df_unsupported = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["John", "Jane", "Mike", "David", "Tom"],
        }
    )
    return pydough.dataframe_collection(
        "unsupported_df", df_unsupported, ["id"], ["name"]
    )


def dataframe_collection_bad_12():
    # Missing unique column names (not in column_subset)
    df_unsupported = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["John", "Jane", "Mike", "David", "Tom"],
        }
    )
    return pydough.dataframe_collection("unsupported_df", df_unsupported)


def dataframe_collection_bad_13():
    # columns_subset include repeats
    df_unsupported = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["John", "Jane", "Mike", "David", "Tom"],
        }
    )
    return pydough.dataframe_collection(
        "unsupported_df", df_unsupported, ["id"], ["id", "id"]
    )


def dataframe_collection_bad_14():
    # Invalid dataframe column names
    df_unsupported = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "1thcolumn": ["John", "Jane", "Mike"],
            "column-name": ["Lee", "Smith", "Thomas"],
            "user@name": ["johnl2", "jasm10", "thomas01"],
        }
    )
    return pydough.dataframe_collection(
        "unsupported_df",
        df_unsupported,
        ["id"],
    )


def dataframe_collection_bad_15():
    # The unique columns is an empty list
    df_unsupported = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["John", "Jane", "Mike", "David", "Tom"],
        }
    )
    return pydough.dataframe_collection(
        "unsupported_df",
        df_unsupported,
        [],
    )


def dataframe_collection_bad_16():
    # The unique columns contains an empty list (e.g. [] or ["A", []])
    df_unsupported = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["John", "Jane", "Mike", "David", "Tom"],
        }
    )
    return pydough.dataframe_collection(
        "unsupported_df",
        df_unsupported,
        ["id", []],
    )


def dataframe_collection_bad_17():
    # Columns are not valid sql identifiers
    df_unsupported = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            1: ["John", "Jane", "Mike", "David", "Tom"],
        }
    )
    return pydough.dataframe_collection(
        "unsupported_df",
        df_unsupported,
        ["id"],
    )
