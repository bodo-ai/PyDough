"""
Various functions containing user generated collections as
PyDough code snippets for testing purposes.
"""
# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pydough
import pandas as pd


def template_simple_call():
    # Top 5 customers with most orders from 1996 where price is greater then 3000
    return customers.CALCULATE(
        key,
        n_orders=orders_filter_count((total_price > 3000) & (YEAR(order_date) == 1996)),
    ).TOP_K(5, by=(n_orders.DESC(), key.ASC()))


def template_api_simple_call():
    # Which nations has the most customers with orders above 3000
    selected_orders = pydough.call_template(
        "orders_filter_count", labels={"orders_filter": "Price above 3000"}
    )
    return nations.CALCULATE(
        name, n_customers=COUNT(customers.WHERE(selected_orders > 0))
    ).TOP_K(3, by=(n_customers.DESC(), name.ASC()))


def template_follow_up_call():
    # Top/bottom segment comparison
    orders_segmentation = pydough.call_template(
        "orders_revenue_by", labels={"arg_year": "Year 1997", "arg_dimension": "Month"}
    )

    return top_bottom_comparison(orders_segmentation, AVG(orders.revenue))


def template_literal_1():

    min_account_balance = pydough.call_template(
        "multiply_by_2", labels={"base_number": "Year 1992"}
    )

    selected_customers = customers.WHERE((account_balance >= min_account_balance))
    return TPCH.CALCULATE(n_custs=COUNT(selected_customers))


def template_recursive_call():

    return TPCH.CALCULATE(
        y_1994=cumulative_orders_counter(1994, 1994),
        y_1994_1996=cumulative_orders_counter(1994, 1996),
        y_1994_1998=cumulative_orders_counter(1994, 1998),
    )


def template_cross_collection():
    selected_regions = regions.WHERE(MONOTONIC(1, key, 5)).CALCULATE(key, name)

    cross_collection = range_cross_collection(
        selected_regions, "new_range", 3, 9, (key * 2 == idx)
    )

    return cross_collection.CALCULATE(idx, key, name)


def template_dataframe_collection():
    # Template that creates a dataframe collection

    generated_collection = pydough.call_template(
        "generate_df_collection", labels={"collection_name": "NAME1", "col1": "COLORS"}
    )

    return generated_collection


def template_df_collection_df():
    # Template that receives a df, build a df collection and cross it with orders

    input_df = pd.DataFrame(
        {
            "cust_id": [1, 2, 3],
            "customer_name": ["customer_1", "customer_2", "customer_3"],
        }
    )

    df_collection = dataframe_input_collection(
        "customers_collection", input_df, ["cust_id"]
    )

    selected_orders = orders.WHERE(ISIN(key, (1, 2, 3))).CALCULATE(key, clerk)

    return (
        df_collection.CALCULATE(cust_id, customer_name)
        .CROSS(selected_orders)
        .WHERE(cust_id == key)
        .CALCULATE(cust_id, customer_name, key, clerk)
    )


def template_datetime_days():

    base_date = pd.to_datetime("1996-12-01")

    return (
        orders.WHERE((order_date == DATETIME(base_date)))
        .CALCULATE(key, order_date, date_plus_days=add_datetime_days(base_date, 10))
        .TOP_K(5, by=key.ASC())
    )


def template_datetime_months():

    base_date = pd.to_datetime("1995-10-11")

    return (
        orders.WHERE((order_date == DATETIME(base_date)))
        .CALCULATE(key, order_date, date_plus_months=add_datetime_months(base_date, 10))
        .TOP_K(5, by=key.ASC())
    )
