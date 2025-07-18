"""
Various functions containing PyDough code snippets for testing purposes.
"""
# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pandas as pd
import datetime


def simple_scan():
    return orders.CALCULATE(key)


def simple_filter():
    return orders.CALCULATE(o_orderkey=key, o_totalprice=total_price).WHERE(
        o_totalprice < 1000.0
    )


def simple_scan_top_five():
    return orders.CALCULATE(key).TOP_K(5, by=key.ASC())


def simple_filter_top_five():
    return (
        orders.CALCULATE(key, total_price)
        .WHERE(total_price < 1000.0)
        .TOP_K(5, by=key.DESC())
    )


def order_info_per_priority():
    # Find information about the highest total price order for each priority
    # type in 1992. Specifically, for each order priority, the key & total
    # price of the order. Order the results by priority.
    prios = orders.WHERE(YEAR(order_date) == 1992).PARTITION(
        name="priorities", by=order_priority
    )
    return (
        prios.orders.WHERE(RANKING(by=total_price.DESC(), per="priorities") == 1)
        .CALCULATE(order_priority, order_key=key, order_total_price=total_price)
        .ORDER_BY(order_priority.ASC())
    )


def dumb_aggregation():
    return nations.TOP_K(2, by=name.ASC()).CALCULATE(
        nation_name=name,
        a1=MIN(region.name),
        a2=MAX(region.name),
        a3=SUM(region.key),
        a4=COUNT(KEEP_IF(region.key, region.name != "AMERICA")),
        a5=COUNT(region),
        a6=AVG(region.key),
        a7=ANYTHING(region.name),
        a8=MEDIAN(region.key),
    )


def simple_collation():
    return (
        suppliers.CALCULATE(
            p=PERCENTILE(
                by=(
                    COUNT(supply_records).ASC(),
                    name,
                    address,
                    nation_key,
                    phone,
                    account_balance.DESC(),
                    comment,
                )
            ),
            r=RANKING(
                by=(
                    key,
                    COUNT(supply_records),
                    name.DESC(),
                    address,
                    nation_key,
                    phone,
                    account_balance.ASC(),
                    comment,
                )
            ),
        )
        .ORDER_BY(
            COUNT(supply_records).ASC(),
            name,
            address,
            nation_key,
            phone,
            account_balance.DESC(),
            comment,
        )
        .TOP_K(
            5,
            by=(
                key,
                COUNT(supply_records),
                name.DESC(),
                address,
                nation_key,
                phone,
                account_balance.ASC(),
                comment,
            ),
        )
    )


def absurd_window_per():
    return (
        regions.nations.customers.orders.lines.supplier.nation.suppliers.nation.customers.nation.region.CALCULATE(
            w1=RELSIZE(per="regions"),
            w2=RELSIZE(per="nations:1"),
        )
        .CALCULATE(
            w3=RELSIZE(per="customers:2"),
            w4=RELSIZE(per="orders:1"),
        )
        .CALCULATE(
            w5=RELSIZE(per="lines"),
            w6=RELSIZE(per="supplier:1"),
        )
        .CALCULATE(
            w7=RELSIZE(per="nation:3"),
            w8=RELSIZE(per="suppliers:1"),
        )
        .CALCULATE(
            w9=RELSIZE(per="nation:2"),
            w10=RELSIZE(per="customers:1"),
        )
        .CALCULATE(
            w11=RELSIZE(per="nation:1"),
            w12=RELSIZE(),
        )
    )


def absurd_partition_window_per():
    return (
        lines.PARTITION(
            name="groups", by=(ship_mode, ship_date, status, return_flag, part_key)
        )
        .PARTITION(name="groups", by=(ship_mode, ship_date, status, return_flag))
        .PARTITION(name="groups", by=(ship_mode, ship_date, status))
        .PARTITION(name="groups", by=(ship_mode, ship_date))
        .PARTITION(name="groups", by=(ship_mode))
        .groups.groups.groups.groups.lines.order.CALCULATE(
            w1=RELSIZE(per="groups:1"),
            w2=RELSIZE(per="groups:2"),
            w3=RELSIZE(per="groups:3"),
            w4=RELSIZE(per="groups:4"),
            w5=RELSIZE(per="groups:5"),
        )
    )


def year_month_nation_orders():
    # Finds the 5 largest instances of numbers of orders made in a month of a
    # year by customers in a nation, only looking at nations from Asia and
    # Africa.
    selected_regions = regions.WHERE(ISIN(name, ("ASIA", "AFRICA")))
    urgent_orders = (
        selected_regions.nations.CALCULATE(nation_name=name)
        .customers.orders.WHERE(order_priority == "1-URGENT")
        .CALCULATE(
            nation_name,
            order_year=YEAR(order_date),
            order_month=MONTH(order_date),
        )
    )
    groups = urgent_orders.PARTITION(
        name="groups", by=(nation_name, order_year, order_month)
    )
    return groups.CALCULATE(
        nation_name, order_year, order_month, n_orders=COUNT(orders)
    ).TOP_K(5, by=n_orders.DESC())


def parts_quantity_increase_95_96():
    # Find the 3 parts with the largest increase in quantity ordered by
    # rail from 1995 to 1996, breaking ties alphabetically by name.
    # Only consider parts with a small size and that have at least one
    # qualifying order from both years.
    orders_95 = lines.WHERE((YEAR(order.order_date) == 1995) & (ship_mode == "RAIL"))
    orders_96 = lines.WHERE((YEAR(order.order_date) == 1996) & (ship_mode == "RAIL"))
    return (
        parts.WHERE(STARTSWITH(container, "SM") & HAS(orders_95) & HAS(orders_96))
        .CALCULATE(name, qty_95=SUM(orders_95.quantity), qty_96=SUM(orders_96.quantity))
        .TOP_K(3, by=((qty_96 - qty_95).DESC(), name.ASC()))
    )


def rank_a():
    return customers.CALCULATE(rank=RANKING(by=account_balance.DESC()))


def rank_b():
    return orders.CALCULATE(rank=RANKING(by=(order_priority.ASC()), allow_ties=True))


def rank_c():
    return orders.CALCULATE(
        order_date, rank=RANKING(by=order_date.ASC(), allow_ties=True, dense=True)
    )


def floor_and_ceil():
    return TPCH.CALCULATE(
        floor_frac=FLOOR(5.6),
        ceil_frac=CEIL(5.4),
        floor_frac_neg=FLOOR(-5.4),
        ceil_frac_neg=CEIL(-5.6),
        floor_int=FLOOR(6),
        ceil_int=CEIL(6),
        floor_int_neg=FLOOR(-6),
        ceil_int_neg=CEIL(-6),
    )


def floor_and_ceil_2():
    return supply_records.CALCULATE(
        supplier_key=supplier_key,
        part_key=part_key,
        complete_parts=FLOOR(available_quantity),
        total_cost=CEIL(supply_cost * FLOOR(available_quantity)),
    ).TOP_K(10, by=total_cost.DESC())


def rank_nations_by_region():
    return nations.CALCULATE(name, rank=RANKING(by=region.name.ASC(), allow_ties=True))


def rank_nations_per_region_by_customers():
    return regions.nations.CALCULATE(
        name, rank=RANKING(by=COUNT(customers).DESC(), per="regions")
    ).TOP_K(5, by=rank.ASC())


def rank_parts_per_supplier_region_by_size():
    return (
        regions.CALCULATE(region_name=name)
        .nations.suppliers.supply_records.part.CALCULATE(
            key,
            region=region_name,
            rank=RANKING(
                by=(size.DESC(), container.DESC(), part_type.DESC()),
                per="regions",
                allow_ties=True,
                dense=True,
            ),
        )
        .TOP_K(15, by=key.ASC())
    )


def rank_with_filters_a():
    return (
        customers.CALCULATE(n=name, r=RANKING(by=account_balance.DESC()))
        .WHERE(ENDSWITH(name, "0"))
        .WHERE(r <= 30)
    )


def rank_with_filters_b():
    return (
        customers.CALCULATE(n=name, r=RANKING(by=account_balance.DESC()))
        .WHERE(r <= 30)
        .WHERE(ENDSWITH(name, "0"))
    )


def rank_with_filters_c():
    return (
        parts.PARTITION(name="sizes", by=size)
        .TOP_K(5, by=size.DESC())
        .parts.CALCULATE(size, name)
        .WHERE(RANKING(by=retail_price.DESC(), per="sizes") == 1)
    )


def first_order_per_customer():
    # For each customer, find the total price of the first order they made and
    # when it was made. Pick the 5 customers with the highest such values.
    # If a customer ordered multiple orders on the first such day, pick the one
    # with the lowest key. Only consider customers with at least $9k in their
    # account. Only look at customers with at least one order.
    first_order = orders.WHERE(
        RANKING(by=(order_date.ASC(), key.ASC()), per="customers") == 1
    ).SINGULAR()
    return (
        customers.WHERE(account_balance >= 9000.0)
        .WHERE(HAS(first_order))
        .CALCULATE(
            name,
            first_order_date=first_order.order_date,
            first_order_price=first_order.total_price,
        )
        .TOP_K(5, by=first_order_price.DESC())
    )


def percentile_nations():
    # For every nation, give its name & its bucket from 1-5 ordered by name
    # alphabetically
    return nations.CALCULATE(name, p=PERCENTILE(by=name.ASC(), n_buckets=5))


def percentile_customers_per_region():
    # For each region, give the name of all customers in that region that are
    # in the 95th percentile in terms of account balance (larger percentile
    # means more money) and whose phone number ends in two zeros, sorted by the
    # name of the customers
    return (
        regions.nations.customers.CALCULATE(name)
        .WHERE(
            (PERCENTILE(by=(account_balance.ASC()), per="regions") == 95)
            & ENDSWITH(phone, "00")
        )
        .ORDER_BY(name.ASC())
    )


def regional_suppliers_percentile():
    # For each region, find the suppliers in the top 0.1% by number of parts
    # they supply, breaking ties by name, only keeping the suppliers in the top
    pct = PERCENTILE(
        by=(COUNT(supply_records).ASC(), name.ASC()), per="regions", n_buckets=1000
    )
    return regions.nations.suppliers.CALCULATE(name).WHERE(
        HAS(supply_records) & (pct == 1000)
    )


def prev_next_regions():
    # Sorts the regions alphabetically and finds the previous and next regions'
    # names in a rolling window.
    return regions.CALCULATE(
        two_preceding=PREV(name, n=2, by=name.ASC()),
        one_preceding=PREV(name, by=name.ASC()),
        current=name,
        one_following=NEXT(name, by=name.ASC()),
        two_following=PREV(name, n=-2, by=name.ASC()),
    ).ORDER_BY(current.ASC())


def avg_order_diff_per_customer():
    # Finds the 5 customers with the highest average difference in days between
    # orders made. Only consider Japanese customers and urgent orders.
    prev_order_date_by_cust = PREV(order_date, by=order_date.ASC(), per="customers")
    order_info = orders.WHERE(order_priority == "1-URGENT").CALCULATE(
        day_diff=DATEDIFF("days", prev_order_date_by_cust, order_date)
    )
    selected_customers = customers.WHERE(nation.name == "JAPAN").WHERE(HAS(order_info))
    return selected_customers.CALCULATE(name, avg_diff=AVG(order_info.day_diff)).TOP_K(
        5, by=avg_diff.DESC()
    )


def yoy_change_in_num_orders():
    # For every year, counts the number of orders made in that year and the
    # percentage change from the previous year.
    years = orders.CALCULATE(year=YEAR(order_date)).PARTITION(name="years", by=year)
    current_year_orders = COUNT(orders)
    prev_year_orders = PREV(COUNT(orders), by=year.ASC())
    return years.CALCULATE(
        year,
        current_year_orders=current_year_orders,
        pct_change=100.0 * (current_year_orders - prev_year_orders) / prev_year_orders,
    ).ORDER_BY(year.ASC())


def first_order_in_year():
    # Find all orders that do not have a previous order in the same year
    # (breaking ties by order key).
    previous_order_date = PREV(order_date, by=(order_date.ASC(), key.ASC()))
    return (
        orders.WHERE(MONTH(order_date) == 1)
        .WHERE(
            ABSENT(previous_order_date)
            | (YEAR(previous_order_date) != YEAR(order_date))
        )
        .CALCULATE(order_date, key)
        .ORDER_BY(order_date.ASC())
    )


def customer_largest_order_deltas():
    # For each customer, find the highest positive/negative difference in
    # revenue between one of their orders and and the most recent order before
    # it, ignoring their first ever order. Return the 5 customers with the
    # largest such difference. Only consider customers with orders. Only
    # consider customers in the AUTOMOBILE market segment, orders made in
    # 1994, and lineitems shipepd by air also in 1994.
    line_revenue = extended_price * (1 - discount)
    order_revenue = SUM(
        lines.WHERE((ship_mode == "AIR") & (YEAR(ship_date) == 1994))
        .CALCULATE(r=line_revenue)
        .r
    )
    previous_order_revenue = PREV(order_revenue, by=order_date.ASC(), per="customers")
    orders_info = (
        orders.WHERE(YEAR(order_date) == 1994)
        .WHERE(PRESENT(previous_order_revenue))
        .CALCULATE(revenue_delta=order_revenue - previous_order_revenue)
    )
    return (
        customers.WHERE(market_segment == "AUTOMOBILE")
        .CALCULATE(
            max_diff=MAX(orders_info.revenue_delta),
            min_diff=MIN(orders_info.revenue_delta),
        )
        .WHERE(HAS(orders_info))
        .CALCULATE(
            name,
            largest_diff=IFF(ABS(min_diff) > max_diff, min_diff, max_diff),
        )
        .TOP_K(5, by=largest_diff.DESC())
    )


def suppliers_bal_diffs():
    # Finds the 5 suppliers with the largest difference in account balance from
    # the supplier with the next smallest account balance in the same region.
    return (
        regions.CALCULATE(region_name=name)
        .nations.suppliers.CALCULATE(
            name,
            region_name,
            acctbal_delta=account_balance
            - PREV(account_balance, by=account_balance.ASC(), per="regions"),
        )
        .TOP_K(5, by=acctbal_delta.DESC())
    )


def month_year_sliding_windows():
    # Finds all months where the total amount spent by customers on urgent
    # orders in that month was more than the preceding/following month,
    # and the amount spent in that year was more than the following year.
    ym_groups = (
        orders.WHERE(order_priority == "1-URGENT")
        .CALCULATE(year=YEAR(order_date), month=MONTH(order_date))
        .PARTITION(
            name="months",
            by=(year, month),
        )
        .CALCULATE(month_total_spent=SUM(orders.total_price))
    )
    y_groups = (
        ym_groups.PARTITION(name="years", by=year)
        .CALCULATE(
            curr_year_total_spent=SUM(months.month_total_spent),
            next_year_total_spent=NEXT(
                SUM(months.month_total_spent), by=year.ASC(), default=0.0
            ),
        )
        .WHERE(curr_year_total_spent > next_year_total_spent)
    )
    return (
        y_groups.months.WHERE(
            (
                month_total_spent
                > PREV(month_total_spent, by=(year.ASC(), month.ASC()), default=0.0)
            )
            & (
                month_total_spent
                > NEXT(month_total_spent, by=(year.ASC(), month.ASC()), default=0.0)
            )
        )
        .CALCULATE(year, month)
        .ORDER_BY(year.ASC(), month.ASC())
    )


def avg_gap_prev_urgent_same_clerk():
    # Finds the average gap in days between each urgent order and the previous
    # urgent order handled by the same clerk
    urgent_orders = orders.WHERE(order_priority == "1-URGENT")
    clerk_groups = urgent_orders.PARTITION(name="clerks", by=clerk)
    order_info = clerk_groups.orders.CALCULATE(
        delta=DATEDIFF(
            "days", PREV(order_date, by=order_date.ASC(), per="clerks"), order_date
        )
    )
    return TPCH.CALCULATE(avg_delta=AVG(order_info.delta))


def customer_most_recent_orders():
    # Finds the 3 customers with the highest total value from their 5 most
    # recent urgent orders, breaking ties in favor of the lower order key.
    most_recent_orders = orders.BEST(
        per="customers", by=(order_date.DESC(), key.ASC()), n_best=5
    )
    return (
        customers.WHERE(HAS(most_recent_orders))
        .CALCULATE(name, total_recent_value=SUM(most_recent_orders.total_price))
        .TOP_K(3, by=total_recent_value.DESC())
    )


def richest_customer_per_region():
    # Identifies the name of the customer with the largest account balance in
    # every region, breaking ties by the customer's name alphabetically, and
    # including the name of the customer's nation.
    return (
        regions.CALCULATE(region_name=name)
        .nations.CALCULATE(nation_name=name)
        .customers.BEST(
            per="regions",
            by=(account_balance.DESC(), name.ASC()),
        )
        .CALCULATE(
            region_name, nation_name, customer_name=name, balance=account_balance
        )
    )


def n_orders_first_day():
    # Counts how many orders were made on the first recorded day of orders.
    first_day_orders = orders.BEST(by=order_date.ASC(), allow_ties=True)
    return TPCH.CALCULATE(n_orders=COUNT(first_day_orders))


def wealthiest_supplier():
    # Identifies the richest supplier globally, breaking ties alphabetically.
    return suppliers.BEST(by=(account_balance.DESC(), name.ASC())).CALCULATE(
        name, account_balance
    )


def supplier_best_part():
    # For each French supplier, identifies the part that the supplier shipped
    # the largest quantity of in 1994, ignoring shipments that were taxed. List
    # out the supplier name, part name, quantity and number of such shipments
    # for the 3 suppliers with the highest such quantities, breaking ties alphabetically.
    selected_suppliers = suppliers.WHERE(nation.name == "FRANCE")
    selected_lines = lines.WHERE((YEAR(ship_date) == 1994) & (tax == 0))
    best_part_supplied = (
        supply_records.WHERE(HAS(selected_lines))
        .CALCULATE(
            part_name=part.name,
            quantity=SUM(selected_lines.quantity),
            n_shipments=COUNT(selected_lines),
        )
        .BEST(
            per="suppliers",
            by=quantity.DESC(),
        )
    )
    return (
        selected_suppliers.WHERE(HAS(best_part_supplied))
        .CALCULATE(
            supplier_name=name,
            part_name=best_part_supplied.part_name,
            total_quantity=best_part_supplied.quantity,
            n_shipments=best_part_supplied.n_shipments,
        )
        .TOP_K(3, by=(total_quantity.DESC(), supplier_name.ASC()))
    )


def nation_window_aggs():
    # Calculating multiple global windowed aggregations for each nation, only
    # considering nations whose names do not start with a vowel.
    return (
        nations.WHERE(~ISIN(name[:1], ("A", "E", "I", "O", "U")))
        .CALCULATE(
            nation_name=name,
            key_sum=RELSUM(key),
            key_avg=RELAVG(key),
            n_short_comment=RELCOUNT(KEEP_IF(comment, LENGTH(comment) < 75)),
            n_nations=RELSIZE(),
        )
        .ORDER_BY(region_key.ASC(), nation_name.ASC())
    )


def region_orders_from_nations_richest():
    # Find the orders from the wealthiest customer in each nation, breaking
    # ties alphabetically by customer name.
    nations_richest_customers = nations.customers.BEST(
        per="nations", by=(account_balance.DESC(), name.ASC())
    ).orders
    return regions.CALCULATE(
        region_name=name, n_orders=COUNT(nations_richest_customers)
    ).ORDER_BY(region_name.ASC())


def regional_first_order_best_line_part():
    # For each region, identify the first order made by a customer in that region,
    # (breaking ties by order key), and the name of the part from that order
    # with the largest quantity shipped, breaking ties by line number within
    # the order.
    first_order = nations.customers.orders.WHERE(YEAR(order_date) == 1992).BEST(
        per="regions", by=(order_date.ASC(), key.ASC())
    )
    largest_quantity_line = first_order.lines.WHERE(YEAR(ship_date) == 1992).BEST(
        per="regions", by=(quantity.DESC(), line_number.ASC())
    )
    return regions.CALCULATE(
        region_name=name, part_name=largest_quantity_line.part.name
    ).ORDER_BY(region_name.ASC())


def orders_versus_first_orders():
    # Identify the 5 biggest day gaps between an order and the customer's first
    # order. Include the customer name, the day gap, and the key or the order.
    # Break ties alphabetically by customer name. Only consider customers from
    # Vietnam.
    first_order_from_cust = (
        customer.WHERE(nation.name == "VIETNAM")
        .CALCULATE(customer_name=name)
        .orders.BEST(per="customer", by=(order_date.ASC(), key.ASC()))
    )
    return orders.CALCULATE(
        customer_name=first_order_from_cust.customer_name,
        order_key=key,
        days_since_first_order=DATEDIFF(
            "days", first_order_from_cust.order_date, order_date
        ),
    ).TOP_K(5, by=(days_since_first_order.DESC(), customer_name.ASC()))


def region_nation_window_aggs():
    # Calculating multiple windowed aggregations for each nation, per-region,
    # only considering nations whose names do not start with a vowel.
    return (
        regions.nations.WHERE(~ISIN(name[:1], ("A", "E", "I", "O", "U")))
        .CALCULATE(
            nation_name=name,
            key_sum=RELSUM(key, per="regions"),
            key_avg=RELAVG(key, per="regions"),
            n_short_comment=RELCOUNT(
                KEEP_IF(comment, LENGTH(comment) < 75), per="regions"
            ),
            n_nations=RELSIZE(per="regions"),
        )
        .ORDER_BY(region_key.ASC(), nation_name.ASC())
    )


def cumulative_stock_analysis():
    # Analyzes successfull ticker transactions in April 2023 with the following
    # cumulative analyses for each transaction:
    # - What ordinal transaction was it within the day
    # - What percentage of all transactions made so far were for Apple/Amazon
    # - What is the total number of net stocks bought (subtracting sold)
    # - What is the rolling average of the amount of money spent on all of the
    #   transactions so far.
    # - What is the number of buy transactions made within the day so far.
    return (
        transactions.WHERE(
            (YEAR(date_time) == 2023) & (MONTH(date_time) == 4) & (status == "success")
        )
        .CALCULATE(txn_day=DATETIME(date_time, "start of day"))
        .PARTITION(name="days", by=txn_day)
        .transactions.CALCULATE(
            date_time,
            txn_within_day=RELSIZE(by=date_time.ASC(), cumulative=True, per="days"),
            n_buys_within_day=RELCOUNT(
                KEEP_IF(transaction_type, transaction_type == "buy"),
                by=date_time.ASC(),
                cumulative=True,
                per="days",
            ),
            pct_apple_txns=ROUND(
                (
                    100.0
                    * RELSUM(
                        ISIN(ticker.symbol, ("AAPL", "AMZN")),
                        by=date_time.ASC(),
                        cumulative=True,
                    )
                )
                / RELSIZE(by=date_time.ASC(), cumulative=True),
                2,
            ),
            share_change=RELSUM(
                IFF(transaction_type == "buy", shares, -shares),
                by=date_time.ASC(),
                cumulative=True,
            ),
            rolling_avg_amount=ROUND(
                RELAVG(amount, by=date_time.ASC(), cumulative=True), 2
            ),
        )
        .ORDER_BY(date_time.ASC())
    )


def time_threshold_reached():
    # For every day in 2023, find the time of the first transaction made that
    # represents at least 50% of all shares bought/sold that day so far
    # having been completed.
    return (
        transactions.WHERE((YEAR(date_time) == 2023))
        .CALCULATE(txn_day=DATETIME(date_time, "start of day"))
        .PARTITION(name="days", by=txn_day)
        .transactions.CALCULATE(
            pct_of_day=(
                100.0 * RELSUM(shares, by=date_time.ASC(), cumulative=True, per="days")
            )
            / RELSUM(shares, per="days"),
        )
        .WHERE(pct_of_day >= 50.0)
        .BEST(by=pct_of_day.ASC(), per="days")
        .CALCULATE(date_time)
        .ORDER_BY(date_time)
    )


def window_sliding_frame_relsize():
    return customers.transactions_made.CALCULATE(
        transaction_id,
        w1=RELSIZE(by=(date_time.ASC(), transaction_id.ASC()), frame=(-4, 0)),
        w2=RELSIZE(
            by=(date_time.ASC(), transaction_id.ASC()), frame=(-4, 0), per="customers"
        ),
        w3=RELSIZE(by=(date_time.ASC(), transaction_id.ASC()), frame=(0, None)),
        w4=RELSIZE(
            by=(date_time.ASC(), transaction_id.ASC()), frame=(0, None), per="customers"
        ),
        w5=RELSIZE(by=(date_time.ASC(), transaction_id.ASC()), frame=(None, -1)),
        w6=RELSIZE(
            by=(date_time.ASC(), transaction_id.ASC()),
            frame=(None, -1),
            per="customers",
        ),
        w7=RELSIZE(by=(date_time.ASC(), transaction_id.ASC()), frame=(-3, 5)),
        w8=RELSIZE(
            by=(date_time.ASC(), transaction_id.ASC()), frame=(-3, 5), per="customers"
        ),
    ).TOP_K(8, by=date_time.ASC())


def window_sliding_frame_relsum():
    return customers.transactions_made.CALCULATE(
        transaction_id,
        w1=RELSUM(shares, by=(date_time.ASC(), transaction_id.ASC()), frame=(0, 4)),
        w2=RELSUM(
            shares,
            by=(date_time.ASC(), transaction_id.ASC()),
            frame=(0, 4),
            per="customers",
        ),
        w3=RELSUM(shares, by=(date_time.ASC(), transaction_id.ASC()), frame=(0, None)),
        w4=RELSUM(
            shares,
            by=(date_time.ASC(), transaction_id.ASC()),
            frame=(0, None),
            per="customers",
        ),
        w5=RELSUM(shares, by=(date_time.ASC(), transaction_id.ASC()), frame=(None, 1)),
        w6=RELSUM(
            shares,
            by=(date_time.ASC(), transaction_id.ASC()),
            frame=(None, 1),
            per="customers",
        ),
        w7=RELSUM(shares, by=(date_time.ASC(), transaction_id.ASC()), frame=(-5, -1)),
        w8=RELSUM(
            shares,
            by=(date_time.ASC(), transaction_id.ASC()),
            frame=(-5, -1),
            per="customers",
        ),
    ).TOP_K(8, by=date_time.ASC())


def supplier_pct_national_qty():
    # Find the 5 African suppliers with the highest percentage of total
    # quantity of product shipped from them out of all suppliers in that nation
    # meeting certain criteria. Include for each such supplier their name,
    # nation name, the quantity, and the percentage. The criteria are that the
    # shipments were done in 1998, they were shipped by ship, the part shipped
    # had "tomato" in the name and was a large container. Also, when
    # finding the sum for each naiton and the best suppliers, ignore all
    # suppliers with a negative account balance and whose comments do not
    # contain the word "careful".
    selected_lines = lines.WHERE(
        (YEAR(ship_date) == 1995)
        & (ship_mode == "SHIP")
        & CONTAINS(part.name, "tomato")
        & STARTSWITH(part.container, "LG")
    )
    supp_qty = SUM(selected_lines.quantity)
    return (
        nations.WHERE(HAS(region.WHERE(name == "AFRICA")))
        .CALCULATE(nation_name=name)
        .suppliers.WHERE((account_balance >= 0.0) & CONTAINS(comment, "careful"))
        .CALCULATE(
            supplier_name=name,
            nation_name=name,
            supplier_quantity=supp_qty,
            national_qty_pct=100.0 * supp_qty / RELSUM(supp_qty, per="nations"),
        )
        .TOP_K(5, by=national_qty_pct.DESC())
    )


def window_filter_order_1():
    # Counts how many german customers made at least 1 order in 1992, but
    # fewer orders in 1992 than the average for all german customers (including
    # those who did not make any orders in 1992).
    selected_orders = orders.WHERE(YEAR(order_date) == 1992)
    selected_customers = customers.WHERE(nation.name == "GERMANY").WHERE(
        (COUNT(selected_orders) < RELAVG(COUNT(selected_orders))) & HAS(selected_orders)
    )
    return TPCH.CALCULATE(n=COUNT(selected_customers))


def window_filter_order_2():
    # Same as window_filter_order_1 but written differnetly.
    selected_orders = orders.WHERE(YEAR(order_date) == 1992)
    selected_customers = customers.WHERE(nation.name == "GERMANY").WHERE(
        HAS(selected_orders) & (COUNT(selected_orders) < RELAVG(COUNT(selected_orders)))
    )
    return TPCH.CALCULATE(n=COUNT(selected_customers))


def window_filter_order_3():
    # Same as window_filter_order_1 but written differnetly.
    selected_orders = orders.WHERE(YEAR(order_date) == 1992)
    selected_customers = (
        customers.WHERE(nation.name == "GERMANY")
        .WHERE(COUNT(selected_orders) < RELAVG(COUNT(selected_orders)))
        .WHERE(HAS(selected_orders))
    )
    return TPCH.CALCULATE(n=COUNT(selected_customers))


def window_filter_order_4():
    # Same as window_filter_order_1 but compares the number of orders
    # against the average number of orders made only by german customers with
    # at least one order made in 1992.
    selected_orders = orders.WHERE(YEAR(order_date) == 1992)
    selected_customers = (
        customers.WHERE(nation.name == "GERMANY")
        .WHERE(HAS(selected_orders))
        .WHERE(COUNT(selected_orders) < RELAVG(COUNT(selected_orders)))
    )
    return TPCH.CALCULATE(n=COUNT(selected_customers))


def window_filter_order_5():
    # Count how many orders were made in 1995 by a customer in the building
    # market segment where the account balance of the customer is below the
    # account balance of all customers (treating those not in the building
    # market segment as having an account balance of 0)
    selected_customer = customer.WHERE(market_segment == "BUILDING")
    selected_orders = orders.WHERE(YEAR(order_date) == 1995).WHERE(
        (
            selected_customer.account_balance
            < RELAVG(DEFAULT_TO(selected_customer.account_balance, 0))
        )
        & HAS(selected_customer)
    )
    return TPCH.CALCULATE(n=COUNT(selected_orders))


def window_filter_order_6():
    # Same as window_filter_order_5 but written differnetly.
    selected_customer = customer.WHERE(market_segment == "BUILDING")
    selected_orders = (
        orders.WHERE(YEAR(order_date) == 1995)
        .WHERE(
            selected_customer.account_balance
            < RELAVG(DEFAULT_TO(selected_customer.account_balance, 0))
        )
        .WHERE(HAS(selected_customer))
    )
    return TPCH.CALCULATE(n=COUNT(selected_orders))


def window_filter_order_7():
    # Same as window_filter_order_5 but only compares against the average
    # account balance of customers in the building market segment.
    selected_customer = customer.WHERE(market_segment == "BUILDING")
    selected_orders = (
        orders.WHERE(YEAR(order_date) == 1995)
        .WHERE(HAS(selected_customer))
        .WHERE(
            selected_customer.account_balance
            < RELAVG(selected_customer.account_balance)
        )
    )
    return TPCH.CALCULATE(n=COUNT(selected_orders))


def window_filter_order_8():
    # Counts how many French customers have no orders made in January of 1995
    # and have and account balance less than the total number of orders made by
    # French customers in January of 1998.
    selected_orders = orders.WHERE(
        (YEAR(order_date) == 1998) & (MONTH(order_date) == 1)
    )
    selected_customers = customers.WHERE((nation.name == "FRANCE")).WHERE(
        (account_balance < (RELSUM(COUNT(selected_orders)))) & HASNOT(selected_orders)
    )
    return TPCH.CALCULATE(n=COUNT(selected_customers))


def window_filter_order_9():
    # Counts how many orders made through clerk #1 have a customer not in the
    # building market segment, and also a total price below 5% of the average
    # total of all order total prices from per such customers.
    selected_custoemrs = customer.WHERE(market_segment == "BUILDING").CALCULATE(
        total_spent=SUM(orders.total_price)
    )
    selected_orders = orders.WHERE(clerk == "Clerk#000000001").WHERE(
        (total_price < (0.05 * RELAVG(selected_custoemrs.total_spent)))
        & HASNOT(selected_custoemrs)
    )
    return TPCH.CALCULATE(n=COUNT(selected_orders))


def window_filter_order_10():
    # Same as window_filter_order_9 but filters to only consider orders without
    # any such customers before computig the average (a paradox that will
    # always be False)
    selected_custoemrs = customer.WHERE(market_segment == "BUILDING").CALCULATE(
        total_spent=SUM(orders.total_price)
    )
    selected_orders = (
        orders.WHERE(clerk == "Clerk#000000001")
        .WHERE(HASNOT(selected_custoemrs))
        .WHERE((total_price < (0.05 * RELAVG(selected_custoemrs.total_spent))))
    )
    return TPCH.CALCULATE(n=COUNT(selected_orders))


"""
-- 525
SELECT COUNT(*)
FROM (
    select o_totalprice, marker, AVG(total_spent) OVER () as global_avg
    from orders
    left join (
        select c_custkey, SUM(o_totalprice) as total_spent, 1 as marker
        from customer
        inner join orders on c_custkey = o_custkey
        where c_mktsegment = 'BUILDING'
        group by c_custkey
    )
    on c_custkey = o_custkey
    WHERE o_clerk = 'Clerk#000000001'
)
WHERE marker IS NULL AND o_totalprice < 0.05 * global_avg
;

-- 525
SELECT COUNT(*)
FROM (
    select o_totalprice, marker, AVG(total_spent) OVER () as global_avg
    from orders
    inner join (
        select c_custkey, SUM(o_totalprice) as total_spent, 1 as marker
        from customer
        inner join orders on c_custkey = o_custkey
        where c_mktsegment = 'BUILDING'
        group by c_custkey
    )
    on c_custkey = o_custkey
    WHERE o_clerk = 'Clerk#000000001'
)
WHERE marker IS NULL AND o_totalprice < 0.05 * global_avg
;
"""


def highest_priority_per_year():
    # For each year, identify the priority with the highest percentage of
    # made in that year with that priority, listing the year, priority, and
    # percentage. Sort the results by year.
    order_info = orders.CALCULATE(order_year=YEAR(order_date))
    yp_groups = order_info.PARTITION(
        name="years_priorities", by=(order_priority, order_year)
    ).CALCULATE(n_orders=COUNT(orders))
    year_groups = yp_groups.PARTITION(name="years", by=order_year)
    return (
        year_groups.years_priorities.CALCULATE(
            order_year,
            highest_priority=order_priority,
            priority_pct=100.0 * n_orders / RELSUM(n_orders, per="years"),
        )
        .WHERE(RANKING(by=priority_pct.DESC(), per="years") == 1)
        .ORDER_BY(order_year.ASC())
    )


def nation_best_order():
    # For each Asian nation, identify the most expensive order made by a
    # customer in that nation in 1998, listing the nation name, customer
    # name, order key, the order's price, and percentage of the price of
    # all orders made in 1998. Order the nations alphabetically.
    selected_nations = nations.WHERE(region.name == "ASIA")
    best_order = (
        customers.CALCULATE(customer_name=name)
        .orders.WHERE(YEAR(order_date) == 1998)
        .CALCULATE(
            order_value=total_price,
            value_percentage=100.0 * total_price / RELSUM(total_price, per="nations"),
            order_key=key,
        )
        .BEST(by=order_value.DESC(), per="nations")
    )
    return (
        selected_nations.WHERE(HAS(best_order))
        .CALCULATE(
            nation_name=name,
            customer_name=best_order.customer_name,
            order_key=best_order.order_key,
            order_value=best_order.order_value,
            value_percentage=best_order.value_percentage,
        )
        .ORDER_BY(name.ASC())
    )


def nation_acctbal_breakdown():
    # For each American nation, identify the number of customers with negative
    # versus non-negative account balances, the median account balance for each
    # as well as the median account balance of all customers in the nation.
    customer_info = customers.CALCULATE(
        negative_acctbal=KEEP_IF(account_balance, account_balance < 0),
        non_negative_acctbal=KEEP_IF(account_balance, account_balance >= 0),
    )
    return (
        nations.WHERE(region.name == "AMERICA")
        .CALCULATE(
            nation_name=name,
            n_red_acctbal=COUNT(customer_info.negative_acctbal),
            n_black_acctbal=COUNT(customer_info.non_negative_acctbal),
            median_red_acctbal=MEDIAN(customer_info.negative_acctbal),
            median_black_acctbal=MEDIAN(customer_info.non_negative_acctbal),
            median_overall_acctbal=MEDIAN(customer_info.account_balance),
        )
        .ORDER_BY(nation_name.ASC())
    )


def region_acctbal_breakdown():
    # For each region identify the number of customers with negative versus
    # non-negative account balances, the median account balance for each
    # as well as the median account balance of all customers in the nation.
    customer_info = nations.customers.CALCULATE(
        negative_acctbal=KEEP_IF(account_balance, account_balance < 0),
        non_negative_acctbal=KEEP_IF(account_balance, account_balance >= 0),
    )
    return (
        regions.CALCULATE(region_name=name)
        .CALCULATE(
            region_name,
            n_red_acctbal=COUNT(customer_info.negative_acctbal),
            n_black_acctbal=COUNT(customer_info.non_negative_acctbal),
            median_red_acctbal=MEDIAN(customer_info.negative_acctbal),
            median_black_acctbal=MEDIAN(customer_info.non_negative_acctbal),
            median_overall_acctbal=MEDIAN(customer_info.account_balance),
        )
        .ORDER_BY(region_name.ASC())
    )


def global_acctbal_breakdown():
    # Count the number of customers with negative versus non-negative account
    # balances, the median account balance for each as well as the median
    # account balance of all customers in the nation.
    customer_info = customers.CALCULATE(
        negative_acctbal=KEEP_IF(account_balance, account_balance < 0),
        non_negative_acctbal=KEEP_IF(account_balance, account_balance >= 0),
    )
    return TPCH.CALCULATE(
        n_red_acctbal=COUNT(customer_info.negative_acctbal),
        n_black_acctbal=COUNT(customer_info.non_negative_acctbal),
        median_red_acctbal=MEDIAN(customer_info.negative_acctbal),
        median_black_acctbal=MEDIAN(customer_info.non_negative_acctbal),
        median_overall_acctbal=MEDIAN(customer_info.account_balance),
    )


def top_customers_by_orders():
    # Finds the keys of the 5 customers with the most orders.
    return customers.CALCULATE(
        customer_key=key,
        n_orders=COUNT(orders),
    ).TOP_K(5, by=(COUNT(orders).DESC(), customer_key.ASC()))


def bad_child_reuse_1():
    # Compute the top 10 customers by account balance, list their keys and
    # number of orders, only keep ones that have orders.
    return (
        customers.CALCULATE(cust_key=key, n_orders=COUNT(orders))
        .TOP_K(10, by=account_balance.DESC())
        .WHERE(HAS(orders))
    )


def bad_child_reuse_2():
    # Compute the top 10 customers by account balance, (after filtering to only
    # include the customers that have orders). For each, calculate the number
    # orders the customers made and the total number of customers in the same
    # nation.
    return (
        nations.customers.CALCULATE(cust_key=key, n_orders=COUNT(orders))
        .CALCULATE(cust_key, n_orders, n_cust=RELSIZE(per="nations"))
        .WHERE(HAS(orders))
        .TOP_K(10, by=account_balance.DESC())
    )


def bad_child_reuse_3():
    # Same as bad_child_reuse_2, written slightly differently.
    return (
        nations.customers.CALCULATE(
            cust_key=key, n_orders=COUNT(orders), n_cust=RELSIZE(per="nations")
        )
        .WHERE(HAS(orders))
        .TOP_K(10, by=account_balance.DESC())
        .CALCULATE(cust_key, n_orders, n_cust)
    )


def bad_child_reuse_4():
    # For each customer whose number of orders is below the average for
    # all orders made by customers in the same nation, find the top 10
    # customers by account balance (only considering customers with orders),
    # and calculate the number of orders they made.
    return (
        nations.customers.WHERE(COUNT(orders) < RELAVG(COUNT(orders), per="nations"))
        .WHERE(HAS(orders))
        .TOP_K(10, by=account_balance.DESC())
        .CALCULATE(cust_key=key, n_orders=COUNT(orders))
    )


def bad_child_reuse_5():
    # Compute the top 10 customers by account balance, list their keys and
    # number of orders, only keep ones that have no orders.
    return (
        customers.CALCULATE(cust_key=key, n_orders=COUNT(orders))
        .TOP_K(10, by=account_balance.DESC())
        .WHERE(HASNOT(orders))
    )


def aggregation_analytics_1():
    # What 8 large products produced by Supplier#000009450 generated the LEAST
    # revenue for them in between 1995 and 1996? Include any products the
    # supplier produces that generated NO revenue for the supplier during that
    # time period, and break ties alphabetically by part name. Use the ship
    # adte to determine which orders are in the time period.
    selected_lines = lines.WHERE(ISIN(YEAR(ship_date), (1995, 1996))).CALCULATE(
        revenue=extended_price * (1 - discount) * (1 - tax) - quantity * ps_supply_cost
    )
    return (
        supply_records.WHERE(
            (supplier.name == "Supplier#000009450") & STARTSWITH(part.container, "LG")
        )
        .CALCULATE(ps_supply_cost=supply_cost)
        .CALCULATE(
            part_name=part.name,
            revenue_generated=ROUND(SUM(selected_lines.revenue), 2),
        )
        .TOP_K(8, by=(revenue_generated.ASC(), part_name.ASC()))
    )


def aggregation_analytics_2():
    # What 4 small products produced by Supplier#000000182 generated the LEAST
    # revenue for them in between 1995 and 1996? Do NOT include any products
    # the supplier produces that generated NO revenue for the supplier during
    # that time period, and break ties alphabetically by part name. Use the ship
    # adte to determine which orders are in the time period.
    selected_lines = lines.WHERE(ISIN(YEAR(ship_date), (1995, 1996))).CALCULATE(
        revenue=extended_price * (1 - discount) * (1 - tax) - quantity * ps_supply_cost
    )
    return (
        supply_records.WHERE(
            (supplier.name == "Supplier#000000182") & STARTSWITH(part.container, "SM")
        )
        .CALCULATE(ps_supply_cost=supply_cost)
        .WHERE(HAS(selected_lines))
        .CALCULATE(
            part_name=part.name,
            revenue_generated=ROUND(SUM(selected_lines.revenue), 2),
        )
        .TOP_K(4, by=(revenue_generated.ASC(), part_name.ASC()))
    )


def aggregation_analytics_3():
    # What 3 medium products produced by Supplier#000002103 generated the LEAST
    # revenue per quantity ordered in 1994? Do NOT include any products the
    # supplier produces that generated NO revenue for the supplier during that
    # time period, and break ties alphabetically by part name. Use the ship
    # adte to determine which orders are in the time period.
    selected_lines = lines.WHERE(YEAR(ship_date) == 1994).CALCULATE(
        revenue=extended_price * (1 - discount) * (1 - tax) - quantity * ps_supply_cost
    )
    return (
        supply_records.WHERE(
            (supplier.name == "Supplier#000000182") & STARTSWITH(part.container, "MED")
        )
        .CALCULATE(ps_supply_cost=supply_cost)
        .WHERE(HAS(selected_lines))
        .CALCULATE(
            part_name=part.name,
            revenue_ratio=ROUND(
                SUM(selected_lines.revenue) / SUM(selected_lines.quantity), 2
            ),
        )
        .TOP_K(3, by=(revenue_ratio.ASC(), part_name.ASC()))
    )


def function_sampler():
    # Functions tested:
    # JOIN_STRINGS,
    # ROUND (with and without precision),
    # KEEP_IF,
    # PRESENT,
    # ABSENT,
    # MONOTONIC
    return (
        regions.CALCULATE(region_name=name)
        .nations.CALCULATE(nation_name=name)
        .customers.CALCULATE(
            a=JOIN_STRINGS("-", region_name, nation_name, name[16:]),
            b=ROUND(account_balance, 1),
            c=KEEP_IF(name, phone[:1] == "3"),
            d=PRESENT(KEEP_IF(name, phone[1:2] == "1")),
            e=ABSENT(KEEP_IF(name, phone[14:] == "7")),
            f=ROUND(account_balance),
        )
        .WHERE(MONOTONIC(0.0, account_balance, 100.0))
        .TOP_K(10, by=address.ASC())
    )


def datetime_current():
    return TPCH.CALCULATE(
        d1=DATETIME("now", "start of year", "5 months", "-1 DAY"),
        d2=DATETIME("current_date", "start  of mm", "+24 hours"),
        d3=DATETIME(
            " Current Timestamp ", "start of day", "+12 hours", "-150 minutes", "+2 s"
        ),
    )


def datetime_relative():
    selected_orders = orders.TOP_K(
        10, by=(customer_key.ASC(), order_date.ASC())
    ).ORDER_BY(order_date.ASC())
    return selected_orders.CALCULATE(
        d1=DATETIME(order_date, "Start of Year"),
        d2=DATETIME(order_date, "START OF MONTHS"),
        d3=DATETIME(
            order_date,
            "-11 years",
            "+9 months",
            " - 7 DaYs ",
            "+5 h",
            "-3 minutes",
            "+1 second",
        ),
        d4=DATETIME(pd.Timestamp("2025-07-04 12:58:45"), "start of hour"),
        d5=DATETIME(pd.Timestamp("2025-07-04 12:58:45"), "start of minute"),
        d6=DATETIME(pd.Timestamp("2025-07-14 12:58:45"), "+ 1000000 seconds"),
    )


def datetime_sampler():
    # Near-exhaustive edge cases coverage testing for DATETIME strings. The
    # terms were generated via random combination selection of various ways
    # of augmenting the base/modifier terms.
    return orders.CALCULATE(
        DATETIME("2025-07-04 12:58:45"),
        DATETIME("2024-12-31 11:59:00"),
        DATETIME("2025-01-01"),
        DATETIME("1999-03-14"),
        DATETIME("now"),
        DATETIME(" Now "),
        DATETIME("NOW\n\t\t\r"),
        DATETIME("current_date"),
        DATETIME(" Current_Date "),
        DATETIME("CURRENT_DATE\n\t\t\r"),
        DATETIME("cUrReNt_Timestamp"),
        DATETIME(" Current_Timestamp "),
        DATETIME("CURRENT_TIMESTAMP\n\t\t\r"),
        DATETIME("current date"),
        DATETIME(" Current Date "),
        DATETIME("CURRENT DATE\n\t\t\r"),
        DATETIME("current timestamp"),
        DATETIME(" Current Timestamp "),
        DATETIME("CURRENT TIMESTAMP\n\t\t\r"),
        DATETIME(order_date),
        DATETIME("CURRENT_DATE\n\t\t\r", "start of seconds"),
        DATETIME("current date", "\n  Start  Of\tY\n\n", "+8 minutes", "-141 mm"),
        DATETIME(
            "CURRENT_TIMESTAMP\n\t\t\r",
            "\tSTART\tOF\tmonth\t",
            "\tSTART\tOF\tsecond\t",
            "\tSTART\tOF\thour\t",
        ),
        DATETIME(
            " Current Timestamp ",
            "start of h",
            "START of    SECOND",
            "START of    HOUR",
        ),
        DATETIME("NOW\n\t\t\r", "- 96 H", "15 year"),
        DATETIME(
            "CURRENT_TIMESTAMP\n\t\t\r",
            "\n  Start  Of\tY\n\n",
            "-3 years",
            "\n  Start  Of\tM\n\n",
            "+65 month",
        ),
        DATETIME(order_date, "-56 h", "start of year"),
        DATETIME(
            "CURRENT_TIMESTAMP\n\t\t\r",
            "-63 days",
            "START of    MINUTE",
            "start of seconds",
        ),
        DATETIME("CURRENT_DATE\n\t\t\r", "\n  Start  Of\tMonth\n\n"),
        DATETIME("NOW\n\t\t\r", "-312 hour", "start of s", " \n +\t48 \nYear \n\r "),
        DATETIME(
            "CURRENT TIMESTAMP\n\t\t\r",
            " 75 DAY",
            "\n  Start  Of\tDays\n\n",
            "+600 minutes",
            " \n -\t294 \nDays \n\r ",
        ),
        DATETIME(
            " Current Date ",
            "\n  Start  Of\tMonths\n\n",
            " \n +\t480 \nMm \n\r ",
            " \n -\t45 \nY \n\r ",
        ),
        DATETIME(
            " CuRrEnT dAtE ",
            "- 270 MINUTES",
            "- 34 SECONDS",
            "\tSTART\tOF\td\t",
            "start of second",
        ),
        DATETIME("current timestamp", "START of    MM", " \n \t213 \nS \n\r "),
        DATETIME(
            " Now ", "\n  Start  Of\tMonth\n\n", "13 minute", "28 year", "+344 second"
        ),
        DATETIME("CURRENT_DATE\n\t\t\r", "\tSTART\tOF\tdays\t"),
        DATETIME("2025-01-01", "START of    H", "+ 49 MINUTE", "+91 y"),
        DATETIME("CURRENT_DATE\n\t\t\r", "START of    YEARS", "\tSTART\tOF\td\t"),
        DATETIME("NOW\n\t\t\r", "start of days", "START of    YEARS"),
        DATETIME("2025-07-04 12:58:45", "\tSTART\tOF\tmonths\t", " \n \t22 \nM \n\r "),
        DATETIME("current_date", "START of    YEAR"),
        DATETIME(
            order_date,
            "+ 82 S",
            "415 second",
            " \n -\t160 \nSecond \n\r ",
            "START of    Y",
        ),
        DATETIME(" Current Date ", "192 months"),
        DATETIME(
            "CURRENT TIMESTAMP\n\t\t\r",
            "START of    H",
            "start of minute",
            "\n  Start  Of\tHours\n\n",
            "+ 486 M",
        ),
        DATETIME(
            "CURRENT_TIMESTAMP\n\t\t\r", "\n  Start  Of\tSeconds\n\n", "- 50 HOURS"
        ),
        DATETIME(
            "CURRENT_TIMESTAMP\n\t\t\r",
            " 297 D",
            "72 months",
            " \n -\t92 \nMonth \n\r ",
            "\tSTART\tOF\thours\t",
        ),
        DATETIME("now", " \n +\t285 \nSeconds \n\r ", "\tSTART\tOF\tday\t"),
        DATETIME("1999-03-14", "+62 d"),
        DATETIME(
            "current_date",
            "START of    MM",
            "+1 hour",
            "start of mm",
            " \n -\t21 \nDay \n\r ",
        ),
        DATETIME("current timestamp", "+212 minute", " \n +\t368 \nYears \n\r "),
        DATETIME(
            "2024-12-31 11:59:00",
            "\n  Start  Of\tMonths\n\n",
            "\n  Start  Of\tYears\n\n",
            "\n  Start  Of\tMinutes\n\n",
            "start of m",
        ),
        DATETIME("1999-03-14", "START of    HOURS", "start of day"),
        DATETIME(
            "now",
            " \n -\t60 \nH \n\r ",
            "START of    D",
            "START of    MINUTE",
            "+196 years",
        ),
        DATETIME(
            "current timestamp",
            "-40 hours",
            " \n -\t385 \nDay \n\r ",
            "start of m",
            " \n +\t29 \nHour \n\r ",
        ),
        DATETIME(
            " Current Date ", "+405 days", "start of hour", "\tSTART\tOF\tminutes\t"
        ),
        DATETIME(
            " Current Timestamp ",
            "\tSTART\tOF\tyear\t",
            "\n  Start  Of\tS\n\n",
            " \n +\t98 \nY \n\r ",
            " \n \t96 \nMonth \n\r ",
        ),
        DATETIME(
            " Now ",
            "\tSTART\tOF\tminutes\t",
            "\tSTART\tOF\ts\t",
            "start of day",
            "78 seconds",
        ),
        DATETIME(
            " Current Date ",
            " 136 HOURS",
            " \n +\t104 \nM \n\r ",
            "-104 months",
            " \n \t312 \nD \n\r ",
        ),
        DATETIME(" Current_Date ", "+ 45 MM", "-135 s"),
        YEAR("Current Date"),
        YEAR(pd.Timestamp("2025-07-04 12:58:45")),
        YEAR("1999-03-14"),
        MONTH("Current Date"),
        MONTH(datetime.date(2001, 6, 30)),
        MONTH("1999-03-14"),
        DAY("Current Date"),
        DAY(pd.Timestamp("2025-07-04 12:58:45")),
        DAY("2025-07-04 12:58:45"),
        HOUR("CURRENT_TIMESTAMP"),
        HOUR(datetime.date(2001, 6, 30)),
        HOUR("2024-01-01"),
        MINUTE("CURRENT_TIMESTAMP"),
        MINUTE(pd.Timestamp("2024-12-25 20:30:59")),
        MINUTE("2024-01-01"),
        SECOND("now"),
        SECOND(pd.Timestamp("2025-07-04 12:58:45")),
        SECOND("1999-03-14"),
        DATEDIFF("year", "2018-02-14 12:41:06", "NOW"),
        DATEDIFF("years", order_date, datetime.date(2022, 11, 24)),
        DATEDIFF("month", datetime.date(2005, 6, 30), "1999-03-14"),
        DATEDIFF(
            "months", datetime.datetime(2006, 5, 1, 12, 0), datetime.date(2022, 11, 24)
        ),
        DATEDIFF("day", "CurrentTimestamp", "CURRENTDATE"),
        DATEDIFF("days", "1999-03-14", "CURRENTDATE"),
        DATEDIFF("hour", "NOW", "CURRENTDATE"),
        DATEDIFF("hours", datetime.date(2005, 6, 30), order_date),
        DATEDIFF("minute", "NOW", datetime.datetime(2006, 5, 1, 12, 0)),
        DATEDIFF("minutes", order_date, pd.Timestamp("2021-01-01 07:35:00")),
        DATEDIFF(
            "second", datetime.date(2022, 11, 24), pd.Timestamp("2021-01-01 07:35:00")
        ),
        DATEDIFF("seconds", datetime.date(2005, 6, 30), "2018-02-14 12:41:06"),
        DATEDIFF("year", order_date, datetime.datetime(2006, 5, 1, 12, 0)),
        DATEDIFF("years", "2018-02-14 12:41:06", order_date),
        DATEDIFF("month", order_date, datetime.datetime(2019, 7, 4, 11, 30)),
        DATEDIFF(
            "months", datetime.datetime(2019, 7, 4, 11, 30), "2018-02-14 12:41:06"
        ),
        DATEDIFF("day", "CURRENTDATE", order_date),
        DATEDIFF("days", datetime.datetime(2019, 7, 4, 11, 30), "CURRENTDATE"),
        DATEDIFF("hour", datetime.date(2022, 11, 24), "1999-03-14"),
        DATEDIFF("hours", "2018-02-14 12:41:06", pd.Timestamp("2020-12-31 00:31:06")),
        DATEDIFF(
            "minute", datetime.date(2005, 6, 30), pd.Timestamp("2020-12-31 00:31:06")
        ),
        DATEDIFF("minutes", "CurrentTimestamp", "2018-02-14 12:41:06"),
        DATEDIFF("second", "CURRENTDATE", "1999-03-14"),
        DATEDIFF(
            "seconds",
            datetime.date(2022, 11, 24),
            datetime.datetime(2019, 7, 4, 11, 30),
        ),
    )


def loop_generated_terms():
    # Using a loop & dictionary to generate PyDough calculate terms
    terms = {"name": name}
    for i in range(3):
        terms[f"interval_{i}"] = COUNT(
            customers.WHERE(MONOTONIC(i * 1000, account_balance, (i + 1) * 1000))
        )
    return nations.CALCULATE(**terms)


def function_defined_terms():
    # Using a regular function to generate PyDough calculate terms
    def interval_n(n):
        return COUNT(
            customers.WHERE(MONOTONIC(n * 1000, account_balance, (n + 1) * 1000))
        )

    return nations.CALCULATE(
        name,
        interval_7=interval_n(7),
        interval_4=interval_n(4),
        interval_13=interval_n(13),
    )


def function_defined_terms_with_duplicate_names():
    # Using a regular function to generate PyDough calculate terms with the
    # function argument same as collection's fields.
    def interval_n(n, name="test"):
        return COUNT(
            customers.WHERE(MONOTONIC(n * 1000, account_balance, (n + 1) * 1000))
        )

    return nations.CALCULATE(
        name,
        redefined_name=name,
        interval_7=interval_n(7),
        interval_4=interval_n(4),
        interval_13=interval_n(13),
    )


def lambda_defined_terms():
    # Using a lambda function to generate PyDough calculate terms
    interval_n = lambda n: COUNT(
        customers.WHERE(MONOTONIC(n * 1000, account_balance, (n + 1) * 1000))
    )

    return nations.CALCULATE(
        name,
        interval_7=interval_n(7),
        interval_4=interval_n(4),
        interval_13=interval_n(13),
    )


def dict_comp_terms():
    # Using a dictionary comprehension to generate PyDough calculate terms
    terms = {"name": name}
    terms.update(
        {
            f"interval_{i}": COUNT(
                customers.WHERE(MONOTONIC(i * 1000, account_balance, (i + 1) * 1000))
            )
            for i in range(3)
        }
    )
    return nations.CALCULATE(**terms)


def list_comp_terms():
    # Using a list comprehension to generate PyDough calculate terms
    terms = [name]
    terms.extend(
        [
            COUNT(customers.WHERE(MONOTONIC(i * 1000, account_balance, (i + 1) * 1000)))
            for i in range(3)
        ]
    )
    return nations.CALCULATE(*terms)


def set_comp_terms():
    # Using a set comprehension to generate PyDough calculate terms
    terms = [name]
    terms.extend(
        set(
            {
                COUNT(
                    customers.WHERE(
                        MONOTONIC(i * 1000, account_balance, (i + 1) * 1000)
                    )
                )
                for i in range(3)
            }
        )
    )
    sorted_terms = sorted(terms, key=lambda x: repr(x))
    return nations.CALCULATE(*sorted_terms)


def generator_comp_terms():
    # Using a generator comprehension to generate PyDough calculate terms
    terms = {"name": name}
    for term, value in (
        (
            f"interval_{i}",
            COUNT(
                customers.WHERE(MONOTONIC(i * 1000, account_balance, (i + 1) * 1000))
            ),
        )
        for i in range(3)
    ):
        terms[term] = value
    return nations.CALCULATE(**terms)


def partition_as_child():
    # Count how many part sizes have an above-average number of parts of that
    # size.
    size_groups = parts.PARTITION(name="sizes", by=size).CALCULATE(n_parts=COUNT(parts))
    return TPCH.CALCULATE(avg_n_parts=AVG(size_groups.n_parts)).CALCULATE(
        n_parts=COUNT(size_groups.WHERE(n_parts > avg_n_parts))
    )


def agg_partition():
    # Doing a global aggregation on the output of a partition aggregation
    yearly_data = (
        orders.CALCULATE(year=YEAR(order_date))
        .PARTITION(name="years", by=year)
        .CALCULATE(n_orders=COUNT(orders))
    )
    return TPCH.CALCULATE(best_year=MAX(yearly_data.n_orders))


def multi_partition_access_1():
    # A use of multiple PARTITION and stepping into partition children that is
    # a no-op.
    data = tickers.CALCULATE(symbol).TOP_K(5, by=symbol.ASC())
    grps_a = data.PARTITION(name="cet", by=(currency, exchange, ticker_type))
    grps_b = grps_a.PARTITION(name="ce", by=(currency, exchange))
    grps_c = grps_b.PARTITION(name="e", by=exchange)
    return grps_c.ce.cet.tickers


def multi_partition_access_2():
    # Identify transactions that are below the average number of shares for
    # transactions of the same combinations of (customer, stock, type), or
    # the same combination of (customer, stock), or the same customer.
    cust_tick_typ_groups = transactions.PARTITION(
        name="ctt_groups",
        by=(customer_id, ticker_id, transaction_type),
    ).CALCULATE(cus_tick_typ_avg_shares=AVG(transactions.shares))
    cust_tick_groups = cust_tick_typ_groups.PARTITION(
        name="ct_groups", by=(customer_id, ticker_id)
    ).CALCULATE(cust_tick_avg_shares=AVG(ctt_groups.transactions.shares))
    cus_groups = cust_tick_groups.PARTITION(name="c_groups", by=customer_id).CALCULATE(
        cust_avg_shares=AVG(ct_groups.ctt_groups.transactions.shares)
    )
    return (
        cus_groups.ct_groups.ctt_groups.transactions.WHERE(
            (shares < cus_tick_typ_avg_shares)
            & (shares < cust_tick_avg_shares)
            & (shares < cust_avg_shares)
        )
        .CALCULATE(
            transaction_id,
            customer.name,
            ticker.symbol,
            transaction_type,
            cus_tick_typ_avg_shares,
            cust_tick_avg_shares,
            cust_avg_shares,
        )
        .ORDER_BY(transaction_id.ASC())
    )


def multi_partition_access_3():
    # Find all daily price updates whose closing price was the high mark for
    # that ticker, but not for tickers of that type.
    data = tickers.CALCULATE(symbol, ticker_type).daily_prices
    ticker_groups = data.PARTITION(name="tickers", by=ticker_id).CALCULATE(
        ticker_high_price=MAX(daily_prices.close)
    )
    type_groups = ticker_groups.daily_prices.PARTITION(
        name="types", by=ticker_type
    ).CALCULATE(type_high_price=MAX(daily_prices.close))
    return (
        type_groups.daily_prices.WHERE(
            (close == ticker_high_price) & (close < type_high_price)
        )
        .CALCULATE(symbol, close)
        .ORDER_BY(symbol.ASC())
    )


def multi_partition_access_4():
    # Find all transactions that were the largest for a customer of that ticker
    # (by number of shares) but not the largest for that customer overall.
    cust_ticker_groups = transactions.PARTITION(
        name="groups", by=(customer_id, ticker_id)
    ).CALCULATE(cust_ticker_max_shares=MAX(transactions.shares))
    cust_groups = cust_ticker_groups.PARTITION(
        name="cust_groups", by=customer_id
    ).CALCULATE(cust_max_shares=MAX(groups.cust_ticker_max_shares))
    return (
        cust_groups.groups.transactions.WHERE(
            (shares >= cust_ticker_max_shares) & (shares < cust_max_shares)
        )
        .CALCULATE(transaction_id)
        .ORDER_BY(transaction_id.ASC())
    )


def multi_partition_access_5():
    # Find all transactions where more than 80% of all transactions of that
    # that ticker were of that type, but less than 20% of all transactions of
    # that type were from that ticker. List the transaction ID, the number of
    # transactions of that ticker/type, ticker, and type. Sort by the number of
    # transactions of that ticker/type, breaking ties by transaction ID.
    ticker_type_groups = transactions.PARTITION(
        name="groups", by=(ticker_id, transaction_type)
    ).CALCULATE(n_ticker_type_trans=COUNT(transactions))
    ticker_groups = ticker_type_groups.PARTITION(
        name="tickers", by=ticker_id
    ).CALCULATE(n_ticker_trans=SUM(groups.n_ticker_type_trans))
    type_groups = ticker_groups.groups.PARTITION(
        name="types", by=transaction_type
    ).CALCULATE(n_type_trans=SUM(groups.n_ticker_type_trans))
    return (
        type_groups.groups.transactions.CALCULATE(
            transaction_id,
            n_ticker_type_trans,
            n_ticker_trans,
            n_type_trans,
        )
        .WHERE(
            ((n_ticker_type_trans / n_ticker_trans) > 0.8)
            & ((n_ticker_type_trans / n_type_trans) < 0.2)
        )
        .ORDER_BY(n_ticker_type_trans.ASC(), transaction_id.ASC())
    )


def multi_partition_access_6():
    # Find all transactions that are the only transaction of that type for
    # that ticker, or the only transaction of that type for that customer,
    # but not the only transaction for that customer, type, or ticker. List
    # the transaction IDs in ascending order.
    ticker_type_groups = transactions.PARTITION(
        name="groups", by=(ticker_id, transaction_type)
    ).CALCULATE(n_ticker_type_trans=COUNT(transactions))
    ticker_groups = ticker_type_groups.PARTITION(name="groups", by=ticker_id).CALCULATE(
        n_ticker_trans=SUM(groups.n_ticker_type_trans)
    )
    type_groups = ticker_groups.groups.PARTITION(
        name="groups", by=transaction_type
    ).CALCULATE(n_type_trans=SUM(groups.n_ticker_type_trans))
    cust_type_groups = type_groups.groups.transactions.PARTITION(
        name="groups",
        by=(customer_id, transaction_type),
    ).CALCULATE(n_cust_type_trans=COUNT(transactions))
    cust_groups = cust_type_groups.PARTITION(name="groups", by=customer_id).CALCULATE(
        n_cust_trans=SUM(groups.n_cust_type_trans)
    )
    return (
        cust_groups.groups.transactions.CALCULATE(transaction_id)
        .WHERE(
            ((n_ticker_type_trans == 1) | (n_cust_type_trans == 1))
            & (n_cust_trans > 1)
            & (n_type_trans > 1)
            & (n_ticker_trans > 1)
        )
        .ORDER_BY(transaction_id.ASC())
    )


def double_partition():
    # Doing a partition aggregation on the output of a partition aggregation
    year_month_data = (
        orders.CALCULATE(year=YEAR(order_date), month=MONTH(order_date))
        .PARTITION(
            name="months",
            by=(year, month),
        )
        .CALCULATE(n_orders=COUNT(orders))
    )
    return year_month_data.PARTITION(
        name="years",
        by=year,
    ).CALCULATE(year, best_month=MAX(months.n_orders))


def triple_partition():
    # Doing three layers of partitioned aggregation. Goal of the question:
    # for each region, calculate the average percentage of purchases made from
    # suppliers in that region belonging to the most common part type shipped
    # from the supplier region to the customer region, averaging across all
    # customer region. Only considers lineitems from June of 1992 where the
    # container is small.
    line_info = (
        parts.CALCULATE(part_type)
        .WHERE(STARTSWITH(container, "SM"))
        .lines.WHERE((MONTH(ship_date) == 6) & (YEAR(ship_date) == 1992))
        .CALCULATE(supp_region=supplier.nation.region.name)
        .order.WHERE(YEAR(order_date) == 1992)
        .CALCULATE(cust_region=customer.nation.region.name)
    )
    rrt_combos = line_info.PARTITION(
        name="combos", by=(supp_region, cust_region, part_type)
    ).CALCULATE(n_instances=COUNT(order))
    rr_combos = rrt_combos.PARTITION(
        name="region_pairs", by=(supp_region, cust_region)
    ).CALCULATE(percentage=100.0 * MAX(combos.n_instances) / SUM(combos.n_instances))
    return (
        rr_combos.PARTITION(
            name="supplier_regions",
            by=supp_region,
        )
        .CALCULATE(supp_region, avg_percentage=AVG(region_pairs.percentage))
        .ORDER_BY(supp_region.ASC())
    )


def hour_minute_day():
    """
    Return the transaction IDs with the hour, minute, and second extracted from
    transaction timestamps for specific ticker symbols ("AAPL","GOOGL","NFLX"),
    ordered by transaction ID in ascending order.
    """
    return (
        transactions.CALCULATE(
            transaction_id, HOUR(date_time), MINUTE(date_time), SECOND(date_time)
        )
        .WHERE(ISIN(ticker.symbol, ("AAPL", "GOOGL", "NFLX")))
        .ORDER_BY(transaction_id.ASC())
    )


def exponentiation():
    return daily_prices.CALCULATE(
        low_square=low**2,
        low_sqrt=SQRT(low),
        low_cbrt=POWER(low, 1 / 3),
    ).TOP_K(10, by=low_square.ASC())


def args_kwargs():
    def impl(*args, **kwargs):
        terms = {}
        for i, color in enumerate(args):
            terms[f"n_{color}"] = COUNT(parts.WHERE(CONTAINS(part_name, color)))
        for n, size in kwargs.items():
            terms[n] = COUNT(parts.WHERE(size == size))
        return TPCH.CALCULATE(**terms)

    result = impl("tomato", "almond", small=10, large=40)
    return result


def unpacking():
    start, end = (1992, 1994)
    selects_orders = orders.WHERE(MONOTONIC(start, YEAR(order_date), end))
    return selects_orders


def nested_unpacking():
    a, (b, c) = ["GERMANY", ["FRANCE", "ARGENTINA"]]
    chosen_customers = customers.WHERE(ISIN(nation.name, (a, b, c)))
    return chosen_customers


def unpacking_in_iterable():
    terms = {}
    for i, j in zip(range(5), range(1992, 1997)):
        terms[f"c{i}"] = COUNT(orders.WHERE(YEAR(order_date) == j))
    return nations.CALCULATE(**terms)


def with_import_statement():
    import tempfile as tf
    import logging.config

    logging.config.dictConfig({"version": 1})
    from os import path as opath

    result1 = opath.join("folder", "file.txt")
    with tf.TemporaryFile() as tf_handle1, tf.TemporaryFile() as tf_handle2:
        tf_handle1.write(b"Canada")
        tf_handle2.write(b"Mexico")
        tf_handle1.seek(0)
        tf_handle2.seek(0)
        a = str(tf_handle1.read().decode("utf-8"))
        b = str(tf_handle2.read().decode("utf-8"))
        return customers.WHERE(ISIN(nation.name, (a, b)))


def exception_handling():
    try:
        raise Exception("Canada")
    except Exception as e:
        country = str(e)
        return customers.WHERE(ISIN(nation.name, (country, "Mexico")))
    finally:
        pass


def class_handling():
    class Customer:
        def __init__(self, countries):
            self._countries = countries

        def query(self):
            return customers.WHERE(ISIN(nation.name, self._countries))

    return Customer(("Canada", "Mexico")).query()


def annotated_assignment():
    direction1: str
    direction1 = "SOUTH "
    specific_region: tuple[str, str] = "WEST", "AMERICA"
    chosen_region: str = direction1 + " ".join(specific_region)
    return nations.WHERE(region.name == chosen_region)


def abs_round_magic_method():
    return daily_prices.CALCULATE(
        abs_low=abs(low), round_low=round(low, 2), round_zero=round(low)
    )


def years_months_days_hours_datediff():
    y1_datetime = datetime.datetime(2025, 5, 2, 11, 00, 0)
    return (
        transactions.WHERE((YEAR(date_time) < 2025))
        .CALCULATE(
            x=date_time,
            y1=y1_datetime,
            years_diff=DATEDIFF("years", date_time, y1_datetime),
            c_years_diff=DATEDIFF("YEARS", date_time, y1_datetime),
            c_y_diff=DATEDIFF("Y", date_time, y1_datetime),
            y_diff=DATEDIFF("y", date_time, y1_datetime),
            months_diff=DATEDIFF("months", date_time, y1_datetime),
            c_months_diff=DATEDIFF("MONTHS", date_time, y1_datetime),
            mm_diff=DATEDIFF("mm", date_time, y1_datetime),
            days_diff=DATEDIFF("days", date_time, y1_datetime),
            c_days_diff=DATEDIFF("DAYS", date_time, y1_datetime),
            c_d_diff=DATEDIFF("D", date_time, y1_datetime),
            d_diff=DATEDIFF("d", date_time, y1_datetime),
            hours_diff=DATEDIFF("hours", date_time, y1_datetime),
            c_hours_diff=DATEDIFF("HOURS", date_time, y1_datetime),
            c_h_diff=DATEDIFF("H", date_time, y1_datetime),
        )
        .TOP_K(30, by=years_diff.ASC())
    )


def minutes_seconds_datediff():
    y_datetime = datetime.datetime(2023, 4, 3, 13, 16, 30)
    return (
        transactions.WHERE(YEAR(date_time) <= 2024)
        .CALCULATE(
            x=date_time,
            y=y_datetime,
            minutes_diff=DATEDIFF("m", date_time, y_datetime),
            seconds_diff=DATEDIFF("s", date_time, y_datetime),
        )
        .TOP_K(30, by=x.DESC())
    )


def simple_week_sampler():
    x_dt = datetime.datetime(2025, 3, 10, 11, 00, 0)
    y_dt = datetime.datetime(2025, 3, 14, 11, 00, 0)
    y_dt2 = datetime.datetime(2025, 3, 15, 11, 00, 0)
    y_dt3 = datetime.datetime(2025, 3, 16, 11, 00, 0)
    y_dt4 = datetime.datetime(2025, 3, 17, 11, 00, 0)
    y_dt5 = datetime.datetime(2025, 3, 18, 11, 00, 0)
    y_dt6 = datetime.datetime(2025, 3, 19, 11, 00, 0)
    y_dt7 = datetime.datetime(2025, 3, 20, 11, 00, 0)
    y_dt8 = datetime.datetime(2025, 3, 21, 11, 00, 0)
    return Broker.CALCULATE(
        weeks_diff=DATEDIFF("weeks", x_dt, y_dt),
        sow1=DATETIME(y_dt, "start of week"),
        sow2=DATETIME(y_dt2, "start of week"),
        sow3=DATETIME(y_dt3, "start of week"),
        sow4=DATETIME(y_dt4, "start of week"),
        sow5=DATETIME(y_dt5, "start of week"),
        sow6=DATETIME(y_dt6, "start of week"),
        sow7=DATETIME(y_dt7, "start of week"),
        sow8=DATETIME(y_dt8, "start of week"),
        dayname1=DAYNAME(y_dt),
        dayname2=DAYNAME(y_dt2),
        dayname3=DAYNAME(y_dt3),
        dayname4=DAYNAME(y_dt4),
        dayname5=DAYNAME(y_dt5),
        dayname6=DAYNAME(y_dt6),
        dayname7=DAYNAME(y_dt7),
        dayname8=DAYNAME(y_dt8),
        dayofweek1=DAYOFWEEK(y_dt),
        dayofweek2=DAYOFWEEK(y_dt2),
        dayofweek3=DAYOFWEEK(y_dt3),
        dayofweek4=DAYOFWEEK(y_dt4),
        dayofweek5=DAYOFWEEK(y_dt5),
        dayofweek6=DAYOFWEEK(y_dt6),
        dayofweek7=DAYOFWEEK(y_dt7),
        dayofweek8=DAYOFWEEK(y_dt8),
    )


def transaction_week_sampler():
    return transactions.WHERE(
        (YEAR(date_time) < 2025) & (DAY(date_time) > 1)
    ).CALCULATE(
        date_time,
        sow=DATETIME(date_time, "start of week"),
        dayname=DAYNAME(date_time),
        dayofweek=DAYOFWEEK(date_time),
    )


def week_offset():
    return transactions.WHERE(
        (YEAR(date_time) < 2025) & (DAY(date_time) > 1)
    ).CALCULATE(
        date_time,
        week_adj1=DATETIME(date_time, "1 week"),
        week_adj2=DATETIME(date_time, "-1 week"),
        week_adj3=DATETIME(date_time, "1 h", "2 w"),
        week_adj4=DATETIME(date_time, "-1 s", "2 w"),
        week_adj5=DATETIME(date_time, "1 d", "2 w"),
        week_adj6=DATETIME(date_time, "-1 m", "2 w"),
        week_adj7=DATETIME(date_time, "1 mm", "2 w"),
        week_adj8=DATETIME(date_time, "1 y", "2 w"),
    )


def datediff():
    y1_datetime = datetime.datetime(2025, 5, 2, 11, 00, 0)
    y_datetime = datetime.datetime(2023, 4, 3, 13, 16, 30)
    return (
        transactions.WHERE((YEAR(date_time) < 2025))
        .CALCULATE(
            x=date_time,
            y1=y1_datetime,
            y=y_datetime,
            years_diff=DATEDIFF("years", date_time, y1_datetime),
            months_diff=DATEDIFF("months", date_time, y1_datetime),
            days_diff=DATEDIFF("days", date_time, y1_datetime),
            hours_diff=DATEDIFF("hours", date_time, y1_datetime),
            minutes_diff=DATEDIFF("minutes", date_time, y_datetime),
            seconds_diff=DATEDIFF("seconds", date_time, y_datetime),
        )
        .TOP_K(30, by=years_diff.ASC())
    )


def padding_functions():
    return customers.CALCULATE(
        original_name=name,
        ref_rpad=RPAD("Cust0001", 30, "*"),
        ref_lpad=LPAD("Cust0001", 30, "*"),
        right_padded=RPAD(name, 30, "*"),
        left_padded=LPAD(name, 30, "#"),
        truncated_right=RPAD(name, 8, "-"),
        truncated_left=LPAD(name, 8, "-"),
        zero_pad_right=RPAD(name, 0, "."),
        zero_pad_left=LPAD(name, 0, "."),
        right_padded_space=RPAD(name, 30, " "),
        left_padded_space=LPAD(name, 30, " "),
    ).TOP_K(5, by=name.ASC())


def step_slicing():
    return customers.CALCULATE(
        name,
        neg_none_step=name[-2::1],
        pos_none_step=name[3::1],
        none_pos_step=name[:3:1],
        none_neg_step=name[:-2:1],
        pos_pos_step=name[2:4:1],
        pos_neg_step=name[2:-2:1],
        neg_pos_step=name[-12:2:1],
        neg_neg_step=name[-4:-2:1],
        inbetween_chars=name[1:-1:1],
        empty1=name[2:2:1],
        empty2=name[-2:-2:1],
        empty3=name[-2:-4:1],
        empty4=name[4:2:1],
        oob1=name[100:200:1],
        oob2=name[-200:-100:1],
        oob3=name[100::1],
        oob4=name[-200::1],
        oob5=name[:100:1],
        oob6=name[:-200:1],
        oob7=name[100:-200:1],
        oob8=name[-200:100:1],
        oob9=name[100:-1:1],
        oob10=name[-100:-1:1],
        oob11=name[-3:100:1],
        oob12=name[-3:-100:1],
        zero1=name[0:0:1],
        zero2=name[0:1:1],
        zero3=name[-1:0:1],
        zero4=name[1:0:1],
        zero5=name[0:-1:1],
        zero6=name[0:-20:1],
        zero7=name[0:100:1],
        zero8=name[20:0:1],
        zero9=name[-20:0:1],
        wo_step1=name[-2:],
        wo_step2=name[3:],
        wo_step3=name[:3],
        wo_step4=name[:-2],
        wo_step5=name[2:4],
        wo_step6=name[2:-2],
        wo_step7=name[-4:2],
        wo_step8=name[-4:-2],
        wo_step9=name[2:2],
    )


def sign():
    return (
        daily_prices.CALCULATE(
            high,
            high_neg=-1 * high,
            high_zero=0 * high,
        )
        .TOP_K(5, by=high.ASC())
        .CALCULATE(
            high,
            high_neg,
            high_zero,
            sign_high=SIGN(high),
            sign_high_neg=SIGN(high_neg),
            sign_high_zero=SIGN(high_zero),
        )
    )


def find():
    return customers.WHERE(name == "Alex Rodriguez").CALCULATE(
        name,
        idx_Alex=FIND(name, "Alex"),
        idx_Rodriguez=FIND(name, "Rodriguez"),
        idx_bob=FIND(name, "bob"),
        idx_e=FIND(name, "e"),
        idx_space=FIND(name, " "),
        idx_of_R=FIND(name, "R"),
        idx_of_Alex_Rodriguez=FIND(name, "Alex Rodriguez"),
    )


def strip():
    return (
        customers.WHERE(name == "Alex Rodriguez")
        .CALCULATE(
            name,
            alt_name1="  Alex Rodriguez  ",
            alt_name2="aeiAlex Rodriguezaeiou",
            alt_name3=";;Alex Rodriguez;;",
            alt_name4="""
    Alex Rodriguez
        """,  # equivalent to "\n\tAlex Rodriguez\n"
        )
        .CALCULATE(
            stripped_name=STRIP(name, "Alex Rodriguez"),
            stripped_name1=STRIP(name),
            stripped_name_with_chars=STRIP(name, "lAez"),
            stripped_alt_name1=STRIP(alt_name1),
            stripped_alt_name2=STRIP(alt_name2, "aeiou"),
            stripped_alt_name3=STRIP(alt_name3, ";"),
            stripped_alt_name4=STRIP(alt_name4),
        )
    )


def replace():
    return (
        customers.WHERE(name == "Alex Rodriguez")
        .CALCULATE(
            name,
            empty_string="",
            simple_text="banana",
            overlapping_text="aaa",
            case_sensitive_text="Apple",
            unicode_text="café",
            special_chars="a$b$c",
            same_all="foofoo",
            nested_replace="abcabc",
        )
        .CALCULATE(
            replaced_name=REPLACE(name, "Alex", "Alexander"),
            removed_name=REPLACE(name, "Alex"),
            case_name=REPLACE(name, "alex"),
            replace_empty_text=REPLACE(empty_string, "a", "b"),  # ""
            replace_with_empty_pattern=REPLACE("abc", "", "x"),  # "abc"
            remove_substring=REPLACE("abc", "a", ""),  # "bc"
            empty_all=REPLACE("", "", ""),  # ""
            substring_not_found=REPLACE("hello", "x", "y"),  # "hello"
            overlapping_matches=REPLACE(overlapping_text, "aa", "b"),  # "ba"
            multiple_occurrences=REPLACE("a a a", "a", "b"),  # "b b b"
            case_sensitive=REPLACE(case_sensitive_text, "apple", "orange"),  # "Apple"
            unicode_handling=REPLACE(unicode_text, "é", "e"),  # "cafe"
            special_character_replace=REPLACE(special_chars, "$", ""),  # "abc"
            longer_replacement=REPLACE("x", "x", "xyz"),  # "xyz"
            shorter_replacement=REPLACE("xyzxyz", "xyz", "x"),  # "xx"
            same_value_args=REPLACE(same_all, "foo", "foo"),  # "foofoo"
            nested_like_replace=REPLACE(
                nested_replace, "abc", "abcabc"
            ),  # "abcabcabcabc"
        )
    )


def str_count():
    return (
        customers.WHERE(name == "Alex Rodriguez")
        .CALCULATE(
            name,
            char_substring="e",
            char_substring_2="f",
            lower_lastname="rodriguez",
            lastname="Rodriguez",
            empty_string="",
            space=" ",
            special_chars="$@!%",
            numbers="123",
        )
        .CALCULATE(  # cases
            count_letter=STRCOUNT(name, char_substring),
            not_in_letter=STRCOUNT(name, char_substring_2),
            count_lastname=STRCOUNT(name, lastname),
            count_sensitive_lastname=STRCOUNT(name, lower_lastname),
            count_empty=STRCOUNT(name, empty_string),
            all_empty=STRCOUNT(empty_string, empty_string),
            first_empty=STRCOUNT(empty_string, name),
            count_numbers=STRCOUNT("123456789", numbers),
            count_char_numbers=STRCOUNT("Alex123Rodrigez12", "12"),
            no_occurence=STRCOUNT(name, "Luis"),
            count_spaces=STRCOUNT(name, space),
            space_arround=STRCOUNT(" Hello world ", space),
            count_special_chars=STRCOUNT("Hello $@!%", special_chars),
            no_overlapping=STRCOUNT("aaaa", "aa"),
            no_overlapping_2=STRCOUNT("ababab", "aba"),
            entire_string_match=STRCOUNT(name, name),
            longer_substring=STRCOUNT(lastname, name),
        )
    )


def get_part_multiple():
    k = INTEGER(_id[1:])
    return (
        customers.WHERE(k <= 4)
        .CALCULATE(
            k,
            p1=GETPART(name, " ", k),
            p2=GETPART(name, " ", -k),
            p3=GETPART(email, ".", k),
            p4=GETPART(email, ".", -k),
            p5=GETPART(phone, "-", k),
            p6=GETPART(phone, "-", -k),
            p7=GETPART(postal_code, "00", k),
            p8=GETPART(postal_code, "00", -k),
            p9=GETPART(name, "!", k),
            p10=GETPART(name, "@", -k),
            p11=GETPART(name, "aa", k),
            p12=GETPART(name, "#$*", -k),
            p13=GETPART(name, "", k),
            p14=GETPART("", " ", k),
            p15=GETPART(name, " ", 0),
            p16=GETPART(state, state, k),
            p17=GETPART(GETPART(phone, "-", 1), "5", k),
            p18=GETPART(postal_code, "0", k),
        )
        .ORDER_BY(k.ASC())
    )


def get_part_single():
    return customers.WHERE(name == "Alex Rodriguez").CALCULATE(
        last_name=GETPART(name, " ", -1)
    )


def extract_colors():
    return parts.CALCULATE(
        key,
        c1=UPPER(GETPART(name, " ", 1)),
        c2=UPPER(GETPART(name, " ", 2)),
        c3=UPPER(GETPART(name, " ", 3)),
        c4=UPPER(GETPART(name, " ", 4)),
        c5=UPPER(GETPART(name, " ", 5)),
        c6=UPPER(GETPART(name, " ", 6)),
    ).TOP_K(5, by=key.ASC())


def singular1():
    # Singular in CALCULATE & WHERE
    nation_4 = nations.WHERE(key == 4).SINGULAR()
    return regions.CALCULATE(name, nation_4_name=nation_4.name)


def singular2():
    # Singular in CALCULATE & WHERE with multiple SINGULARs
    return nations.CALCULATE(
        name,
        okey=customers.WHERE(key == 1)
        .SINGULAR()
        .orders.WHERE(key == 454791)
        .SINGULAR()
        .key,
    )


def singular3():
    # Singular in ORDER_BY
    # Finds the names of the first 5 customers alphabetically, and sorts them
    # by the date of the most expensive order they ever made.
    return (
        customers.TOP_K(5, by=name.ASC())
        .CALCULATE(name)
        .ORDER_BY(
            orders.WHERE(RANKING(by=total_price.DESC(), per="customers") == 1)
            .SINGULAR()
            .order_date.ASC(na_pos="last")
        )
    )


def singular4():
    # Singular in TOP_K
    # Finds the names of the first 5 customers from nation #6
    # by the date of the most expensive high-priority order they ever made.
    return (
        customers.WHERE(nation_key == 6)
        .TOP_K(
            5,
            by=orders.WHERE(order_priority == "1-URGENT")
            .WHERE(RANKING(by=total_price.DESC(), per="customers") == 1)
            .SINGULAR()
            .order_date.ASC(na_pos="last"),
        )
        .CALCULATE(name)
    )


def singular5():
    # Find the ship date of the most expensive line item per each container
    # presented in parts (breaking ties in favor of the smaller ship date).
    # Find the 5 containers with the earliest such date, breaking ties
    # alphabetically. For the purpose of this question, only shipments made by
    # rail without tax and for parts from Brand#13.
    top_containers = parts.WHERE(brand == "Brand#13").PARTITION(
        name="containers",
        by=container,
    )
    highest_price_line = (
        parts.lines.WHERE((ship_mode == "RAIL") & (tax == 0))
        .WHERE(
            RANKING(by=(extended_price.DESC(), ship_date.ASC()), per="containers") == 1
        )
        .SINGULAR()
    )
    return (
        top_containers.WHERE(HAS(highest_price_line))
        .CALCULATE(
            container,
            highest_price_ship_date=highest_price_line.ship_date,
        )
        .TOP_K(5, by=(highest_price_ship_date.ASC(), container.ASC()))
    )


def singular6():
    # For each customer in nation #4, what is the first part they received in
    # an order handled by clerk #17, and the nation it came from (breaking ties
    # in favor of the one with the largest revenue)? Include the 5 customers
    # with the earliest such received parts (breaking ties alphabetically by
    # customer name).
    revenue = extended_price * (1 - discount)
    lq = (
        orders.WHERE(clerk == "Clerk#000000017")
        .lines.CALCULATE(receipt_date)
        .WHERE(RANKING(by=(receipt_date.ASC(), revenue.DESC()), per="customers") == 1)
        .SINGULAR()
        .supplier.nation.CALCULATE(nation_name=name)
    )
    selected_customers = customers.WHERE((nation_key == 4) & HAS(lq))
    return selected_customers.CALCULATE(name, lq.receipt_date, lq.nation_name).TOP_K(
        5, by=(receipt_date.ASC(), name.ASC())
    )


def singular7():
    # For each supplier in nation #20, what is the most popular part (by # of
    # purchases) they supplied in 1994 (breaking ties alphabetically by part
    # name)? Include the 5 suppliers with the highest number of purchases along
    # with part name, and number of orders (breaking ties alphabetically by
    # supplier name). Only consider parts from Brand#13.
    best_part = (
        supply_records.WHERE(part.brand == "Brand#13")
        .CALCULATE(
            n_orders=COUNT(lines.WHERE(YEAR(ship_date) == 1994)),
            part_name=part.name,
        )
        .WHERE(RANKING(by=(n_orders.DESC(), part_name.ASC()), per="suppliers") == 1)
        .SINGULAR()
    )
    return (
        suppliers.WHERE(nation_key == 20)
        .CALCULATE(
            supplier_name=name,
            part_name=best_part.part_name,
            n_orders=best_part.n_orders,
        )
        .TOP_K(5, by=(n_orders.DESC(), supplier_name.ASC()))
    )


def quarter_function_test():
    return TPCH.CALCULATE(
        QUARTER("2023-01-15"),  # Q1
        QUARTER("2023-02-28"),  # Q1
        QUARTER("2023-03-31"),  # Q1
        QUARTER("2023-04-01"),  # Q2
        QUARTER("2023-05-15"),  # Q2
        QUARTER("2023-06-30"),  # Q2
        QUARTER("2023-07-01"),  # Q3
        QUARTER("2023-08-15"),  # Q3
        QUARTER("2023-09-30"),  # Q3
        QUARTER("2023-10-01"),  # Q4
        QUARTER("2023-11-15"),  # Q4
        QUARTER("2023-12-31"),  # Q4
        QUARTER(pd.Timestamp("2024-02-29 12:30:45")),  # Q1 (leap year)
        # Testing start of quarter for different months
        q1_jan=DATETIME(
            "2023-01-15 12:30:45", "start of quarter"
        ),  # Should be 2023-01-01
        q1_feb=DATETIME(
            "2023-02-28 12:30:45", "start of quarter"
        ),  # Should be 2023-01-01
        q1_mar=DATETIME("2023-03-31", "start of quarter"),  # Should be 2023-01-01
        q2_apr=DATETIME("2023-04-01", "start of quarter"),  # Should be 2023-04-01
        q2_may=DATETIME(
            "2023-05-15 12:30:45", "start of quarter"
        ),  # Should be 2023-04-01
        q2_jun=DATETIME(
            "2023-06-30 12:30:45", "start of quarter"
        ),  # Should be 2023-04-01
        q3_jul=DATETIME(
            "2023-07-01 12:30:45", "start of quarter"
        ),  # Should be 2023-07-01
        q3_aug=DATETIME("2023-08-15", "start of quarter"),  # Should be 2023-07-01
        q3_sep=DATETIME("2023-09-30", "start of quarter"),  # Should be 2023-07-01
        q4_oct=DATETIME("2023-10-01", "start of quarter"),  # Should be 2023-10-01
        q4_nov=DATETIME("2023-11-15", "start of quarter"),  # Should be 2023-10-01
        q4_dec=DATETIME("2023-12-31", "start of quarter"),  # Should be 2023-10-01
        # Testing with different aliases for 'start of quarter'
        ts_q1=DATETIME(pd.Timestamp("2024-02-29 12:30:45"), "start of quarter"),
        alias1=DATETIME("2023-05-15", "START OF QUARTER"),
        alias2=DATETIME("2023-08-15", "Start Of Quarter"),
        alias3=DATETIME("2023-11-15", "\n  Start  Of\tQuarter\n\n"),
        alias4=DATETIME("2023-02-15", "\tSTART\tOF\tquarter\t"),
        # Testing chained operations
        chain1=DATETIME("2023-05-15", "start of quarter", "+1 day", "+2 hours"),
        chain2=DATETIME("2023-08-15", "start of quarter", "start of day"),
        # Oct 1 from previous quarter
        chain3=DATETIME("2023-11-15", "-1 month", "start of quarter"),
        plus_1q=DATETIME("2023-01-15 12:30:45", "+1 quarter"),  # Should be 2023-04-15
        plus_2q=DATETIME("2023-01-15 12:30:45", "+2 quarters"),  # Should be 2023-07-15
        plus_3q=DATETIME("2023-01-15", "+3 quarters"),  # Should be 2023-10-15
        minus_1q=DATETIME("2023-01-15 12:30:45", "-1 quarter"),  # Should be 2022-10-15
        minus_2q=DATETIME("2023-01-15 12:30:45", "-2 quarters"),  # Should be 2022-07-15
        minus_3q=DATETIME("2023-01-15", "-3 quarters"),  # Should be 2022-04-15
        # Testing with different syntax for quarter offsets
        syntax1=DATETIME("2023-05-15", " +1 QUARTER "),
        syntax2=DATETIME("2023-08-15", "+2 Q"),
        syntax3=DATETIME("2023-11-15", " \n +\t3 \nQuarters \n\r "),
        syntax4=DATETIME("2023-02-15", "\t-\t2\tq\t"),
        # Basic quarter differences within the same year
        q_diff1=DATEDIFF("quarter", "2023-01-15", "2023-04-15"),  # 1 quarter
        q_diff2=DATEDIFF("quarter", "2023-01-15", "2023-07-15"),  # 2 quarters
        q_diff3=DATEDIFF("quarter", "2023-01-15", "2023-10-15"),  # 3 quarters
        q_diff4=DATEDIFF("quarter", "2023-01-15", "2023-12-31"),  # 3 quarters
        # Quarter differences across year boundaries
        q_diff5=DATEDIFF("quarter", "2023-01-15", "2024-01-15"),  # 4 quarters
        q_diff6=DATEDIFF("quarter", "2023-01-15", "2024-04-15"),  # 5 quarters
        q_diff7=DATEDIFF("quarter", "2022-10-15", "2024-04-15"),  # 6 quarters
        q_diff8=DATEDIFF("quarter", "2020-01-01", "2025-01-01"),  # 20 quarters
        # Negative quarter differences (earlier end date)
        q_diff9=DATEDIFF("quarter", "2023-04-15", "2023-01-15"),  # -1 quarter
        q_diff10=DATEDIFF("quarter", "2024-01-15", "2023-01-15"),  # -4 quarters
        # Testing with partial quarters (should still count as crossing a quarter boundary)
        q_diff11=DATEDIFF("quarter", "2023-03-31", "2023-04-01"),  # 1 quarter
        q_diff12=DATEDIFF("quarter", "2023-12-31", "2024-01-01"),  # 1 quarter
        # QUARTER(order_date),
    )


def order_quarter_test():
    return (
        orders.WHERE(YEAR(order_date) == 1995)
        .TOP_K(1, by=order_date.ASC())
        .CALCULATE(
            order_date,
            quarter=QUARTER(order_date),
            quarter_start=DATETIME(order_date, "start of quarter"),
            next_quarter=DATETIME(order_date, "+1 quarter"),
            prev_quarter=DATETIME(order_date, "-1 quarter"),
            two_quarters_ahead=DATETIME(order_date, "+2 quarters"),
            two_quarters_behind=DATETIME(order_date, "-2 quarters"),
            quarters_since_1995=DATEDIFF("quarter", "1995-01-01", order_date),
            quarters_until_2000=DATEDIFF("quarter", order_date, "2000-01-01"),
            same_quarter_prev_year=DATETIME(order_date, "-4 quarters"),
            same_quarter_next_year=DATETIME(order_date, "+4 quarters"),
        )
    )


def simple_int_float_string_cast():
    # String format specifiers for date/time with a static datetime
    # Using a specific date: 2023-07-15 14:30:45
    # static_date = DATETIME("2023-07-15 14:30:45")
    return TPCH.CALCULATE(
        i1=INTEGER(1),
        i2=INTEGER(2.2),
        i3=INTEGER("3"),
        i4=INTEGER("4.3"),
        i5=INTEGER("-5.888"),
        i6=INTEGER(-6.0),
        f1=FLOAT(1.0),
        f2=FLOAT(2.2),
        f3=FLOAT("3"),
        f4=FLOAT("4.3"),
        f5=FLOAT("-5.888"),
        f6=FLOAT(-6.0),
        f7=FLOAT(0.0),
        s1=STRING(1),
        s2=STRING(2.2),
        s3=STRING("3"),
        s4=STRING("4.3"),
        s5=STRING("-5.888"),
        s6=STRING(-6.0),
        s7=STRING(0.0),
        s8=STRING("0.0"),
        s9=STRING("abc def"),
    )


def string_format_specifiers_sqlite():
    # String format specifiers for date/time with a static datetime
    # Works for SQLite versions >= v3.43.2
    # Using a specific date: 2023-07-15 14:30:45
    static_date = pd.Timestamp("2023-07-15 14:30:45")
    return TPCH.CALCULATE(
        # day of month: 01-31
        d1=STRING(static_date, "%d"),
        # day of month without leading zero: 1-31
        d2=STRING("2023-07-15 14:30:45", "%e"),
        # fractional seconds: SS.SSS
        d3=STRING("2023-07-15 14:30:45", "%f"),
        # ISO 8601 date: YYYY-MM-DD
        d4=STRING("2023-07-15 14:30:45", "%F"),
        # hour: 00-24
        d5=STRING(static_date, "%H"),
        # hour for 12-hour clock: 01-12
        d6=STRING(static_date, "%I"),
        # day of year: 001-366
        d7=STRING(static_date, "%j"),
        # Julian day number (fractional)
        d8=STRING(static_date, "%J"),
        # hour without leading zero: 0-24
        d9=STRING(static_date, "%k"),
        # %I without leading zero: 1-12
        d10=STRING(static_date, "%l"),
        # month: 01-12
        d11=STRING(static_date, "%m"),
        # minute: 00-59
        d12=STRING(static_date, "%M"),
        # "AM" or "PM" depending on the hour
        d13=STRING(static_date, "%p"),
        # "am" or "pm" depending on the hour
        d14=STRING(static_date, "%P"),
        # ISO 8601 time: HH:MM
        d15=STRING(static_date, "%R"),
        # seconds since 1970-01-01
        d16=STRING(static_date, "%s"),
        # seconds: 00-59
        d17=STRING(static_date, "%S"),
        # ISO 8601 time: HH:MM:SS
        d18=STRING(static_date, "%T"),
        # day of week 1-7 with Monday==1
        d19=STRING(static_date, "%u"),
        # day of week 0-6 with Sunday==0
        d20=STRING(static_date, "%w"),
        # week of year (00-53)
        d21=STRING(static_date, "%W"),
        # year: 0000-9999
        d22=STRING(static_date, "%Y"),
        # month-day-year
        d23=STRING(static_date, "%m-%d-%Y"),
    )


def part_reduced_size():
    # What are the top 5 line items with the highest discounts
    # on parts with the lowest retail prices casted to integers?
    # Include the part name, the reduced size, the retail price, the discount,
    # and the date in day-month-year, month/day, and AM/PM format.
    return (
        parts.CALCULATE(
            reduced_size=FLOAT(size / 2.5),
            retail_price_int=INTEGER(retail_price),
            message=JOIN_STRINGS(
                "",
                "old size: ",
                STRING(size),
            ),
        )
        .TOP_K(2, by=retail_price_int.ASC())
        .lines.CALCULATE(
            reduced_size,
            retail_price_int,
            message,
            discount,
            # day-month-year: 15-07-2023
            date_dmy=STRING(receipt_date, "%d-%m-%Y"),
            # month/day: 07/15
            date_md=STRING(receipt_date, "%m/%d"),
            # AM or PM
            am_pm=STRING(receipt_date, "%H:%M%p"),
        )
        .TOP_K(5, by=discount.DESC())
    )


def simple_smallest_or_largest():
    return TPCH.CALCULATE(
        s1=SMALLEST(20, 10),
        s2=SMALLEST(20, 20),
        s3=SMALLEST(20, 10, 0),
        s4=SMALLEST(20, 10, 10, -1, -2, 100, -200),
        s5=SMALLEST(20, 10, None, 100, 200),
        s6=SMALLEST(20.22, 10.22, -0.34),
        s7=SMALLEST(
            datetime.datetime(2025, 1, 1),
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2023, 1, 1),
        ),
        s8=SMALLEST("", "alphabet soup", "Hello World"),
        s9=SMALLEST(None, "alphabet soup", "Hello World"),
        l1=LARGEST(20, 10),
        l2=LARGEST(20, 20),
        l3=LARGEST(20, 10, 0),
        l4=LARGEST(20, 10, 10, -1, -2, 100, -200, 300),
        l5=LARGEST(20, 10, None, 100, 200),
        l6=LARGEST(20.22, 100.22, -0.34),
        l7=LARGEST(
            datetime.datetime(2025, 1, 1),
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2023, 1, 1),
        ),
        l8=LARGEST("", "alphabet soup", "Hello World"),
        l9=LARGEST(None, "alphabet soup", "Hello World"),
    )


def avg_acctbal_wo_debt():
    # For each region, what is the average account balance of all
    # customers in a hypothetical scenario where all debt was erased
    return regions.CALCULATE(
        region_name=name,
        avg_bal_without_debt_erasure=AVG(LARGEST(nations.customers.account_balance, 0)),
    )


def odate_and_rdate_avggap():
    # Average gap, in days, for shipments between when they were ordered
    # versus when they were expected to arrive (or when they actually arrived,
    # if they were early), for shipments done via rail
    delay_info = lines.WHERE(HAS(order) & (ship_mode == "RAIL")).CALCULATE(
        day_gap=DATEDIFF("days", order.order_date, SMALLEST(commit_date, receipt_date))
    )
    return TPCH.CALCULATE(avg_gap=AVG(delay_info.day_gap))


def simple_var_std():
    return nations.WHERE(ISIN(name, ("ALGERIA", "ARGENTINA"))).CALCULATE(
        name,
        var=VAR(suppliers.account_balance),
        std=STD(suppliers.account_balance),
        sample_var=VAR(suppliers.account_balance, type="sample"),
        sample_std=STD(suppliers.account_balance, type="sample"),
        pop_var=VAR(suppliers.account_balance, type="population"),
        pop_std=STD(suppliers.account_balance, type="population"),
    )


def simple_var_std_with_nulls():
    first_customers = customers.WHERE(ISIN(key, (1, 2, 3))).CALCULATE(
        key_0=KEEP_IF(account_balance, key > 3),
        key_1=KEEP_IF(account_balance, key > 2),
        key_2=KEEP_IF(account_balance, key > 1),
    )
    return TPCH.CALCULATE(
        var_samp_0_nnull=VAR(first_customers.key_0, type="sample"),
        var_samp_1_nnull=VAR(first_customers.key_1, type="sample"),
        var_samp_2_nnull=VAR(first_customers.key_2, type="sample"),
        var_pop_0_nnull=VAR(first_customers.key_0),
        var_pop_1_nnull=VAR(first_customers.key_1),
        var_pop_2_nnull=VAR(first_customers.key_2),
        std_samp_0_nnull=STD(first_customers.key_0, type="sample"),
        std_samp_1_nnull=STD(first_customers.key_1, type="sample"),
        std_samp_2_nnull=STD(first_customers.key_2, type="sample"),
        std_pop_0_nnull=STD(first_customers.key_0),
        std_pop_1_nnull=STD(first_customers.key_1),
        std_pop_2_nnull=STD(first_customers.key_2),
    )


def deep_best_analysis():
    richest_nation_cust = customers.BEST(
        by=(account_balance.DESC(), key.ASC()), per="nations"
    )
    richest_region_cust = customers.BEST(
        by=(account_balance.DESC(), key.ASC()), per="regions"
    )
    most_avail_region_supply = supply_records.BEST(
        by=(available_quantity.DESC(), part_key.ASC()), per="regions"
    )
    best_region_supplier = (
        suppliers.WHERE(HAS(most_avail_region_supply))
        .CALCULATE(
            p_key=most_avail_region_supply.part_key,
            p_qty=most_avail_region_supply.available_quantity,
        )
        .BEST(by=(p_qty.DESC(), key.ASC()), per="regions")
    )
    return (
        regions.CALCULATE(r_name=name)
        .nations.WHERE(HAS(richest_nation_cust))
        .CALCULATE(
            r_name=r_name,
            n_name=name,
            c_key=richest_nation_cust.key,
            c_bal=richest_nation_cust.account_balance,
            cr_bal=richest_region_cust.account_balance,
            s_key=best_region_supplier.key,
            p_key=best_region_supplier.p_key,
            p_qty=best_region_supplier.p_qty,
            cg_key=customers.BEST(by=(account_balance.DESC(), key.ASC())).key,
        )
        .TOP_K(10, by=n_name.ASC())
    )


def simple_cross_1():
    # Every combination of region names
    return (
        regions.CALCULATE(r1=name)
        .CROSS(regions)
        .CALCULATE(r1, r2=name)
        .ORDER_BY(r1.ASC(), r2.ASC())
    )


def simple_cross_2():
    # Every combination of region names, excluding cases where
    # the two regions are the same
    return (
        regions.CALCULATE(r1=name)
        .CROSS(regions)
        .CALCULATE(r1, r2=name)
        .WHERE(r1 != r2)
        .ORDER_BY(r1.ASC(), r2.ASC())
    )


def simple_cross_3():
    # Same as simple_cross_8, but for every combination of nation1 in ASIA
    # and nation2 in AMERICA in April-1992, only consider orders shipped
    # by "SHIP" mode, only considering customers in debt.
    nation_combinations = (
        regions.WHERE(name == "ASIA")
        .nations.CALCULATE(s_key=key, supplier_nation=name)
        .CROSS(regions.WHERE(name == "AMERICA").nations.CALCULATE(customer_nation=name))
    )
    selected_purchases = (
        customers.WHERE(account_balance < 0)
        .orders.WHERE((YEAR(order_date) == 1992) & (MONTH(order_date) == 4))
        .lines.WHERE((supplier.nation_key == s_key) & (ship_mode == "SHIP"))
    )
    return nation_combinations.CALCULATE(
        supplier_nation,
        customer_nation,
        nation_combinations=COUNT(selected_purchases),
    ).WHERE(HAS(selected_purchases))


def simple_cross_4():
    # For each region, count how many OTHER regions
    # have the same first letter as the region
    other_regions = CROSS(regions).WHERE(
        (name != region_name) & (name[:1] == region_name[:1])
    )
    return (
        regions.CALCULATE(region_name=name)
        .CALCULATE(region_name, n_other_regions=COUNT(other_regions))
        .ORDER_BY(region_name.ASC())
    )


def simple_cross_5():
    # For every combination of part size & order priority, which order priority
    # had the highest quantity of parts of that size shipped in January 1998
    # without tax or discount and via ship? Only consider the 10 smallest part
    # sizes, and large parts.
    sizes = (
        parts.WHERE(STARTSWITH(container, "LG"))
        .PARTITION(name="sizes", by=size)
        .CALCULATE(part_size=size)
        .TOP_K(10, by=size.ASC())
    )
    order_info = (
        orders.CALCULATE(order_priority)
        .WHERE((YEAR(order_date) == 1998) & (MONTH(order_date) == 1))
        .lines.WHERE(
            (part.size == part_size)
            & (tax == 0)
            & (discount == 0)
            & (ship_mode == "SHIP")
            & STARTSWITH(part.container, "LG")
        )
    )
    best_priority = (
        CROSS(order_info.PARTITION(name="priorities", by=order_priority))
        .CALCULATE(total_qty=SUM(lines.quantity))
        .BEST(by=total_qty.DESC(), per="sizes")
    )
    return sizes.CALCULATE(
        part_size,
        best_order_priority=best_priority.order_priority,
        best_order_priority_qty=best_priority.total_qty,
    )


def simple_cross_6():
    # Count how many combinations of 2 DISTINCT orders exist that were from the
    # same day from the same customer, only considering orders handled
    # by any of the clerks whose numbers is at least 900.
    predicates = INTEGER(clerk[6:]) >= 900
    original_orders = orders.CALCULATE(
        original_customer_key=customer_key,
        original_order_key=key,
        original_order_date=order_date,
    ).WHERE(predicates)
    other_orders_by_same_customer_same_date = original_orders.CROSS(
        orders.WHERE(predicates)
    ).WHERE(
        (customer_key == original_customer_key)
        & (key > original_order_key)
        & (order_date == original_order_date)
    )
    return TPCH.CALCULATE(n_pairs=COUNT(other_orders_by_same_customer_same_date))


def simple_cross_7():
    # For every order with status P, count how many DIFFERENT orders with
    # status "P" were made by the same
    # customer on the same date, considering only the first 5 orders
    # with the highest number of other orders, breaking ties by key
    original_orders = orders.WHERE(order_status == "P").CALCULATE(
        original_customer_key=customer_key,
        original_order_key=key,
        original_order_date=order_date,
    )
    return original_orders.CALCULATE(
        original_order_key,
        n_other_orders=COUNT(
            CROSS(
                orders.WHERE(
                    (customer_key == original_customer_key)
                    & (order_status == "P")
                    & (key > original_order_key)
                    & (order_date == original_order_date)
                )
            )
        ),
    ).TOP_K(5, by=[n_other_orders.DESC(), original_order_key.ASC()])


def simple_cross_8():
    # For every unique combination of region1 & region2, count how many
    # lineitems shipped in March-1998 and clerk #7
    # were from a supplier in debt in region1 by a customer in region2 in the
    # automobile market segment.
    selected_lineitems = (
        nations.customers.WHERE(market_segment == "AUTOMOBILE")
        .orders.WHERE(clerk == "Clerk#000000007")
        .lines.WHERE(
            (YEAR(ship_date) == 1998)
            & (MONTH(ship_date) == 3)
            & (
                supplier.WHERE(account_balance < 0).nation.region.name
                == supplier_region
            )
        )
    )
    region_combinations = regions.CALCULATE(supplier_region=name).CROSS(
        regions.CALCULATE(customer_region=name)
    )
    return region_combinations.WHERE(HAS(selected_lineitems)).CALCULATE(
        supplier_region, customer_region, region_combinations=COUNT(selected_lineitems)
    )


def simple_cross_9():
    # Every combination of different nations from the same region
    return (
        regions.CALCULATE(r1=name)
        .nations.CALCULATE(n1=name)
        .CROSS(regions.CALCULATE(r2=name).nations)
        .CALCULATE(n1, n2=name)
        .WHERE((r1 == r2) & (n1 != n2))
        .ORDER_BY(n1.ASC(), n2.ASC())
        .TOP_K(10, by=[n1.ASC(), n2.ASC()])
    )


def simple_cross_10():
    # For each region, count how many nations in OTHER regions
    # have the same first letter as the region
    other_regions = CROSS(regions.CALCULATE(r2=name).nations).WHERE(
        (r2 != region_name) & (name[:1] == region_name[:1])
    )
    return (
        regions.CALCULATE(region_name=name)
        .CALCULATE(region_name, n_other_nations=COUNT(other_regions))
        .ORDER_BY(region_name.ASC())
    )


def simple_cross_11():
    # Count how many orders were made on the first date that the orders
    # were made (using CROSS to derive global values)
    global_info = TPCH.CALCULATE(min_date=MIN(orders.order_date))
    selected_orders = orders.WHERE(order_date == CROSS(global_info).SINGULAR().min_date)
    return TPCH.CALCULATE(n=COUNT(selected_orders))


def simple_cross_12():
    # List every combination of order priority and market segment,
    # alphabetized.
    priorities = orders.PARTITION(name="priorities", by=order_priority).CALCULATE(
        order_priority
    )
    segments = customers.PARTITION(name="segments", by=market_segment)
    return (
        priorities.CROSS(segments)
        .CALCULATE(order_priority, market_segment)
        .ORDER_BY(order_priority.ASC(), market_segment.ASC())
    )


def quantile_function_test_1():
    selected_orders = customers.orders.WHERE(YEAR(order_date) == 1998)
    return TPCH.CALCULATE(
        seventieth_order_price=QUANTILE(selected_orders.total_price, 0.7)
    )


def quantile_function_test_2():
    selected_orders = customers.orders.WHERE(YEAR(order_date) == 1998)
    return nations.TOP_K(5, by=name.ASC()).CALCULATE(
        region_name=region.name,
        nation_name=name,
        orders_min=QUANTILE(selected_orders.total_price, 0.0),
        orders_1_percent=QUANTILE(selected_orders.total_price, 0.01),
        orders_10_percent=QUANTILE(selected_orders.total_price, 0.1),
        orders_25_percent=QUANTILE(selected_orders.total_price, 0.25),
        orders_median=QUANTILE(selected_orders.total_price, 0.5),
        orders_75_percent=QUANTILE(selected_orders.total_price, 0.75),
        orders_90_percent=QUANTILE(selected_orders.total_price, 0.90),
        orders_99_percent=QUANTILE(selected_orders.total_price, 0.99),
        orders_max=QUANTILE(selected_orders.total_price, 1.0),
    )


def quantile_function_test_3():
    selected_orders = customers.orders.WHERE(YEAR(order_date) == 1998).CALCULATE(
        value=KEEP_IF(total_price, order_priority == "1-URGENT")
    )
    return nations.TOP_K(5, by=name.ASC()).CALCULATE(
        region_name=region.name,
        nation_name=name,
        orders_min=QUANTILE(selected_orders.total_price, 0.0),
        orders_1_percent=QUANTILE(selected_orders.total_price, 0.01),
        orders_10_percent=QUANTILE(selected_orders.total_price, 0.1),
        orders_25_percent=QUANTILE(selected_orders.total_price, 0.25),
        orders_median=QUANTILE(selected_orders.total_price, 0.5),
        orders_75_percent=QUANTILE(selected_orders.total_price, 0.75),
        orders_90_percent=QUANTILE(selected_orders.total_price, 0.90),
        orders_99_percent=QUANTILE(selected_orders.total_price, 0.99),
        orders_max=QUANTILE(selected_orders.total_price, 1.0),
    )


def quantile_function_test_4():
    selected_orders = customers.orders.WHERE(clerk == "Clerk#000000272").CALCULATE(
        value=KEEP_IF(total_price, order_priority == "1-URGENT")
    )
    return nations.TOP_K(5, by=name.ASC()).CALCULATE(
        region_name=region.name,
        nation_name=name,
        orders_min=QUANTILE(selected_orders.total_price, 0.0),
        orders_1_percent=QUANTILE(selected_orders.total_price, 0.01),
        orders_10_percent=QUANTILE(selected_orders.total_price, 0.1),
        orders_25_percent=QUANTILE(selected_orders.total_price, 0.25),
        orders_median=QUANTILE(selected_orders.total_price, 0.5),
        orders_75_percent=QUANTILE(selected_orders.total_price, 0.75),
        orders_90_percent=QUANTILE(selected_orders.total_price, 0.90),
        orders_99_percent=QUANTILE(selected_orders.total_price, 0.99),
        orders_max=QUANTILE(selected_orders.total_price, 1.0),
    )
