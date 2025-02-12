# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def simple_scan():
    return Orders(key)


def simple_filter():
    # Note: The SQL is non-deterministic once we add nested expressions.
    return Orders(o_orderkey=key, o_totalprice=total_price).WHERE(o_totalprice < 1000.0)


def simple_scan_top_five():
    return Orders(key).TOP_K(5, by=key.ASC())


def simple_filter_top_five():
    return Orders(key, total_price).WHERE(total_price < 1000.0).TOP_K(5, by=key.DESC())


def rank_a():
    return Customers(rank=RANKING(by=acctbal.DESC()))


def rank_b():
    return Orders(rank=RANKING(by=(order_priority.ASC()), allow_ties=True))


def rank_c():
    return Orders(
        order_date, rank=RANKING(by=order_date.ASC(), allow_ties=True, dense=True)
    )


def rank_nations_by_region():
    return Nations(name, rank=RANKING(by=region.name.ASC(), allow_ties=True))


def rank_nations_per_region_by_customers():
    return Regions.nations(
        name, rank=RANKING(by=COUNT(customers).DESC(), levels="Regions")
    ).TOP_K(5, by=rank.ASC())


def rank_parts_per_supplier_region_by_size():
    return Regions.nations.suppliers.supply_records.part(
        key,
        region=BACK(4).name,
        rank=RANKING(
            by=(size.DESC(), container.DESC(), part_type.DESC()),
            levels=4,
            allow_ties=True,
            dense=True,
        ),
    ).TOP_K(15, by=key.ASC())


def rank_with_filters_a():
    return (
        Customers(n=name, r=RANKING(by=acctbal.DESC()))
        .WHERE(ENDSWITH(name, "0"))
        .WHERE(r <= 30)
    )


def rank_with_filters_b():
    return (
        Customers(n=name, r=RANKING(by=acctbal.DESC()))
        .WHERE(r <= 30)
        .WHERE(ENDSWITH(name, "0"))
    )


def rank_with_filters_c():
    return (
        PARTITION(Parts, name="p", by=size)
        .TOP_K(5, by=size.DESC())
        .p(size, name)
        .WHERE(RANKING(by=retail_price.DESC(), levels=1) == 1)
    )


def percentile_nations():
    # For every nation, give its name & its bucket from 1-5 ordered by name
    # alphabetically
    return Nations(name, p=PERCENTILE(by=name.ASC(), n_buckets=5))


def percentile_customers_per_region():
    # For each region, give the name of all customers in that region that are
    # in the 95th percentile in terms of account balance (larger percentile
    # means more money) and whose phone number ends in two zeros, sorted by the
    # name of the customers
    return (
        Regions.nations.customers(name)
        .WHERE((PERCENTILE(by=(acctbal.ASC()), levels=2) == 95) & ENDSWITH(phone, "00"))
        .ORDER_BY(name.ASC())
    )


def regional_suppliers_percentile():
    # For each region, find the suppliers in the top 0.1% by number of parts
    # they supply, breaking ties by name, only keeping the suppliers in the top
    pct = PERCENTILE(
        by=(COUNT(supply_records).ASC(), name.ASC()), levels=2, n_buckets=1000
    )
    return Regions.nations.suppliers(name).WHERE(HAS(supply_records) & (pct == 1000))


def function_sampler():
    # Examples of using different functions
    return (
        Regions.nations.customers(
            a=JOIN_STRINGS("-", BACK(2).name, BACK(1).name, name[16:]),
            b=ROUND(acctbal, 1),
            c=KEEP_IF(name, phone[:1] == "3"),
            d=PRESENT(KEEP_IF(name, phone[1:2] == "1")),
            e=ABSENT(KEEP_IF(name, phone[14:] == "7")),
        )
        .WHERE(MONOTONIC(0.0, acctbal, 100.0))
        .TOP_K(10, by=address.ASC())
    )


def loop_generated_terms():
    # Using a loop & dictionary to generate PyDough calc terms
    terms = {"name": name}
    for i in range(3):
        terms[f"interval_{i}"] = COUNT(
            customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000))
        )
    return Nations(**terms)


def function_defined_terms():
    # Using a regular function to generate PyDough calc terms
    def interval_n(n):
        return COUNT(customers.WHERE(MONOTONIC(n * 1000, acctbal, (n + 1) * 1000)))

    return Nations(
        name,
        interval_7=interval_n(7),
        interval_4=interval_n(4),
        interval_13=interval_n(13),
    )


def lambda_defined_terms():
    # Using a lambda function to generate PyDough calc terms
    interval_n = lambda n: COUNT(
        customers.WHERE(MONOTONIC(n * 1000, acctbal, (n + 1) * 1000))
    )

    return Nations(
        name,
        interval_7=interval_n(7),
        interval_4=interval_n(4),
        interval_13=interval_n(13),
    )


def dict_comp_terms():
    # Using a dictionary comprehension to generate PyDough calc terms
    terms = {"name": name}
    terms.update(
        {
            f"interval_{i}": COUNT(
                customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000))
            )
            for i in range(3)
        }
    )
    return Nations(**terms)


def list_comp_terms():
    # Using a list comprehension to generate PyDough calc terms
    terms = [name]
    terms.extend(
        [
            COUNT(customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000)))
            for i in range(3)
        ]
    )
    return Nations(**terms)


def set_comp_terms():
    # Using a set comprehension to generate PyDough calc terms
    terms = [name]
    terms.extend(
        set(
            {
                COUNT(customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000)))
                for i in range(3)
            }
        )
    )
    return Nations(**terms)


def generator_comp_terms():
    # Using a generator comprehension to generate PyDough calc terms
    terms = {"name": name}
    for term, value in (
        (
            f"interval_{i}",
            COUNT(customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000))),
        )
        for i in range(3)
    ):
        terms[term] = value
    return Nations(**terms)


def agg_partition():
    # Doing a global aggregation on the output of a partition aggregation
    yearly_data = PARTITION(Orders(year=YEAR(order_date)), name="orders", by=year)(
        n_orders=COUNT(orders)
    )
    return TPCH(best_year=MAX(yearly_data.n_orders))


def double_partition():
    # Doing a partition aggregation on the output of a partition aggregation
    year_month_data = PARTITION(
        Orders(year=YEAR(order_date), month=MONTH(order_date)),
        name="orders",
        by=(year, month),
    )(n_orders=COUNT(orders))
    return PARTITION(
        year_month_data,
        name="months",
        by=year,
    )(year, best_month=MAX(months.n_orders))


def triple_partition():
    # Doing three layers of partitioned aggregation. Goal of the question:
    # for each region, calculate the average percentage of purchases made from
    # suppliers in that region belonging to the most common part type shipped
    # from the supplier region to the customer region, averaging across all
    # customer region. Only considers lineitems from June of 1992 where the
    # container is small.
    line_info = (
        Parts(part_type=part_type)
        .WHERE(
            STARTSWITH(container, "SM"),
        )
        .lines.WHERE((MONTH(ship_date) == 6) & (YEAR(ship_date) == 1992))(
            supp_region=supplier.nation.region.name,
        )
        .order.WHERE(YEAR(order_date) == 1992)(
            cust_region=customer.nation.region.name,
        )
    )
    rrt_combos = PARTITION(
        line_info, name="lines", by=(supp_region, cust_region, part_type)
    )(n_instances=COUNT(lines))
    rr_combos = PARTITION(rrt_combos, name="part_types", by=(supp_region, cust_region))(
        percentage=100.0 * MAX(part_types.n_instances) / SUM(part_types.n_instances)
    )
    return PARTITION(
        rr_combos,
        name="cust_regions",
        by=supp_region,
    )(supp_region, avg_percentage=AVG(cust_regions.percentage)).ORDER_BY(
        supp_region.ASC()
    )


def hour_minute_day():
    """
    Return the transaction IDs with the hour, minute, and second extracted from
    transaction timestamps for specific ticker symbols ("AAPL","GOOGL","NFLX"),
    ordered by transaction ID in ascending order.
    """
    return (
        Transactions(
            transaction_id, HOUR(date_time), MINUTE(date_time), SECOND(date_time)
        )
        .WHERE(ISIN(ticker.symbol, ("AAPL", "GOOGL", "NFLX")))
        .ORDER_BY(transaction_id.ASC())
    )


def exponentiation():
    return DailyPrices(
        low_square=low**2,
        low_sqrt=SQRT(low),
        low_cbrt=POWER(low, 1 / 3),
    ).TOP_K(10, by=low_square.ASC())
