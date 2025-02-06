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
        name, rank=RANKING(by=COUNT(customers).DESC(), levels=1)
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
        Parts.WHERE(
            STARTSWITH(container, "SM"),
        )
        .lines.WHERE((MONTH(ship_date) == 6) & (YEAR(ship_date) == 1992))(
            supp_region=supplier.nation.region.name,
        )
        .order.WHERE(YEAR(order_date) == 1992)(
            supp_region=BACK(1).supp_region,
            part_type=BACK(2).part_type,
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


def correl_1():
    # Correlated back reference example #1: simple 1-step correlated reference
    # For each region, count how many of its its nations start with the same
    # letter as the region. This is a true correlated join doing an aggregated
    # access without requiring the RHS be present.
    return Regions(
        name, n_prefix_nations=COUNT(nations.WHERE(name[:1] == BACK(1).name[:1]))
    )


def correl_2():
    # Correlated back reference example #2: simple 2-step correlated reference
    # For each region's nations, count how many customers have a comment
    # starting with the same letter as the region. Exclude regions that start
    # with the letter a. This is a true correlated join doing an aggregated
    # access without requiring the RHS be present.
    selected_custs = customers.WHERE(comment[:1] == LOWER(BACK(2).name[:1]))
    return Regions.WHERE(~STARTSWITH(name, "A")).nations(
        name,
        n_selected_custs=COUNT(selected_custs),
    )


def correl_3():
    # Correlated back reference example #3: double-layer correlated reference
    # For every every region, count how many of its nations have a customer
    # whose comment starts with the same letter as the region. This is a true
    # correlated join doing an aggregated access without requiring the RHS be
    # present.
    selected_custs = customers.WHERE(comment[:1] == LOWER(BACK(2).name[:1]))
    return Regions(name, n_nations=COUNT(nations.WHERE(HAS(selected_custs))))


def correl_4():
    # Correlated back reference example #4: 2-step correlated HASNOT
    # Find every nation that does not have a customer whose account balance is
    # within $5 of the smallest known account balance globally.
    # (This is a correlated ANTI-join)
    selected_customers = customers.WHERE(acctbal <= (BACK(2).smallest_bal + 5.0))
    return (
        TPCH(
            smallest_bal=MIN(Customers.acctbal),
        )
        .Nations(name)
        .WHERE(HASNOT(selected_customers))
        .ORDER_BY(name.ASC())
    )


def correl_5():
    # Correlated back reference example #5: 2-step correlated HAS
    # Find every region that has at least 1 supplier whose account balance is
    # within $4 of the smallest known account balance globally.
    # (This is a correlated SEMI-join)
    selected_suppliers = nations.suppliers.WHERE(
        account_balance <= (BACK(3).smallest_bal + 4.0)
    )
    return (
        TPCH(
            smallest_bal=MIN(Suppliers.account_balance),
        )
        .Regions(name)
        .WHERE(HAS(selected_suppliers))
        .ORDER_BY(name.ASC())
    )


def correl_6():
    # Correlated back reference example #6: simple 1-step correlated reference
    # For each region, count how many of its its nations start with the same
    # letter as the region, but only keep regions with at least one such nation.
    # This is a true correlated join doing an aggregated access that does NOT
    # require that records without the RHS be kept.
    selected_nations = nations.WHERE(name[:1] == BACK(1).name[:1])
    return Regions.WHERE(HAS(selected_nations))(
        name, n_prefix_nations=COUNT(selected_nations)
    )


def correl_7():
    # Correlated back reference example #6: deleted correlated reference
    # For each region, count how many of its its nations start with the same
    # letter as the region, but only keep regions without at least one such
    # nation. The true correlated join is trumped by the correlated ANTI-join.
    selected_nations = nations.WHERE(name[:1] == BACK(1).name[:1])
    return Regions.WHERE(HASNOT(selected_nations))(
        name, n_prefix_nations=COUNT(selected_nations)
    )


def correl_8():
    # Correlated back reference example #8: non-agg correlated reference
    # For each nation, fetch the name of its region, but filter the reigon
    # so it only keeps it if it starts with the same letter as the nation
    # (otherwise, returns NULL). This is a true correlated join doing an
    # access without aggregation without requiring the RHS be
    # present.
    aug_region = region.WHERE(name[:1] == BACK(1).name[:1])
    return Nations(name, rname=aug_region.name).ORDER_BY(name.ASC())


def correl_9():
    # Correlated back reference example #9: non-agg correlated reference
    # For each nation, fetch the name of its region, but filter the reigon
    # so it only keeps it if it starts with the same letter as the nation
    # (otherwise, omit the nation). This is a true correlated join doing an
    # access that also requires the RHS records be present.
    aug_region = region.WHERE(name[:1] == BACK(1).name[:1])
    return Nations.WHERE(HAS(aug_region))(name, rname=aug_region.name).ORDER_BY(
        name.ASC()
    )


def correl_10():
    # Correlated back reference example #10: deleted correlated reference
    # For each nation, fetch the name of its region, but filter the reigon
    # so it only keeps it if it starts with the same letter as the nation
    # (otherwise, returns NULL), and also filter the nations to only keep
    # records where the region is NULL. The true correlated join is trumped by
    # the correlated ANTI-join.
    aug_region = region.WHERE(name[:1] == BACK(1).name[:1])
    return Nations.WHERE(HASNOT(aug_region))(name, rname=aug_region.name).ORDER_BY(
        name.ASC()
    )


def correl_11():
    # Correlated back reference example #11: backref out of partition child.
    # Which part brands have at least 1 part that more than 40% above the
    # average retail price for all parts from that brand.
    # (This is a correlated SEMI-join)
    brands = PARTITION(Parts, name="p", by=brand)(avg_price=AVG(p.retail_price))
    outlier_parts = p.WHERE(retail_price > 1.4 * BACK(1).avg_price)
    selected_brands = brands.WHERE(HAS(outlier_parts))
    return selected_brands(brand).ORDER_BY(brand.ASC())


def correl_12():
    # Correlated back reference example #12: backref out of partition child.
    # Which part brands have at least 1 part that is above the average retail
    # price for parts of that brand, below the average retail price for all
    # parts, and has a size below 3.
    # (This is a correlated SEMI-join)
    global_info = TPCH(avg_price=AVG(Parts.retail_price))
    brands = global_info.PARTITION(Parts, name="p", by=brand)(
        avg_price=AVG(p.retail_price)
    )
    selected_parts = p.WHERE(
        (retail_price > BACK(1).avg_price)
        & (retail_price < BACK(2).avg_price)
        & (size < 3)
    )
    selected_brands = brands.WHERE(HAS(selected_parts))
    return selected_brands(brand).ORDER_BY(brand.ASC())


def correl_13():
    # Correlated back reference example #13: multiple correlation.
    # Count how many suppliers sell at least one part where the retail price
    # is less than a 50% markup over the supply cost. Only considers suppliers
    # from nations #1/#2/#3, and small parts.
    # (This is a correlated SEMI-joins)
    selected_part = part.WHERE(STARTSWITH(container, "SM")).WHERE(
        retail_price < (BACK(1).supplycost * 1.5)
    )
    selected_supply_records = supply_records.WHERE(HAS(selected_part))
    supplier_info = Suppliers.WHERE(nation_key <= 3)(
        avg_price=AVG(supply_records.part.retail_price)
    )
    selected_suppliers = supplier_info.WHERE(COUNT(selected_supply_records) > 0)
    return TPCH(n=COUNT(selected_suppliers))


def correl_14():
    # Correlated back reference example #14: multiple correlation.
    # Count how many suppliers sell at least one part where the retail price
    # is less than a 50% markup over the supply cost, and the retail price of
    # the part is below the average for all parts from the supplier. Only
    # considers suppliers from nations #19, and LG DRUM parts.
    # (This is multiple correlated SEMI-joins)
    selected_part = part.WHERE(container == "LG DRUM").WHERE(
        (retail_price < (BACK(1).supplycost * 1.5)) & (retail_price < BACK(2).avg_price)
    )
    selected_supply_records = supply_records.WHERE(HAS(selected_part))
    supplier_info = Suppliers.WHERE(nation_key == 19)(
        avg_price=AVG(supply_records.part.retail_price)
    )
    selected_suppliers = supplier_info.WHERE(HAS(selected_supply_records))
    return TPCH(n=COUNT(selected_suppliers))


def correl_15():
    # Correlated back reference example #15: multiple correlation.
    # Count how many suppliers sell at least one part where the retail price
    # is less than a 50% markup over the supply cost, and the retail price of
    # the part is below the 90% of the average of the retail price for all
    # parts globally and below the average for all parts from the supplier.
    # Only considers suppliers from nations #19, and LG DRUM parts.
    # (This is multiple correlated SEMI-joins & a correlated aggregate)
    selected_part = part.WHERE(container == "LG DRUM").WHERE(
        (retail_price < (BACK(1).supplycost * 1.5))
        & (retail_price < BACK(2).avg_price)
        & (retail_price < BACK(3).avg_price * 0.9)
    )
    selected_supply_records = supply_records.WHERE(HAS(selected_part))
    supplier_info = Suppliers.WHERE(nation_key == 19)(
        avg_price=AVG(supply_records.part.retail_price)
    )
    selected_suppliers = supplier_info.WHERE(HAS(selected_supply_records))
    global_info = TPCH(avg_price=AVG(Parts.retail_price))
    return global_info(n=COUNT(selected_suppliers))


def correl_16():
    # Correlated back reference example #16: hybrid tree order of operations.
    # Count how many european suppliers have the exact same percentile value
    # of account balance (relative to all other suppliers) as at least one
    # customer's percentile value of account balance relative to all other
    # customers. Percentile should be measured down to increments of 0.01%.
    # (This is a correlated SEMI-joins)
    selected_customers = nation(rname=region.name).customers.WHERE(
        (PERCENTILE(by=(acctbal.ASC(), key.ASC()), n_buckets=10000) == BACK(2).tile)
        & (BACK(1).rname == "EUROPE")
    )
    supplier_info = Suppliers(
        tile=PERCENTILE(by=(account_balance.ASC(), key.ASC()), n_buckets=10000)
    )
    selected_suppliers = supplier_info.WHERE(HAS(selected_customers))
    return TPCH(n=COUNT(selected_suppliers))


def correl_17():
    # Correlated back reference example #17: hybrid tree order of operations.
    # An extremely roundabout way of getting each region_name-nation_name
    # pair as a string.
    # (This is a correlated singular/semi access)
    region_info = region(fname=JOIN_STRINGS("-", LOWER(name), BACK(1).lname))
    nation_info = Nations(lname=LOWER(name)).WHERE(HAS(region_info))
    return nation_info(fullname=region_info.fname).ORDER_BY(fullname.ASC())


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
