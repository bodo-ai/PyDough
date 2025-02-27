"""
Variant of `simple_pydough_functions.py` for functions testing edge cases in
correlation & de-correlation handling.
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def correl_1():
    # Correlated back reference example #1: simple 1-step correlated reference
    # For each region, count how many of its its nations start with the same
    # letter as the region. This is a true correlated join doing an aggregated
    # access without requiring the RHS be present.
    return (
        Regions.CALCULATE(region_name=name)
        .CALCULATE(
            region_name,
            n_prefix_nations=COUNT(nations.WHERE(name[:1] == region_name[:1])),
        )
        .ORDER_BY(region_name.ASC())
    )


def correl_2():
    # Correlated back reference example #2: simple 2-step correlated reference
    # For each region's nations, count how many customers have a comment
    # starting with the same letter as the region. Exclude regions that start
    # with the letter a. This is a true correlated join doing an aggregated
    # access without requiring the RHS be present.
    selected_custs = customers.WHERE(comment[:1] == LOWER(region_name[:1]))
    return (
        Regions.CALCULATE(region_name=name)
        .WHERE(~STARTSWITH(name, "A"))
        .nations.CALCULATE(
            name,
            n_selected_custs=COUNT(selected_custs),
        )
        .ORDER_BY(name.ASC())
    )


def correl_3():
    # Correlated back reference example #3: double-layer correlated reference
    # For every every region, count how many of its nations have a customer
    # whose comment starts with the same 2 letter as the region. This is a true
    # correlated join doing an aggregated access without requiring the RHS be
    # present.
    selected_custs = customers.WHERE(comment[:2] == LOWER(region_name[:2]))
    return (
        Regions.CALCULATE(region_name=name)
        .CALCULATE(region_name, n_nations=COUNT(nations.WHERE(HAS(selected_custs))))
        .ORDER_BY(name.ASC())
    )


def correl_4():
    # Correlated back reference example #4: 2-step correlated HASNOT
    # Find every nation that does not have a customer whose account balance is
    # within $5 of the smallest known account balance globally.
    # (This is a correlated ANTI-join)
    selected_customers = customers.WHERE(acctbal <= (smallest_bal + 5.0))
    return (
        TPCH.CALCULATE(
            smallest_bal=MIN(Customers.acctbal),
        )
        .Nations.CALCULATE(name)
        .WHERE(HASNOT(selected_customers))
        .ORDER_BY(name.ASC())
    )


def correl_5():
    # Correlated back reference example #5: 2-step correlated HAS
    # Find every region that has at least 1 supplier whose account balance is
    # within $4 of the smallest known account balance globally.
    # (This is a correlated SEMI-join)
    selected_suppliers = nations.suppliers.WHERE(
        account_balance <= (smallest_bal + 4.0)
    )
    return (
        TPCH.CALCULATE(
            smallest_bal=MIN(Suppliers.account_balance),
        )
        .Regions.CALCULATE(name)
        .WHERE(HAS(selected_suppliers))
        .ORDER_BY(name.ASC())
    )


def correl_6():
    # Correlated back reference example #6: simple 1-step correlated reference
    # For each region, count how many of its its nations start with the same
    # letter as the region, but only keep regions with at least one such nation.
    # This is a true correlated join doing an aggregated access that does NOT
    # require that records without the RHS be kept.
    selected_nations = nations.WHERE(name[:1] == region_name[:1])
    return (
        Regions.CALCULATE(region_name=name)
        .WHERE(HAS(selected_nations))
        .CALCULATE(name, n_prefix_nations=COUNT(selected_nations))
    )


def correl_7():
    # Correlated back reference example #6: deleted correlated reference
    # For each region, count how many of its its nations start with the same
    # letter as the region, but only keep regions without at least one such
    # nation. The true correlated join is trumped by the correlated ANTI-join.
    selected_nations = nations.WHERE(name[:1] == region_name[:1])
    return (
        Regions.CALCULATE(region_name=name)
        .WHERE(HASNOT(selected_nations))
        .CALCULATE(name, n_prefix_nations=COUNT(selected_nations))
    )


def correl_8():
    # Correlated back reference example #8: non-agg correlated reference
    # For each nation, fetch the name of its region, but filter the reigon
    # so it only keeps it if it starts with the same letter as the nation
    # (otherwise, returns NULL). This is a true correlated join doing an
    # access without aggregation without requiring the RHS be
    # present.
    aug_region = region.WHERE(name[:1] == nation_name[:1])
    return (
        Nations.CALCULATE(nation_name=name)
        .CALCULATE(name, rname=aug_region.name)
        .ORDER_BY(name.ASC())
    )


def correl_9():
    # Correlated back reference example #9: non-agg correlated reference
    # For each nation, fetch the name of its region, but filter the reigon
    # so it only keeps it if it starts with the same letter as the nation
    # (otherwise, omit the nation). This is a true correlated join doing an
    # access that also requires the RHS records be present.
    aug_region = region.WHERE(name[:1] == nation_name[:1])
    return (
        Nations.CALCULATE(nation_name=name)
        .WHERE(HAS(aug_region))
        .CALCULATE(name, rname=aug_region.name)
        .ORDER_BY(name.ASC())
    )


def correl_10():
    # Correlated back reference example #10: deleted correlated reference
    # For each nation, fetch the name of its region, but filter the reigon
    # so it only keeps it if it starts with the same letter as the nation
    # (otherwise, returns NULL), and also filter the nations to only keep
    # records where the region is NULL. The true correlated join is trumped by
    # the correlated ANTI-join.
    aug_region = region.WHERE(name[:1] == nation_name[:1])
    return (
        Nations.CALCULATE(nation_name=name)
        .WHERE(HASNOT(aug_region))
        .CALCULATE(name, rname=aug_region.name)
        .ORDER_BY(name.ASC())
    )


def correl_11():
    # Correlated back reference example #11: backref out of partition child.
    # Which part brands have at least 1 part that more than 40% above the
    # average retail price for all parts from that brand.
    # (This is a correlated SEMI-join)
    brands = PARTITION(Parts, name="p", by=brand).CALCULATE(
        avg_price=AVG(p.retail_price)
    )
    outlier_parts = p.WHERE(retail_price > 1.4 * avg_price)
    selected_brands = brands.WHERE(HAS(outlier_parts))
    return selected_brands.CALCULATE(brand).ORDER_BY(brand.ASC())


def correl_12():
    # Correlated back reference example #12: backref out of partition child.
    # Which part brands have at least 1 part that is above the average retail
    # price for parts of that brand, below the average retail price for all
    # parts, and has a size below 3.
    # (This is a correlated SEMI-join)
    global_info = TPCH.CALCULATE(global_avg_price=AVG(Parts.retail_price))
    brands = global_info.PARTITION(Parts, name="p", by=brand).CALCULATE(
        brand_avg_price=AVG(p.retail_price)
    )
    selected_parts = p.WHERE(
        (retail_price > brand_avg_price)
        & (retail_price < global_avg_price)
        & (size < 3)
    )
    selected_brands = brands.WHERE(HAS(selected_parts))
    return selected_brands.CALCULATE(brand).ORDER_BY(brand.ASC())


def correl_13():
    # Correlated back reference example #13: multiple correlation.
    # Count how many suppliers sell at least one part where the retail price
    # is less than a 50% markup over the supply cost. Only considers suppliers
    # from nations #1/#2/#3, and small parts.
    # (This is a correlated SEMI-joins)
    selected_part = part.WHERE(
        STARTSWITH(container, "SM") & (retail_price < (supplycost * 1.5))
    )
    selected_supply_records = supply_records.CALCULATE(supplycost).WHERE(
        HAS(selected_part)
    )
    supplier_info = Suppliers.WHERE(nation_key <= 3).CALCULATE(
        avg_price=AVG(supply_records.part.retail_price)
    )
    selected_suppliers = supplier_info.WHERE(COUNT(selected_supply_records) > 0)
    return TPCH.CALCULATE(n=COUNT(selected_suppliers))


def correl_14():
    # Correlated back reference example #14: multiple correlation.
    # Count how many suppliers sell at least one part where the retail price
    # is less than a 50% markup over the supply cost, and the retail price of
    # the part is below the average for all parts from the supplier. Only
    # considers suppliers from nations #19, and LG DRUM parts.
    # (This is multiple correlated SEMI-joins)
    selected_part = part.WHERE(
        (container == "LG DRUM")
        & (retail_price < (supplycost * 1.5))
        & (retail_price < avg_price)
    )
    selected_supply_records = supply_records.CALCULATE(supplycost).WHERE(
        HAS(selected_part)
    )
    supplier_info = Suppliers.WHERE(nation_key == 19).CALCULATE(
        avg_price=AVG(supply_records.part.retail_price)
    )
    selected_suppliers = supplier_info.WHERE(HAS(selected_supply_records))
    return TPCH.CALCULATE(n=COUNT(selected_suppliers))


def correl_15():
    # Correlated back reference example #15: multiple correlation.
    # Count how many suppliers sell at least one part where the retail price
    # is less than a 50% markup over the supply cost, and the retail price of
    # the part is below the 85% of the average of the retail price for all
    # parts globally and below the average for all parts from the supplier.
    # Only considers suppliers from nations #19, and LG DRUM parts.
    # (This is multiple correlated SEMI-joins & a correlated aggregate)
    selected_part = part.WHERE(
        (container == "LG DRUM")
        & (retail_price < (supplycost * 1.5))
        & (retail_price < supplier_avg_price)
        & (retail_price < global_avg_price * 0.85)
    )
    selected_supply_records = supply_records.CALCULATE(supplycost).WHERE(
        HAS(selected_part)
    )
    supplier_info = Suppliers.WHERE(nation_key == 19).CALCULATE(
        supplier_avg_price=AVG(supply_records.part.retail_price)
    )
    selected_suppliers = supplier_info.WHERE(HAS(selected_supply_records))
    global_info = TPCH.CALCULATE(global_avg_price=AVG(Parts.retail_price))
    return global_info.CALCULATE(n=COUNT(selected_suppliers))


def correl_16():
    # Correlated back reference example #16: hybrid tree order of operations.
    # Count how many european suppliers have the exact same percentile value
    # of account balance (relative to all other suppliers) as at least one
    # customer's percentile value of account balance relative to all other
    # customers. Percentile should be measured down to increments of 0.01%.
    # (This is a correlated SEMI-joins)
    selected_customers = nation.CALCULATE(rname=region.name).customers.WHERE(
        (PERCENTILE(by=(acctbal.ASC(), key.ASC()), n_buckets=10000) == tile)
        & (rname == "EUROPE")
    )
    supplier_info = Suppliers.CALCULATE(
        tile=PERCENTILE(by=(account_balance.ASC(), key.ASC()), n_buckets=10000)
    )
    selected_suppliers = supplier_info.WHERE(HAS(selected_customers))
    return TPCH.CALCULATE(n=COUNT(selected_suppliers))


def correl_17():
    # Correlated back reference example #17: hybrid tree order of operations.
    # An extremely roundabout way of getting each region_name-nation_name
    # pair as a string.
    # (This is a correlated singular/semi access)
    region_info = region.CALCULATE(fname=JOIN_STRINGS("-", LOWER(name), lname))
    nation_info = Nations.CALCULATE(lname=LOWER(name)).WHERE(HAS(region_info))
    return nation_info.CALCULATE(fullname=region_info.fname).ORDER_BY(fullname.ASC())


def correl_18():
    # Correlated back reference example #18: partition decorrelation edge case.
    # Count how many orders corresponded to at least half of the total price
    # spent by the ordering customer in a single day, but only if the customer
    # ordered multiple orders in on that day. Only considers orders made in
    # 1993.
    # (This is a correlated aggregation access)
    cust_date_groups = PARTITION(
        Orders.WHERE(YEAR(order_date) == 1993),
        name="o",
        by=(customer_key, order_date),
    )
    selected_groups = (
        cust_date_groups.WHERE(COUNT(o) > 1)
        .CALCULATE(
            total_price_sum=SUM(o.total_price),
        )
        .CALCULATE(n_above_avg=COUNT(o.WHERE(total_price >= 0.5 * total_price_sum)))
    )
    return TPCH.CALCULATE(n=SUM(selected_groups.n_above_avg))


def correl_19():
    # Correlated back reference example #19: cardinality edge case.
    # For every supplier, count how many customers in the same nation have a
    # higher account balance than that supplier. Pick the 5 suppliers with the
    # largest such count.
    # (This is a correlated aggregation access)
    super_cust = customers.WHERE(acctbal > account_balance)
    return (
        Suppliers.CALCULATE(account_balance, supplier_name=name)
        .nation.CALCULATE(supplier_name, n_super_cust=COUNT(super_cust))
        .TOP_K(5, n_super_cust.DESC())
    )


def correl_20():
    # Correlated back reference example #20: multiple ancestor uniqueness keys.
    # Count the instances where a nation's suppliers shipped a part to a
    # customer in the same nation, only counting instances where the order was
    # made in June of 1998.
    # (This is a correlated singular/semi access)
    is_domestic = nation.CALCULATE(domestic=name == source_nation_name).domestic
    selected_orders = Nations.CALCULATE(source_nation_name=name).customers.orders.WHERE(
        (YEAR(order_date) == 1998) & (MONTH(order_date) == 6)
    )
    instances = selected_orders.lines.supplier.WHERE(is_domestic)
    return TPCH.CALCULATE(n=COUNT(instances))


def correl_21():
    # Correlated back reference example #21: partition edge case.
    # Count how many part sizes have an above-average number of parts
    # of that size.
    # (This is a correlated aggregation access)
    sizes = PARTITION(Parts, name="p", by=size).CALCULATE(n_parts=COUNT(p))
    return TPCH.CALCULATE(avg_n_parts=AVG(sizes.n_parts)).CALCULATE(
        n_sizes=COUNT(sizes.WHERE(n_parts > avg_n_parts))
    )


def correl_22():
    # Correlated back reference example #22: partition edge case.
    # Finds the top 5 part sizes with the most container types
    # where the average retail price of parts of that container type
    # & part type is above the global average retail price.
    # (This is a correlated aggregation access)
    ct_combos = PARTITION(Parts, name="p", by=(container, part_type)).CALCULATE(
        avg_price=AVG(p.retail_price)
    )
    return (
        TPCH.CALCULATE(global_avg_price=AVG(Parts.retail_price))
        .PARTITION(
            ct_combos.WHERE(avg_price > global_avg_price),
            name="ct",
            by=container,
        )
        .CALCULATE(container, n_types=COUNT(ct))
        .TOP_K(5, (n_types.DESC(), container.ASC()))
    )


def correl_23():
    # Correlated back reference example #23: partition edge case.
    # Counts how many part sizes have an above-average number of combinations
    # of part types/containers.
    # (This is a correlated aggregation access)
    combos = PARTITION(Parts, name="p", by=(size, part_type, container))
    sizes = PARTITION(combos, name="c", by=size).CALCULATE(n_combos=COUNT(c))
    return TPCH.CALCULATE(avg_n_combo=AVG(sizes.n_combos)).CALCULATE(
        n_sizes=COUNT(sizes.WHERE(n_combos > avg_n_combo)),
    )
