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
    return Regions(
        name, n_prefix_nations=COUNT(nations.WHERE(name[:1] == BACK(1).name[:1]))
    ).ORDER_BY(name.ASC())


def correl_2():
    # Correlated back reference example #2: simple 2-step correlated reference
    # For each region's nations, count how many customers have a comment
    # starting with the same letter as the region. Exclude regions that start
    # with the letter a. This is a true correlated join doing an aggregated
    # access without requiring the RHS be present.
    selected_custs = customers.WHERE(comment[:1] == LOWER(BACK(2).name[:1]))
    return (
        Regions.WHERE(~STARTSWITH(name, "A"))
        .nations(
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
    selected_custs = customers.WHERE(comment[:2] == LOWER(BACK(2).name[:2]))
    return Regions(name, n_nations=COUNT(nations.WHERE(HAS(selected_custs)))).ORDER_BY(
        name.ASC()
    )


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
