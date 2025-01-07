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
    return Nations(name, p=PERCENTILE(by=name.ASC(), n_buckets=5))


def percentile_customers_per_region():
    return (
        Regions.nations.customers(name)
        .WHERE((PERCENTILE(by=acctbal.ASC(), levels=1) == 95) & ENDSWITH(phone, "00"))
        .ORDER_BY(name.ASC())
    )
