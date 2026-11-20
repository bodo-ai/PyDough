"""TPC-H queries that exercise window scope across CROSS boundaries."""

# ruff: noqa
# mypy: ignore-errors
# The test harness supplies the PyDough names when it transforms each function.

import pydough


def cross_top():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    return accounts.orders.CALCULATE(
        c, oid, running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True)
    )


def cross_crossed():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.CALCULATE(
        c, oid, running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True)
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).WHERE(c == k).CALCULATE(c, oid, running)


def cross_reversed_crossed():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    lhs = accounts.orders.CALCULATE(
        c, oid, running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True)
    )
    rhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).WHERE(c == k).CALCULATE(c, oid, running)


def cross_top2():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    detail = accounts.orders.CALCULATE(c, oid, amount=total_price)
    return detail.CALCULATE(
        c, oid, running=RELSUM(amount, by=oid.ASC(), per="acct", cumulative=True)
    )


def cross_crossed2():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    detail = accounts.orders.CALCULATE(c, oid, amount=total_price)
    rhs = detail.CALCULATE(
        c, oid, running=RELSUM(amount, by=oid.ASC(), per="acct", cumulative=True)
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).WHERE(c == k).CALCULATE(c, oid, running)


def cross_top_neighbors():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    return accounts.orders.CALCULATE(
        c,
        oid,
        prev_oid=PREV(oid, by=oid.ASC(), per="acct"),
        next_oid=NEXT(oid, by=oid.ASC(), per="acct"),
    )


def cross_crossed_neighbors():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.CALCULATE(
        c,
        oid,
        prev_oid=PREV(oid, by=oid.ASC(), per="acct"),
        next_oid=NEXT(oid, by=oid.ASC(), per="acct"),
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).WHERE(c == k).CALCULATE(c, oid, prev_oid, next_oid)


def cross_unfiltered_neighbors():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.CALCULATE(
        c,
        oid,
        prev_oid=PREV(oid, by=oid.ASC(), per="acct"),
        next_oid=NEXT(oid, by=oid.ASC(), per="acct"),
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(c, oid, prev_oid, next_oid)


def cross_post_cross_window():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.CALCULATE(c, oid, amount=total_price)
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(c, oid, cross_total=RELSUM(amount))


def cross_filtered_top():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    return accounts.orders.WHERE(total_price > 100000).CALCULATE(
        c, oid, running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True)
    )


def cross_filtered_crossed():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.WHERE(total_price > 100000).CALCULATE(
        c, oid, running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True)
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).WHERE(c == k).CALCULATE(c, oid, running)


def cross_mixed_windows():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.CALCULATE(
        c,
        oid,
        amount=total_price,
        running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True),
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(c, oid, running, cross_total=RELSUM(amount))


def cross_top_multi():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    stage1 = accounts.orders.CALCULATE(
        c, oid, running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True)
    )
    return stage1.CALCULATE(
        c, oid, running, prev_oid=PREV(oid, by=oid.ASC(), per="acct")
    )


def cross_crossed_multi():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    stage1 = accounts.orders.CALCULATE(
        c, oid, running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True)
    )
    rhs = stage1.CALCULATE(
        c, oid, running, prev_oid=PREV(oid, by=oid.ASC(), per="acct")
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).WHERE(c == k).CALCULATE(c, oid, running, prev_oid)


def cross_nested_partition_windows():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.CALCULATE(
        c,
        oid,
        amount=total_price,
        running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True),
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    inner = lhs.CROSS(rhs).CALCULATE(
        k, c, oid, amount, running, inner_total=RELSUM(amount)
    )
    return (
        regions.WHERE(key <= 1)
        .CALCULATE(r=key)
        .CROSS(inner)
        .CALCULATE(r, k, c, oid, running, inner_total, outer_total=RELSUM(amount))
    )


def cross_without_windows():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.CALCULATE(c, oid, amount=total_price)
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(c, oid, amount)


def cross_correlated_partition_source():
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key, threshold=key * 100000)
    accounts = (
        orders.WHERE((customer_key <= 2) & (total_price > threshold))
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.CALCULATE(
        c, oid, running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True)
    )
    return lhs.CROSS(rhs).CALCULATE(k, c, oid, running)


def cross_rhs_window_no_per():
    rhs = orders.WHERE(customer_key <= 2).CALCULATE(
        oid=key, r=RANKING(by=key.ASC()), tot=RELSUM(total_price)
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(k, oid, r, tot)


def cross_rhs_window_per_nation():
    # Customers 5 and 13 share a nation in the full TPC-H fixture.
    rhs = nations.customers.orders.WHERE(customer_key <= 14).CALCULATE(
        oid=key, s=RELSUM(total_price, per="nations")
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(k, oid, s)


def cross_rhs_window_per_customer():
    rhs = customers.orders.WHERE(customer_key <= 2).CALCULATE(
        oid=key, s=RELSUM(total_price, per="customers")
    )
    lhs = nations.WHERE(key <= 1).CALCULATE(nk=key)
    return lhs.CROSS(rhs).CALCULATE(nk, oid, s)


def cross_rhs_partition_aggregate():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    groups = accounts.CALCULATE(n=COUNT(orders))
    rhs = groups.orders.CALCULATE(
        c,
        n,
        oid,
        running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True),
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).WHERE(c == k).CALCULATE(c, n, oid, running)


def cross_rhs_lhs_filter_after_window():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.CALCULATE(
        c, oid, running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True)
    ).WHERE(c == k)
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(c, oid, running)


def cross_rhs_lhs_filter_before_window():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.WHERE(c == k).CALCULATE(
        c, oid, running=RELSUM(total_price, by=oid.ASC(), per="acct", cumulative=True)
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(c, oid, running)


def cross_rhs_filter_before_global_window():
    rhs = orders.WHERE((customer_key <= 2) & (customer_key == k)).CALCULATE(
        oid=key, total=RELSUM(total_price)
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(k, oid, total)


def cross_rhs_filter_after_global_window():
    rhs = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(oid=key, total=RELSUM(total_price))
        .WHERE(customer_key == k)
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(k, oid, total)


def cross_rhs_top_k_before_window():
    rhs = (
        orders.WHERE(customer_key <= 2)
        .TOP_K(2, by=key.ASC())
        .CALCULATE(oid=key, tot=RELSUM(total_price))
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(k, oid, tot)


def cross_rhs_top_k_after_window():
    rhs = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(oid=key, tot=RELSUM(total_price))
        .TOP_K(2, by=oid.ASC())
    )
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(k, oid, tot)


def cross_nested_global_windows():
    rhs = orders.WHERE(customer_key <= 2).CALCULATE(oid=key, t=RELSUM(total_price))
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    inner = lhs.CROSS(rhs).CALCULATE(k, oid, t, inner_total=RELSUM(total_price))
    return (
        regions.WHERE(key <= 1)
        .CALCULATE(rk=key)
        .CROSS(inner)
        .CALCULATE(rk, k, oid, t, inner_total, outer_total=RELSUM(total_price))
    )


def cross_lhs_has_multiple_levels():
    lhs = (
        regions.WHERE(key <= 1)
        .CALCULATE(rk=key)
        .CROSS(customers.WHERE(key <= 2).CALCULATE(k=key))
    )
    rhs = orders.WHERE(customer_key <= 2).CALCULATE(oid=key, t=RELSUM(total_price))
    return lhs.CROSS(rhs).CALCULATE(rk, k, oid, t)


def cross_window_inside_aggregate():
    ranked = orders.WHERE(customer_key <= 2).CALCULATE(oid=key, t=RELSUM(total_price))
    rhs = CROSS(regions.WHERE(key <= 1).CALCULATE(rk=key)).CROSS(ranked)
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CALCULATE(k, total=SUM(rhs.t))


def cross_window_in_rhs_child_subtree():
    ranked = orders.CALCULATE(t=RELSUM(total_price))
    rhs = customers.WHERE(key <= 2).CALCULATE(ck=key, total=SUM(ranked.t))
    lhs = nations.WHERE(key <= 1).CALCULATE(nk=key)
    return lhs.CROSS(rhs).CALCULATE(nk, ck, total)


def cross_explicit_per_after_join():
    accounts = (
        orders.WHERE(customer_key <= 2)
        .CALCULATE(c=customer_key, oid=key)
        .PARTITION(name="acct", by=c)
    )
    rhs = accounts.orders.CALCULATE(c, oid, amount=total_price)
    lhs = customers.WHERE(key <= 2).CALCULATE(k=key)
    return lhs.CROSS(rhs).CALCULATE(k, c, oid, total=RELSUM(amount, per="acct"))


def cross_rhs_per_lhs_ancestor():
    # `per` names an ancestor on the LHS side of the CROSS. The explicit
    # request wins: the window spans both LHS nations of the region.
    rhs = orders.WHERE(customer_key <= 2).CALCULATE(
        oid=key,
        n=RELCOUNT(key, per="regions"),
        total=RELSUM(total_price, per="regions"),
    )
    lhs = (
        regions.WHERE(key == 1)
        .CALCULATE(rk=key)
        .nations.WHERE(key <= 2)
        .CALCULATE(nk=key)
    )
    return lhs.CROSS(rhs).CALCULATE(rk, nk, oid, n, total)


def cross_nested_per_inner_lhs():
    # `per` names the LHS of the inner CROSS, which lies inside the RHS of
    # the outer CROSS. The inner CROSS adds no implicit key, the outer one
    # still adds the region.
    ranked = orders.WHERE(customer_key <= 2).CALCULATE(
        oid=key, t=RELSUM(total_price, per="customers")
    )
    inner = customers.WHERE(key <= 2).CALCULATE(k=key).CROSS(ranked)
    lhs = regions.WHERE(key <= 1).CALCULATE(rk=key)
    return lhs.CROSS(inner).CALCULATE(rk, k, oid, t)


def cross_range_lhs():
    # A generated collection as the LHS still identifies each LHS row.
    rhs = orders.WHERE(customer_key <= 2).CALCULATE(
        oid=key, r=RANKING(by=key.ASC()), tot=RELSUM(total_price)
    )
    lhs = pydough.range_collection("rng", "i", 0, 2).CALCULATE(i)
    return lhs.CROSS(rhs).CALCULATE(i, oid, r, tot)
