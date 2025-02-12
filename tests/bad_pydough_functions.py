# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def bad_bool_1():
    # Using `or`
    return Customer(
        is_eurasian=(nation.region.name == "EUROPE") or (nation.region.name == "ASIA")
    )


def bad_bool_2():
    # Using `and`
    return Parts.WHERE((size == 38) and CONTAINS(name, "green"))


def bad_bool_3():
    # Using `not`
    return Parts.WHERE(not STARTSWITH(size, "LG"))


def bad_window_1():
    # Missing `by`
    return Orders.CALCULATE(RANKING())


def bad_window_2():
    # Empty `by`
    return Orders.CALCULATE(PERCENTILE(by=()))


def bad_window_3():
    # Non-collations in `by`
    return Orders.CALCULATE(RANKING(by=order_key))


def bad_window_4():
    # Non-positive levels
    return Orders.CALCULATE(RANKING(by=order_key.ASC(), levels=0))


def bad_window_5():
    # Non-integer levels
    return Orders.CALCULATE(RANKING(by=order_key.ASC(), levels="hello"))


def bad_window_6():
    # Non-positive n_buckets
    return Orders.CALCULATE(PERCENTILE(by=order_key.ASC(), n_buckets=-3))


def bad_window_7():
    # Non-integer n_buckets
    return Orders.CALCULATE(PERCENTILE(by=order_key.ASC(), n_buckets=[1, 2, 3]))


def bad_slice_1():
    # Unsupported slicing: negative stop
    return Customers.CALCULATE(name[:-1])


def bad_slice_2():
    # Unsupported slicing: negative start
    return Customers.CALCULATE(name[-5:])


def bad_slice_3():
    # Unsupported slicing: skipping
    return Customers.CALCULATE(name[1:10:2])


def bad_slice_4():
    # Unsupported slicing: reversed
    return Customers.CALCULATE(name[::-1])
