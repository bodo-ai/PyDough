# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def bad_window_1():
    # Missing `by`
    return Orders(RANKING())


def bad_window_2():
    # Empty `by`
    return Orders(PERCENTILE(by=()))


def bad_window_3():
    # Non-collations in `by`
    return Orders(RANKING(by=order_key))


def bad_window_4():
    # Non-positive levels
    return Orders(RANKING(by=order_key.ASC(), levels=0))


def bad_window_5():
    # Non-integer levels
    return Orders(RANKING(by=order_key.ASC(), levels="hello"))


def bad_window_6():
    # Non-positive n_buckets
    return Orders(PERCENTILE(by=order_key.ASC(), n_buckets=-3))


def bad_window_7():
    # Non-integer n_buckets
    return Orders(PERCENTILE(by=order_key.ASC(), n_buckets=[1, 2, 3]))
