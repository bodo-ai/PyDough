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
