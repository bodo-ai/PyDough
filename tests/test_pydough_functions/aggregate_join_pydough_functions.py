"""PyDough functions for aggregate-join transposition regression tests (#653)."""

# ruff: noqa
# mypy: ignore-errors
# PyDough names are resolved by the test utilities when the functions run.


def aggregate_join_two_sided_key():
    """
    Test a calculated grouping key referencing both inputs of a join.
    """
    return (
        customers.CALCULATE(
            label=JOIN_STRINGS(" - ", market_segment, nation.name)
        )
        .PARTITION(name="groups", by=label)
        .CALCULATE(label, n=COUNT(customers))
        .ORDER_BY(label.ASC())
    )


def aggregate_join_nested_two_sided_key():
    """
    Test a grouping key that is single-sided at the outer join but
    references both inputs of the inner join.
    """
    return (
        customers.CALCULATE(
            label=JOIN_STRINGS(" - ", nation.name, nation.region.name)
        )
        .PARTITION(name="groups", by=label)
        .CALCULATE(label, n_customers=COUNT(customers))
        .ORDER_BY(label.ASC())
    )


def aggregate_join_separate_keys():
    """
    Test separate grouping keys that individually reference only one
    join input, even though together they reference both inputs.
    """
    return (
        customers.CALCULATE(nation_name=nation.name)
        .PARTITION(name="groups", by=(market_segment, nation_name))
        .CALCULATE(market_segment, nation_name, n=COUNT(customers))
        .ORDER_BY(market_segment.ASC(), nation_name.ASC())
    )
