"""
Adds tests for the ColumnPruner class, which is used to determine
how to prune columns from relational expressions.
"""

import pytest
from test_utils import (
    build_simple_scan,
    make_relational_column_reference,
    make_relational_literal,
    make_relational_ordering,
)

from pydough.pydough_operators import (
    ADD,
    EQU,
    SUM,
)
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnPruner,
    Filter,
    Join,
    JoinType,
    Limit,
    Project,
    RelationalRoot,
    Scan,
)
from pydough.types import BooleanType, Int64Type, UnknownType


@pytest.fixture(scope="module")
def column_pruner() -> ColumnPruner:
    return ColumnPruner()


@pytest.mark.parametrize(
    "input, output",
    [
        pytest.param(
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=build_simple_scan(),
            ),
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Scan(
                    table_name="table",
                    columns={"b": make_relational_column_reference("b")},
                ),
            ),
            id="scan_pruning",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("a", make_relational_column_reference("a"))],
                input=Filter(
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("c"),
                            make_relational_literal(1, Int64Type()),
                        ],
                    ),
                    input=Scan(
                        table_name="table",
                        columns={
                            "a": make_relational_column_reference("a"),
                            "b": make_relational_column_reference("b"),
                            "c": make_relational_column_reference("c"),
                        },
                    ),
                ),
            ),
            RelationalRoot(
                ordered_columns=[("a", make_relational_column_reference("a"))],
                input=Filter(
                    columns={"a": make_relational_column_reference("a")},
                    condition=CallExpression(
                        EQU,
                        BooleanType(),
                        [
                            make_relational_column_reference("c"),
                            make_relational_literal(1, Int64Type()),
                        ],
                    ),
                    input=Scan(
                        table_name="table",
                        columns={
                            "a": make_relational_column_reference("a"),
                            "c": make_relational_column_reference("c"),
                        },
                    ),
                ),
            ),
            id="filter_pruning",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("a", make_relational_column_reference("a"))],
                input=Project(
                    columns={
                        "a": CallExpression(
                            ADD,
                            UnknownType(),
                            [
                                make_relational_column_reference("a"),
                                make_relational_literal(1, Int64Type()),
                            ],
                        ),
                        "b": make_relational_column_reference("b"),
                    },
                    input=Scan(
                        table_name="table",
                        columns={
                            "a": make_relational_column_reference("a"),
                            "b": make_relational_column_reference("b"),
                            "c": make_relational_column_reference("c"),
                        },
                    ),
                ),
            ),
            RelationalRoot(
                ordered_columns=[("a", make_relational_column_reference("a"))],
                input=Project(
                    columns={
                        "a": CallExpression(
                            ADD,
                            UnknownType(),
                            [
                                make_relational_column_reference("a"),
                                make_relational_literal(1, Int64Type()),
                            ],
                        )
                    },
                    input=Scan(
                        table_name="table",
                        columns={"a": make_relational_column_reference("a")},
                    ),
                ),
            ),
            id="projection_pruning",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Project(
                    columns={
                        "a": CallExpression(
                            ADD,
                            UnknownType(),
                            [
                                make_relational_column_reference("a"),
                                make_relational_literal(1, Int64Type()),
                            ],
                        ),
                        "b": make_relational_column_reference("b"),
                    },
                    input=Scan(
                        table_name="table",
                        columns={
                            "a": make_relational_column_reference("a"),
                            "b": make_relational_column_reference("b"),
                            "c": make_relational_column_reference("c"),
                        },
                    ),
                ),
            ),
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Scan(
                    table_name="table",
                    columns={"b": make_relational_column_reference("b")},
                ),
            ),
            id="identity_projection_pruning",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("a", make_relational_column_reference("a"))],
                input=Limit(
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                    limit=make_relational_literal(1, Int64Type()),
                    orderings=[
                        make_relational_ordering(
                            make_relational_column_reference("b"), True
                        )
                    ],
                    input=Scan(
                        table_name="table",
                        columns={
                            "a": make_relational_column_reference("a"),
                            "b": make_relational_column_reference("b"),
                            "c": make_relational_column_reference("c"),
                        },
                    ),
                ),
                orderings=[
                    make_relational_ordering(
                        make_relational_column_reference("b"), True
                    )
                ],
            ),
            RelationalRoot(
                ordered_columns=[("a", make_relational_column_reference("a"))],
                input=Limit(
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                    },
                    limit=make_relational_literal(1, Int64Type()),
                    orderings=[
                        make_relational_ordering(
                            make_relational_column_reference("b"), True
                        )
                    ],
                    input=Scan(
                        table_name="table",
                        columns={
                            "a": make_relational_column_reference("a"),
                            "b": make_relational_column_reference("b"),
                        },
                    ),
                ),
                orderings=[
                    make_relational_ordering(
                        make_relational_column_reference("b"), True
                    )
                ],
            ),
            id="limit_pruning",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("a", make_relational_column_reference("a"))],
                input=Aggregate(
                    keys={"a": make_relational_column_reference("a")},
                    aggregations={
                        "b": CallExpression(
                            SUM, Int64Type(), [make_relational_column_reference("b")]
                        )
                    },
                    input=Scan(
                        table_name="table",
                        columns={
                            "a": make_relational_column_reference("a"),
                            "b": make_relational_column_reference("b"),
                            "c": make_relational_column_reference("c"),
                        },
                    ),
                ),
            ),
            RelationalRoot(
                ordered_columns=[("a", make_relational_column_reference("a"))],
                input=Aggregate(
                    keys={"a": make_relational_column_reference("a")},
                    aggregations={},
                    input=Scan(
                        table_name="table",
                        columns={"a": make_relational_column_reference("a")},
                    ),
                ),
            ),
            id="aggregate_pruning",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Aggregate(
                    keys={"a": make_relational_column_reference("a")},
                    aggregations={
                        "b": CallExpression(
                            SUM, Int64Type(), [make_relational_column_reference("b")]
                        )
                    },
                    input=Scan(
                        table_name="table",
                        columns={
                            "a": make_relational_column_reference("a"),
                            "b": make_relational_column_reference("b"),
                            "c": make_relational_column_reference("c"),
                        },
                    ),
                ),
            ),
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Aggregate(
                    keys={"a": make_relational_column_reference("a")},
                    aggregations={
                        "b": CallExpression(
                            SUM, Int64Type(), [make_relational_column_reference("b")]
                        )
                    },
                    input=Scan(
                        table_name="table",
                        columns={
                            "a": make_relational_column_reference("a"),
                            "b": make_relational_column_reference("b"),
                        },
                    ),
                ),
            ),
            id="attempted_key_pruning",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Join(
                    columns={
                        "a": make_relational_column_reference("a", input_name="t0"),
                        "b": make_relational_column_reference("b", input_name="t1"),
                    },
                    join_types=[JoinType.INNER],
                    conditions=[
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("c", input_name="t1"),
                                make_relational_column_reference("c", input_name="t0"),
                            ],
                        )
                    ],
                    inputs=[
                        Scan(
                            table_name="table",
                            columns={
                                "a": make_relational_column_reference("a"),
                                "b": make_relational_column_reference("b"),
                                "c": make_relational_column_reference("c"),
                            },
                        ),
                        Scan(
                            table_name="table",
                            columns={
                                "a": make_relational_column_reference("a"),
                                "b": make_relational_column_reference("b"),
                                "c": make_relational_column_reference("c"),
                            },
                        ),
                    ],
                ),
            ),
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Join(
                    columns={
                        "b": make_relational_column_reference("b", input_name="t1")
                    },
                    join_types=[JoinType.INNER],
                    conditions=[
                        CallExpression(
                            EQU,
                            BooleanType(),
                            [
                                make_relational_column_reference("c", input_name="t1"),
                                make_relational_column_reference("c", input_name="t0"),
                            ],
                        )
                    ],
                    inputs=[
                        Scan(
                            table_name="table",
                            columns={"c": make_relational_column_reference("c")},
                        ),
                        Scan(
                            table_name="table",
                            columns={
                                "b": make_relational_column_reference("b"),
                                "c": make_relational_column_reference("c"),
                            },
                        ),
                    ],
                ),
            ),
            id="join_pruning",
        ),
        pytest.param(
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Scan(
                    table_name="table",
                    columns={
                        "a": make_relational_column_reference("a"),
                        "b": make_relational_column_reference("b"),
                        "c": make_relational_column_reference("c"),
                    },
                ),
                orderings=[
                    make_relational_ordering(
                        make_relational_column_reference("c"), True
                    )
                ],
            ),
            RelationalRoot(
                ordered_columns=[("b", make_relational_column_reference("b"))],
                input=Scan(
                    table_name="table",
                    columns={
                        "b": make_relational_column_reference("b"),
                        "c": make_relational_column_reference("c"),
                    },
                ),
                orderings=[
                    make_relational_ordering(
                        make_relational_column_reference("c"), True
                    )
                ],
            ),
            id="pruned_ordering",
        ),
    ],
)
def test_pruning_columns(
    column_pruner: ColumnPruner, input: RelationalRoot, output: RelationalRoot
) -> None:
    pruned = column_pruner.prune_unused_columns(input)
    assert pruned == output
