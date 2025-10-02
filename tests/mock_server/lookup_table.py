"""
A lookup table for the mock server to return predefined responses based on
request column reference and predicate.
"""

LOOKUP_TABLE: dict = {
    # key: (column_reference, tuple(predicate))
    ("srv.db.tbl.col", ("EQUAL", 2, "__col__", 0)): {
        "index": 0,
        "result": "SUCCESS",
        "decision": {"strategy": "values", "reason": "mock"},
        "predicate_hash": "hash1",
        "encryption_mode": "clear",
        "materialization": {
            "type": "literal",
            "operator": "NOT_IN",
            "values": [
                "value1",
                "value2",
                "value3",
            ],
            "count": 3,
        },
    },
    (
        "srv.db.orders.order_date",
        ("BETWEEN", 3, "__col__", "2025-01-01", "2025-02-01"),
    ): {
        "index": 0,
        "result": "SUCCESS",
        "decision": {"strategy": "values", "reason": "mock"},
        "predicate_hash": "hash1",
        "encryption_mode": "clear",
        "materialization": {
            "type": "literal",
            "operator": "IN",
            "values": [
                "2025-01-01",
                "2025-01-02",
                "2025-01-03",
                "2025-01-04",
                "2025-01-05",
            ],
            "count": 5,
        },
    },
    ("srv.db.tbl.col", ("NOT_EQUAL", 2, "__col__", "LOWER", 1, "Smith")): {
        "index": 0,
        "result": "SUCCESS",
        "decision": {"strategy": "values", "reason": "mock"},
        "predicate_hash": "hash2",
        "encryption_mode": "clear",
        "materialization": {
            "type": "literal",
            "operator": "NOT_IN",
            "values": ["smith"],
            "count": 1,
        },
    },
    # booleans,
    ("srv.db.tbl.col", ("NOT_EQUAL", 2, "__col__", True)): {
        "index": 0,
        "result": "SUCCESS",
        "decision": {"strategy": "values", "reason": "mock"},
        "predicate_hash": "hash2",
        "encryption_mode": "clear",
        "materialization": {
            "type": "literal",
            "operator": "IN",
            "values": [False],
            "count": 1,
        },
    },
    # decimals (string format)
    ("srv.db.tbl.col", ("LT", 2, "__col__", "123.654445")): {
        "index": 0,
        "result": "SUCCESS",
        "decision": {"strategy": "values", "reason": "mock"},
        "predicate_hash": "hash2",
        "encryption_mode": "clear",
        "materialization": {
            "type": "literal",
            "operator": "IN",
            "values": ["123.121123", "123.654444", "123.654445"],
            "count": 3,
        },
    },
    # json embedded
    ("srv.db.tbl.col", ("NOT_EQUAL", 2, "__col__", '{"key": "value"}')): {
        "index": 0,
        "result": "SUCCESS",
        "decision": {"strategy": "values", "reason": "mock"},
        "predicate_hash": "hash2",
        "encryption_mode": "clear",
        "materialization": {
            "type": "literal",
            "operator": "NOT_IN",
            "values": ['{"key": "value"}'],
            "count": 1,
        },
    },
    # NULLs and Money
    (
        "srv.db.tbl.col",
        ("AND", 2, "NOT_EQUAL", 2, "__col__", None, "GT", 2, "__col__", "$45.00"),
    ): {
        "index": 0,
        "result": "SUCCESS",
        "decision": {"strategy": "values", "reason": "mock"},
        "predicate_hash": "hash2",
        "encryption_mode": "clear",
        "materialization": {
            "type": "literal",
            "operator": "NOT_IN",
            "values": [None, "$44.50", "$43.20", "$44.99"],
            "count": 4,
        },
    },
    # Result with Regex, Bytea, Backslash in really nested expression.
    (
        "srv.db.tbl.col",
        (
            "OR",
            2,
            "AND",
            2,
            "REGEXP",
            2,
            "__col__",
            "^[A-Z][a-z]+$",
            "NOT_EQUAL",
            2,
            "__col__",
            "SGVsbG9Xb3JsZA==",
            "EQUAL",
            2,
            "__col__",
            '"Hello World"',
        ),
    ): {
        "index": 0,
        "result": "SUCCESS",
        "decision": {"strategy": "values", "reason": "mock"},
        "predicate_hash": "hash2",
        "encryption_mode": "clear",
        "materialization": {
            "type": "literal",
            "operator": "IN",
            "values": ['"Hello"', "HelloWorld", "SGVsbG9Xb3JsZA=="],
            "count": 3,
        },
    },
}
