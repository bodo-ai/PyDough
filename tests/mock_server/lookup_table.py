"""
A lookup table for the mock server to return predefined responses based on
request column reference and predicate.
"""

LOOKUP_TABLE: dict = {
    # key: (column_reference, tuple(predicate))
    ("srv.db.tbl.col", ("EQUAL", 2, "__col__", 0)): {
        "index": 1,
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
        "index": 1,
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
    ("srv.db.tbl.col", ("NOT_EQUAL", 2, "__col__", 0)): {
        "index": 2,
        "result": "SUCCESS",
        "decision": {"strategy": "none", "reason": "no_match"},
        "predicate_hash": "hash2",
        "encryption_mode": "clear",
        "materialization": {
            "type": "literal",
            "operator": "NOT_IN",
            "values": [0],
            "count": 1,
        },
    },
}
