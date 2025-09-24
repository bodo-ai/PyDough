"""
A lookup table for the mock server to return predefined responses based on
request column reference and predicate.
"""

LOOKUP_TABLE: dict = {
    # key: (column_reference, tuple(predicate))
    ("srv.analytics.tbl.col", ("EQUAL", 2, "__col__", 1)): {
        "index": 1,
        "result": "SUCCESS",
        "decision": {"strategy": "values", "reason": "mock"},
        "predicate_hash": "hash1",
        "encryption_mode": "clear",
        "materialization": {
            "type": "literal",
            "operator": "IN",
            "values": ["value1", "value2", "value3", "value4", "value5"],
            "count": 1,
        },
    },
    ("srv.analytics.tbl.col", ("EQUAL", 2, "__col__", 0)): {
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
            "count": 1,
        },
    },
}
