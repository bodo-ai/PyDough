"""
A lookup table for the mock server to return predefined responses based on
request column reference and predicate.
"""

LOOKUP_TABLE: dict = {
    # key: (column_reference, tuple(predicate))
    ("srv.db.tbl.col", ("EQUAL", 2, "__col__", 0)): {
        "type": "literal",
        "operator": "NOT_IN",
        "values": [
            "value1",
            "value2",
            "value3",
        ],
        "count": 3,
    },
    (
        "srv.db.orders.order_date",
        ("BETWEEN", 3, "__col__", "2025-01-01", "2025-02-01"),
    ): {
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
    ("srv.db.tbl.col", ("NOT_EQUAL", 2, "__col__", "LOWER", 1, "Smith")): {
        "type": "literal",
        "operator": "NOT_IN",
        "values": ["smith"],
        "count": 1,
    },
    # booleans,
    ("srv.db.tbl.col", ("NOT_EQUAL", 2, "__col__", True)): {
        "type": "literal",
        "operator": "IN",
        "values": [False],
        "count": 1,
    },
    # decimals (string format)
    ("srv.db.tbl.col", ("LT", 2, "__col__", "123.654445")): {
        "type": "literal",
        "operator": "IN",
        "values": ["123.121123", "123.654444", "123.654445"],
        "count": 3,
    },
    # json embedded
    ("srv.db.tbl.col", ("NOT_EQUAL", 2, "__col__", '{"key": "value"}')): {
        "type": "literal",
        "operator": "NOT_IN",
        "values": ['{"key": "value"}'],
        "count": 1,
    },
    # NULLs and Money
    (
        "srv.db.tbl.col",
        ("AND", 2, "NOT_EQUAL", 2, "__col__", None, "GT", 2, "__col__", "$45.00"),
    ): {
        "type": "literal",
        "operator": "NOT_IN",
        "values": [None, "$44.50", "$43.20", "$44.99"],
        "count": 4,
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
        "type": "literal",
        "operator": "IN",
        "values": ['"Hello"', "HelloWorld", "SGVsbG9Xb3JsZA=="],
        "count": 3,
    },
    # CRYPTBANK hardcoded responses
    ("CRBNK.CUSTOMERS.c_lname", ("EQUAL", 2, "__col__", "lee")): {
        "type": "literal",
        "operator": "IN",
        "values": ["LEE"],
        "count": 1,
    },
    ("CRBNK.CUSTOMERS.c_birthday", ("BETWEEN", 3, 1980, "YEAR", 1, "__col__", 1985)): {
        "type": "literal",
        "operator": "IN",
        "values": [
            "1980-01-18",
            "1981-07-21",
            "1981-11-15",
            "1982-11-07",
            "1983-12-27",
        ],
        "count": 5,
    },
    ("CRBNK.TRANSACTIONS.t_amount", ("GT", 2, "__col__", 9000.0)): {
        "type": "literal",
        "operator": "IN",
        "values": [
            -8934.44,
            -8881.98,
            -8736.83,
            -8717.7,
            -8648.33,
            -8639.5,
            -8620.48,
            -8593.09,
            -8553.43,
            -8527.34,
            -8484.61,
            -8480.79,
            -8472.7,
            -8457.49,
            -8366.52,
            -8361.27,
            -8352.72,
            -8308.42,
            -8254.69,
            -8077.89,
            -8067.8,
        ],
        "count": 21,
    },
    (
        "CRBNK.TRANSACTIONS.t_ts",
        (
            "AND",
            2,
            "EQUAL",
            2,
            "MONTH",
            1,
            "__col__",
            6,
            "EQUAL",
            2,
            "YEAR",
            1,
            "__col__",
            2022,
        ),
    ): {
        "type": "literal",
        "operator": "IN",
        "values": [
            "2022-06-03 05:08:58",
            "2022-06-12 00:24:06",
            "2022-06-13 05:50:39",
            "2022-06-14 19:08:57",
            "2022-06-16 03:15:13",
            "2022-06-18 03:37:49",
            "2022-06-27 06:08:04",
            "2022-06-28 15:35:47",
            "2022-06-29 05:40:38",
            "2022-06-29 19:53:42",
        ],
        "count": 10,
    },
    (
        "CRBNK.ACCOUNTS.a_type",
        (
            "OR",
            2,
            "EQUAL",
            2,
            "__col__",
            "retirement",
            "EQUAL",
            2,
            "__col__",
            "savings",
        ),
    ): {
        "type": "literal",
        "operator": "IN",
        "values": ["avingss", "etirementr"],
        "count": 2,
    },
    ("CRBNK.CUSTOMERS.c_phone", ("ENDSWITH", 2, "__col__", "5")): {
        "type": "literal",
        "operator": "IN",
        "values": ["555-091-2345", "555-901-2345"],
        "count": 2,
    },
    (
        "CRBNK.CUSTOMERS.c_fname",
        ("OR", 2, "ENDSWITH", 2, "__col__", "a", "ENDSWITH", 2, "__col__", "e"),
    ): {
        "type": "literal",
        "operator": "IN",
        "values": ["ALICE", "GRACE", "LUKE", "MARIA", "OLIVIA", "QUEENIE", "SOPHIA"],
        "count": 8,
    },
    ("CRBNK.CUSTOMERS.c_fname", ("ENDSWITH", 2, "__col__", "s")): {
        "type": "literal",
        "operator": "IN",
        "values": ["JAMES", "NICHOLAS", "THOMAS"],
        "count": 3,
    },
    ("CRBNK.CUSTOMERS.c_lname", ("NOT_EQUAL", 2, "__col__", "lopez")): {
        "type": "literal",
        "operator": "NOT_IN",
        "values": ["LOPEZ"],
        "count": 1,
    },
}

"""
DONE:
- agg_01
- analysis_04
- filter_count_27
- filter_count_28

select c_birthday
from customers
where STRFTIME('%Y', DATE(c_birthday, '+472 days')) IN ('1980', '1981', '1982', '1983', '1984', '1985')
ORDER BY 1
;


SELECT c_fname
FROM customers
WHERE c_fname LIKE '%A' OR c_fname LIKE '%E'
ORDER BY 1;

SELECT c_fname
FROM customers
WHERE c_fname LIKE '%S'
ORDER BY 1;
"""
