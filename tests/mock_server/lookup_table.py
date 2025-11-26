"""
A lookup table for the mock server to return predefined responses based on
request column reference and predicate.
"""

LOOKUP_TABLE: dict[tuple[str, tuple], tuple[str, list]] = {
    # key: (column_reference, tuple(predicate))
    # value: (response_case, payload)
    ("srv.db.tbl.col", ("EQUAL", 2, "__col__", 0)): (
        "NOT_IN",
        [
            "value1",
            "value2",
            "value3",
        ],
    ),
    (
        "srv.db.orders.order_date",
        ("BETWEEN", 3, "__col__", "2025-01-01", "2025-02-01"),
    ): (
        "IN",
        [
            "2025-01-01",
            "2025-01-02",
            "2025-01-03",
            "2025-01-04",
            "2025-01-05",
        ],
    ),
    ("srv.db.tbl.col", ("NOT_EQUAL", 2, "__col__", "LOWER", 1, "Smith")): (
        "NOT_IN",
        ["smith"],
    ),
    # booleans,
    ("srv.db.tbl.col", ("NOT_EQUAL", 2, "__col__", True)): (
        "IN",
        [False],
    ),
    # decimals (string format)
    ("srv.db.tbl.col", ("LT", 2, "__col__", "123.654445")): (
        "IN",
        ["123.121123", "123.654444", "123.654445"],
    ),
    # json embedded
    ("srv.db.tbl.col", ("NOT_EQUAL", 2, "__col__", '("key": "value")')): (
        "NOT_IN",
        ['("key": "value")'],
    ),
    # NULLs and Money
    (
        "srv.db.tbl.col",
        ("AND", 2, "NOT_EQUAL", 2, "__col__", None, "GT", 2, "__col__", "$45.00"),
    ): (
        "NOT_IN",
        [None, "$44.50", "$43.20", "$44.99"],
    ),
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
    ): (
        "IN",
        ['"Hello"', "HelloWorld", "SGVsbG9Xb3JsZA=="],
    ),
    # CRYPTBANK hardcoded responses
    ("srv/CRBNK/CUSTOMERS/c_lname", ("EQUAL", 2, "__col__", "lee")): (
        "IN",
        ["LEE"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_birthday",
        ("BETWEEN", 3, 1980, "YEAR", 1, "__col__", 1985),
    ): (
        "IN",
        [
            "1980-01-18",
            "1981-07-21",
            "1981-11-15",
            "1982-11-07",
            "1983-12-27",
        ],
    ),
    ("srv/CRBNK/TRANSACTIONS/t_amount", ("GT", 2, "__col__", 9000.0)): (
        "IN",
        [
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
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
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
    ): (
        "IN",
        [
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
    ),
    (
        "srv/CRBNK/ACCOUNTS/a_type",
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
    ): (
        "IN",
        ["avingss", "etirementr"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_phone", ("ENDSWITH", 2, "__col__", "5")): (
        "IN",
        ["555-091-2345", "555-901-2345"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        ("OR", 2, "ENDSWITH", 2, "__col__", "a", "ENDSWITH", 2, "__col__", "e"),
    ): (
        "IN",
        ["ALICE", "GRACE", "LUKE", "MARIA", "OLIVIA", "QUEENIE", "SOPHIA"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_fname", ("ENDSWITH", 2, "__col__", "s")): (
        "IN",
        ["JAMES", "NICHOLAS", "THOMAS"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_lname", ("NOT_EQUAL", 2, "__col__", "lopez")): (
        "NOT_IN",
        ["LOPEZ"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_lname", ("NOT_EQUAL", 2, "__col__", "lee")): (
        "NOT_IN",
        ["LEE"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_lname",
        ("IN", 4, "__col__", "lee", "smith", "rodriguez"),
    ): (
        "IN",
        ["LEE", "SMITH", "RODRIGUEZ"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_lname",
        ("NOT", 1, "IN", 4, "__col__", "lee", "smith", "rodriguez"),
    ): (
        "NOT_IN",
        ["LEE", "SMITH", "RODRIGUEZ"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_phone", ("STARTSWITH", 2, "__col__", "555-8")): (
        "IN",
        ["555-809-1234", "555-870-9123"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_email", ("ENDSWITH", 2, "__col__", "gmail.com")): (
        "IN",
        [
            "livia.a22@gmail.como",
            "ob.smith77@gmail.comb",
            "ob_moore78@gmail.comr",
            "opez.luke99@gmail.coml",
        ],
    ),
    ("srv/CRBNK/CUSTOMERS/c_birthday", ("EQUAL", 2, "YEAR", 1, "__col__", 1978)): (
        "IN",
        ["1976-10-27", "1976-12-02"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_birthday", ("EQUAL", 2, "__col__", "1985-04-12")): (
        "IN",
        ["1983-12-27"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_fname", ("ENDSWITH", 2, "__col__", "e")): (
        "IN",
        ["ALICE", "GRACE", "LUKE", "QUEENIE"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_lname", ("ENDSWITH", 2, "__col__", "e")): (
        "IN",
        ["LEE", "MOORE"],
    ),
    (
        "srv/CRBNK/ACCOUNTS/a_type",
        (
            "AND",
            2,
            "NOT_EQUAL",
            2,
            "__col__",
            "checking",
            "NOT_EQUAL",
            2,
            "__col__",
            "savings",
        ),
    ): (
        "NOT_IN",
        ["avingss", "heckingc"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_birthday", ("NOT_EQUAL", 2, "__col__", "1991-11-15")): (
        "NOT_IN",
        ["1990-07-31"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_birthday", ("LTE", 2, "__col__", "1991-11-15")): (
        "NOT_IN",
        ["1991-03-13", "1992-05-06", "1993-01-01", "1994-06-15"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_birthday", ("GT", 2, "__col__", "1991-11-15")): (
        "IN",
        ["1991-03-13", "1992-05-06", "1993-01-01", "1994-06-15"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_birthday", ("LT", 2, "__col__", "1991-11-15")): (
        "NOT_IN",
        [
            "1990-07-31",
            "1991-03-13",
            "1992-05-06",
            "1993-01-01",
            "1994-06-15",
        ],
    ),
    ("srv/CRBNK/CUSTOMERS/c_birthday", ("GTE", 2, "__col__", "1991-11-15")): (
        "IN",
        [
            "1990-07-31",
            "1991-03-13",
            "1992-05-06",
            "1993-01-01",
            "1994-06-15",
        ],
    ),
    ("srv/CRBNK/TRANSACTIONS/t_amount", ("LT", 2, "__col__", 0)): (
        "IN",
        [],
    ),
    ("srv/CRBNK/TRANSACTIONS/t_amount", ("GT", 2, "__col__", 0)): (
        "NOT_IN",
        [],
    ),
    ("srv/CRBNK/CUSTOMERS/c_birthday", ("LTE", 2, "__col__", "1925-01-01")): (
        "IN",
        [],
    ),
    ("srv/CRBNK/CUSTOMERS/c_phone", ("EQUAL", 2, "__col__", "555-123-456")): (
        "IN",
        [],
    ),
    ("srv/CRBNK/ACCOUNTS/a_open_ts", ("EQUAL", 2, "YEAR", 1, "__col__", 2021)): (
        "IN",
        [
            "2017-02-11 10:59:51",
            "2017-06-15 12:41:51",
            "2017-07-07 14:26:51",
            "2017-07-09 12:21:51",
            "2017-09-15 11:26:51",
            "2018-01-02 12:26:51",
        ],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_birthday",
        (
            "AND",
            2,
            "IN",
            7,
            "ADD",
            2,
            "MONTH",
            1,
            "__col__",
            1,
            2,
            4,
            6,
            8,
            10,
            12,
            "IN",
            11,
            "SUB",
            2,
            "YEAR",
            1,
            "__col__",
            2,
            1975,
            1977,
            1979,
            1981,
            1983,
            1985,
            1987,
            1989,
            1991,
            1993,
        ),
    ): (
        "IN",
        ["1980-01-18", "1981-11-15", "1990-07-31", "1994-06-15"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_birthday",
        ("IN", 5, "__col__", "1991-11-15", "1978-02-11", "2005-03-14", "1985-04-12"),
    ): (
        "IN",
        ["1990-07-31", "1976-10-27", "1983-12-27"],
    ),
    (
        "srv/CRBNK/ACCOUNTS/a_open_ts",
        ("BETWEEN", 3, "2020-03-28 09:20:00", "__col__", "2020-09-20 08:30:00"),
    ): (
        "IN",
        [
            "2016-04-29 11:46:51",
            "2016-06-10 12:56:51",
            "2016-07-20 15:46:51",
            "2016-08-22 10:41:51",
            "2016-09-03 12:01:51",
        ],
    ),
    ("srv/CRBNK/CUSTOMERS/c_email", ("CONTAINS", 2, "__col__", "mail")): (
        "NOT_IN",
        [
            "homasl@outlook.comt",
            "ueenie.t@outlook.netq",
            ".hernandez@icloud.comk",
            "martinez94@outlook.orgj",
            "sa.rodriguez@zoho.comi",
            ".brown88@yahoo.comd",
            ".lee@outlook.comc",
            "lice_j@example.orga",
        ],
    ),
    ("srv/CRBNK/CUSTOMERS/c_email", ("LIKE", 2, "__col__", "%.%@%mail%")): (
        "IN",
        [
            "ophia.jackson@mail.orgs",
            "livia.a22@gmail.como",
            ".gonzalez@ymail.comm",
            "opez.luke99@gmail.coml",
            "enry.g@fastmail.comh",
            "rank.k@protonmail.comf",
            "mily.jones@mail.come",
            "ob.smith77@gmail.comb",
        ],
    ),
    ("srv/CRBNK/ACCOUNTS/a_open_ts", ("IN", 4, "MONTH", 1, "__col__", 1, 2, 3)): (
        "IN",
        [
            "2013-04-22 11:37:51",
            "2017-02-11 10:59:51",
            "2011-04-30 15:16:51",
            "2016-03-23 12:41:51",
            "2013-02-15 12:46:51",
            "2018-03-15 10:36:51",
            "2014-04-07 14:21:51",
            "2015-02-08 17:26:51",
            "2016-04-29 11:46:51",
            "2012-03-22 12:16:51",
            "2015-04-06 13:46:51",
        ],
    ),
    (
        "srv/CRBNK/ACCOUNTS/a_open_ts",
        ("EQUAL", 2, "QUARTER", 1, "__col__", "DAY", 1, "__col__"),
    ): (
        "IN",
        ["2015-05-04 18:01:51"],
    ),
    (
        "srv/CRBNK/ACCOUNTS/a_open_ts",
        (
            "AND",
            2,
            "LT",
            2,
            "HOUR",
            1,
            "__col__",
            10,
            "LT",
            2,
            "MINUTE",
            1,
            "__col__",
            20,
        ),
    ): (
        "IN",
        [
            "2013-04-22 11:37:51",
            "2017-09-15 11:26:51",
            "2018-03-15 10:36:51",
            "2014-05-23 11:31:51",
            "2016-08-22 10:41:51",
            "2014-08-15 11:31:51",
        ],
    ),
    ("srv/CRBNK/TRANSACTIONS/t_ts", ("EQUAL", 2, "SECOND", 1, "__col__", 23)): (
        "IN",
        [
            "2020-11-11 09:03:02",
            "2023-09-15 09:00:02",
            "2024-07-21 23:24:02",
        ],
    ),
    (
        "srv/CRBNK/ACCOUNTS/a_balance",
        ("BETWEEN", 3, 200, "ABS", 1, "SUB", 2, "__col__", 7250, 600),
    ): (
        "IN",
        [
            46240000.0,
            57760000.0,
        ],
    ),
    (
        "srv/CRBNK/ACCOUNTS/a_open_ts",
        (
            "EQUAL",
            2,
            "GREATEST",
            3,
            "HOUR",
            1,
            "__col__",
            "MINUTE",
            1,
            "__col__",
            "SECOND",
            1,
            "__col__",
            10,
        ),
    ): (
        "IN",
        [
            "2018-03-15 10:36:51",
            "2018-01-02 12:26:51",
        ],
    ),
    (
        "srv/CRBNK/ACCOUNTS/a_open_ts",
        ("EQUAL", 2, "LEAST", 2, "HOUR", 1, "__col__", "MINUTE", 1, "__col__", 15),
    ): (
        "IN",
        [
            "2015-08-10 18:11:51",
            "2015-05-04 18:01:51",
            "2015-10-19 18:11:51",
            "2014-10-03 17:41:51",
        ],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_phone",
        ("CONTAINS", 2, "CONCAT", 2, "1-", "__col__", "1-5"),
    ): (
        "NOT_IN",
        [],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_phone",
        ("CONTAINS", 2, "CONCAT", 3, "1", "-", "__col__", "1-5"),
    ): (
        "NOT_IN",
        [],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_phone",
        ("CONTAINS", 2, "CONCAT", 5, "1", "-", "__col__", "-", "1", "5-1"),
    ): (
        "IN",
        [
            "555-112-3456",
            "555-901-2345",
            "555-091-2345",
            "555-123-4567",
        ],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_birthday",
        ("IN", 3, "COALESCE", 2, "YEAR", 1, "__col__", 1990, 1990, 1991),
    ): (
        "IN",
        [
            "1990-07-31",
            "1989-04-07",
            None,
        ],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_birthday",
        ("IN", 3, "COALESCE", 2, "YEAR", 1, "__col__", 1990, 1990, 2005),
    ): (
        "IN",
        [
            "1989-04-07",
            None,
        ],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_birthday",
        ("IN", 3, "COALESCE", 2, "YEAR", 1, "__col__", 2005, 2005, 2006),
    ): (
        "IN",
        [
            None,
        ],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_birthday",
        ("NOT", 1, "IN", 3, "COALESCE", 2, "YEAR", 1, "__col__", 1990, 1990, 1991),
    ): (
        "NOT_IN",
        [
            "1990-07-31",
            "1989-04-07",
            None,
        ],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_birthday",
        ("NOT", 1, "IN", 3, "COALESCE", 2, "YEAR", 1, "__col__", 1990, 1990, 2005),
    ): (
        "NOT_IN",
        [
            "1989-04-07",
            None,
        ],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_birthday",
        ("NOT", 1, "IN", 3, "COALESCE", 2, "YEAR", 1, "__col__", 2005, 2005, 2006),
    ): (
        "NOT_IN",
        [
            None,
        ],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        ("IN", 4, "SLICE", 3, "__col__", 0, 1, "q", "r", "s"),
    ): ("IN", ["QUEENIE", "ROBERT", "SOPHIA"]),
    (
        "srv/CRBNK/CUSTOMERS/c_lname",
        (
            "CONTAINS",
            2,
            "__col__",
            "CONCAT",
            2,
            "e",
            "IFF",
            3,
            "IN",
            4,
            "SLICE",
            3,
            "__col__",
            0,
            1,
            "q",
            "r",
            "s",
            "z",
            "e",
        ),
    ): (
        "IN",
        ["LEE", "RODRIGUEZ"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_fname", ("EQUAL", 2, "SLICE", 3, "__col__", 0, 1, "i")): (
        "IN",
        ["ISABEL"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        ("IN", 6, "SLICE", 3, "__col__", 1, 2, "ar", "li", "ra", "to", "am"),
    ): (
        "IN",
        [
            "ALICE",
            "CAROL",
            "FRANK",
            "GRACE",
            "JAMES",
            "KAREN",
            "MARIA",
            "OLIVIA",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATETRUNC", 2, "year", "__col__", "2023-01-01"),
    ): (
        "IN",
        [
            "2022-12-31 17:42:54",
            "2023-01-04 12:05:15",
            "2023-01-07 22:11:27",
            "2023-01-20 04:38:03",
            "2023-01-20 16:40:54",
            "2023-01-27 15:13:18",
            "2023-01-30 19:58:26",
            "2023-02-02 19:12:58",
            "2023-02-11 11:13:53",
            "2023-02-11 12:32:55",
            "2023-02-15 21:54:29",
            "2023-02-16 14:18:36",
            "2023-02-28 07:11:29",
            "2023-03-07 01:26:10",
            "2023-03-08 18:58:18",
            "2023-03-14 14:23:33",
            "2023-03-16 06:17:44",
            "2023-03-17 08:48:16",
            "2023-03-24 03:33:40",
            "2023-03-26 06:52:52",
            "2023-04-18 00:35:40",
            "2023-04-25 18:54:26",
            "2023-04-29 04:58:30",
            "2023-05-04 23:30:10",
            "2023-05-12 04:42:28",
            "2023-05-17 18:54:12",
            "2023-05-19 10:10:44",
            "2023-05-21 13:52:14",
            "2023-05-24 03:51:10",
            "2023-06-01 13:50:10",
            "2023-06-01 13:50:14",
            "2023-06-04 10:35:26",
            "2023-06-11 21:53:04",
            "2023-06-25 15:06:06",
            "2023-06-25 21:58:37",
            "2023-06-27 03:21:19",
            "2023-06-27 10:34:20",
            "2023-06-30 15:27:03",
            "2023-07-07 15:17:47",
            "2023-07-17 03:23:15",
            "2023-07-18 14:41:26",
            "2023-08-03 20:24:35",
            "2023-08-11 20:25:39",
            "2023-08-29 03:07:18",
            "2023-09-01 16:50:48",
            "2023-09-08 09:30:23",
            "2023-09-13 06:42:39",
            "2023-09-15 09:00:02",
            "2023-09-30 08:57:30",
            "2023-10-15 02:47:04",
            "2023-10-19 09:40:06",
            "2023-10-30 00:20:45",
            "2023-11-08 12:52:24",
            "2023-11-10 17:20:29",
            "2023-11-16 11:30:24",
            "2023-11-21 15:17:10",
            "2023-11-28 06:34:03",
            "2023-12-07 14:11:33",
            "2023-12-15 05:57:23",
            "2023-12-16 00:51:23",
            "2023-12-23 07:54:22",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATETRUNC", 2, "quarter", "__col__", "2023-04-01"),
    ): (
        "IN",
        [
            "2023-04-18 00:35:40",
            "2023-04-25 18:54:26",
            "2023-04-29 04:58:30",
            "2023-05-04 23:30:10",
            "2023-05-12 04:42:28",
            "2023-05-17 18:54:12",
            "2023-05-19 10:10:44",
            "2023-05-21 13:52:14",
            "2023-05-24 03:51:10",
            "2023-06-01 13:50:10",
            "2023-06-01 13:50:14",
            "2023-06-04 10:35:26",
            "2023-06-11 21:53:04",
            "2023-06-25 15:06:06",
            "2023-06-25 21:58:37",
            "2023-06-27 03:21:19",
            "2023-06-27 10:34:20",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATETRUNC", 2, "month", "__col__", "2023-06-01"),
    ): (
        "IN",
        [
            "2023-06-01 13:50:10",
            "2023-06-01 13:50:14",
            "2023-06-04 10:35:26",
            "2023-06-11 21:53:04",
            "2023-06-25 15:06:06",
            "2023-06-25 21:58:37",
            "2023-06-27 03:21:19",
            "2023-06-27 10:34:20",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATETRUNC", 2, "day", "__col__", "2023-06-02"),
    ): (
        "IN",
        [
            "2023-06-01 13:50:10",
            "2023-06-01 13:50:14",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATETRUNC", 2, "hour", "__col__", "2023-06-02 04:00:00"),
    ): (
        "IN",
        [
            "2023-06-01 13:50:10",
            "2023-06-01 13:50:14",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATETRUNC", 2, "minute", "__col__", "2023-06-02 04:55:00"),
    ): (
        "IN",
        [
            "2023-06-01 13:50:10",
            "2023-06-01 13:50:14",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATETRUNC", 2, "second", "__col__", "2023-06-02 04:55:31"),
    ): (
        "IN",
        [
            "2023-06-01 13:50:10",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATEADD", 3, 1, "years", "__col__", "2020-11-11 18:00:52"),
    ): (
        "IN",
        [
            "2019-11-11 02:55:31",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATEADD", 3, 2, "quarters", "__col__", "2020-05-11 18:00:52"),
    ): (
        "IN",
        [
            "2019-11-11 02:55:31",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATEADD", 3, -5, "months", "__col__", "2019-06-11 18:00:52"),
    ): (
        "IN",
        [
            "2019-11-11 02:55:31",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATEADD", 3, 10, "days", "__col__", "2019-11-21 18:00:52"),
    ): (
        "IN",
        [
            "2019-11-11 02:55:31",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATEADD", 3, 1000, "hours", "__col__", "2019-12-23 10:00:52"),
    ): (
        "IN",
        [
            "2019-11-11 02:55:31",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        ("EQUAL", 2, "DATEADD", 3, 10000, "minutes", "__col__", "2019-11-18 16:40:52"),
    ): (
        "IN",
        [
            "2019-11-11 02:55:31",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        (
            "EQUAL",
            2,
            "DATEADD",
            3,
            -1000000,
            "seconds",
            "__col__",
            "2019-10-31 04:14:12",
        ),
    ): (
        "IN",
        [
            "2019-11-11 02:55:31",
        ],
    ),
    (
        "srv/CRBNK/TRANSACTIONS/t_ts",
        (
            "EQUAL",
            2,
            "DATEADD",
            3,
            -1,
            "days",
            "DATETRUNC",
            2,
            "month",
            "__col__",
            "2019-10-31",
        ),
    ): (
        "IN",
        [
            "2019-11-02 11:58:37",
            "2019-11-02 12:54:09",
            "2019-11-11 02:55:31",
            "2019-11-11 15:44:22",
        ],
    ),
    ("srv/CRBNK/CUSTOMERS/c_fname", ("CONTAINS", 2, "__col__", "a")): (
        "NOT_IN",
        ["BOB", "EMILY", "HENRY", "LUKE", "PETER", "QUEENIE", "ROBERT"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_fname", ("CONTAINS", 2, "__col__", "e")): (
        "NOT_IN",
        [
            "BOB",
            "CAROL",
            "DAVID",
            "FRANK",
            "MARIA",
            "NICHOLAS",
            "OLIVIA",
            "SOPHIA",
            "THOMAS",
        ],
    ),
    ("srv/CRBNK/CUSTOMERS/c_fname", ("CONTAINS", 2, "__col__", "i")): (
        "IN",
        [
            "ALICE",
            "DAVID",
            "EMILY",
            "ISABEL",
            "MARIA",
            "NICHOLAS",
            "OLIVIA",
            "QUEENIE",
            "SOPHIA",
        ],
    ),
    ("srv/CRBNK/CUSTOMERS/c_fname", ("CONTAINS", 2, "__col__", "o")): (
        "IN",
        ["BOB", "CAROL", "NICHOLAS", "OLIVIA", "ROBERT", "SOPHIA", "THOMAS"],
    ),
    ("srv/CRBNK/CUSTOMERS/c_fname", ("CONTAINS", 2, "__col__", "u")): (
        "IN",
        ["LUKE", "QUEENIE"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        ("AND", 2, "CONTAINS", 2, "__col__", "a", "CONTAINS", 2, "__col__", "e"),
    ): (
        "IN",
        ["ALICE", "GRACE", "ISABEL", "JAMES", "KAREN"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        ("AND", 2, "CONTAINS", 2, "__col__", "e", "CONTAINS", 2, "__col__", "i"),
    ): (
        "IN",
        ["ALICE", "EMILY", "ISABEL", "QUEENIE"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        ("AND", 2, "CONTAINS", 2, "__col__", "i", "CONTAINS", 2, "__col__", "o"),
    ): (
        "IN",
        ["NICHOLAS", "OLIVIA", "SOPHIA"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        ("AND", 2, "CONTAINS", 2, "__col__", "o", "CONTAINS", 2, "__col__", "u"),
    ): (
        "IN",
        [],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        ("AND", 2, "CONTAINS", 2, "__col__", "u", "CONTAINS", 2, "__col__", "a"),
    ): (
        "IN",
        [],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        (
            "AND",
            3,
            "CONTAINS",
            2,
            "__col__",
            "a",
            "CONTAINS",
            2,
            "__col__",
            "e",
            "CONTAINS",
            2,
            "__col__",
            "i",
        ),
    ): (
        "IN",
        ["ALICE", "ISABEL"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        (
            "AND",
            3,
            "CONTAINS",
            2,
            "__col__",
            "e",
            "CONTAINS",
            2,
            "__col__",
            "i",
            "CONTAINS",
            2,
            "__col__",
            "o",
        ),
    ): (
        "IN",
        [],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        (
            "NOT",
            1,
            "AND",
            2,
            "CONTAINS",
            2,
            "__col__",
            "i",
            "CONTAINS",
            2,
            "__col__",
            "o",
        ),
    ): (
        "NOT_IN",
        ["NICHOLAS", "OLIVIA", "SOPHIA"],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        (
            "AND",
            2,
            "CONTAINS",
            2,
            "__col__",
            "i",
            "NOT",
            1,
            "AND",
            2,
            "CONTAINS",
            2,
            "__col__",
            "a",
            "CONTAINS",
            2,
            "__col__",
            "e",
        ),
    ): (
        "IN",
        [],
    ),
    (
        "srv/CRBNK/CUSTOMERS/c_fname",
        (
            "NOT",
            1,
            "AND",
            2,
            "CONTAINS",
            2,
            "__col__",
            "e",
            "CONTAINS",
            2,
            "__col__",
            "i",
        ),
    ): (
        "NOT_IN",
        ["ALICE", "EMILY", "ISABEL", "QUEENIE"],
    ),
}
