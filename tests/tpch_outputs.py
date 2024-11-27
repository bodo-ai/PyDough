"""
File that holds expected outputs for the TPC-H queries.
"""

import pandas as pd


def tpch_q1_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 1.
    """
    columns = [
        "L_RETURNFLAG",
        "L_LINESTATUS",
        "SUM_QTY",
        "SUM_BASE_PRICE",
        "SUM_DISC_PRICE",
        "SUM_CHARGE",
        "AVG_QTY",
        "AVG_PRICE",
        "AVG_DISC",
        "COUNT_ORDER",
    ]
    data = [
        (
            "A",
            "F",
            37734107,
            56586554400.73,
            53758257134.87,
            55909065222.82769,
            25.522005853257337,
            38273.129734621674,
            0.049985295838397614,
            1478493,
        ),
        (
            "N",
            "F",
            991417,
            1487504710.38,
            1413082168.0541,
            1469649223.194375,
            25.516471920522985,
            38284.4677608483,
            0.0500934266742163,
            38854,
        ),
        (
            "N",
            "O",
            76633518,
            114935210409.19,
            109189591897.472,
            113561024263.01378,
            25.50201963528761,
            38248.015609058646,
            0.05000025956756044,
            3004998,
        ),
        (
            "R",
            "F",
            37719753,
            56568041380.9,
            53741292684.604,
            55889619119.83193,
            25.50579361269077,
            38250.85462609966,
            0.05000940583012706,
            1478870,
        ),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q3_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 3.
    """
    columns = ["L_ORDERKEY", "REVENUE", "O_ORDERDATE", "O_SHIPPRIORITY"]
    data = [
        (2456423, 406181.0111, "1995-03-05", 0),
        (3459808, 405838.6989, "1995-03-04", 0),
        (492164, 390324.061, "1995-02-19", 0),
        (1188320, 384537.9359, "1995-03-09", 0),
        (2435712, 378673.0558, "1995-02-26", 0),
        (4878020, 378376.7952, "1995-03-12", 0),
        (5521732, 375153.9215, "1995-03-13", 0),
        (2628192, 373133.3094, "1995-02-22", 0),
        (993600, 371407.4595, "1995-03-05", 0),
        (2300070, 367371.1452, "1995-03-13", 0),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q6_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 6.
    """
    columns = ["REVENUE"]
    data = [(123141078.2283,)]
    return pd.DataFrame(data, columns=columns)
