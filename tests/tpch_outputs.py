"""
File that holds expected outputs for the TPC-H queries.
"""

import pandas as pd


def tpch_q1_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 1.
    """
    columns = [
        "l_returnflag",
        "l_linestatus",
        "sum_qty",
        "sum_base_price",
        "sum_disc_price",
        "sum_charge",
        "avg_qty",
        "avg_price",
        "avg_disc",
        "count_order",
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


def tpch_q2_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 2. Note: This is truncated to
    the first 10 rows.
    """
    columns = [
        "s_acctbal",
        "s_name",
        "n_name",
        "p_partkey",
        "p_mfgr",
        "s_address",
        "s_phone",
        "s_comment",
    ]
    data = [
        (
            9938.53,
            "Supplier#000005359",
            "UNITED KINGDOM",
            185358,
            "Manufacturer#4",
            "QKuHYh,vZGiwu2FWEJoLDx04",
            "33-429-790-6131",
            "uriously regular requests hag",
        ),
        (
            9937.84,
            "Supplier#000005969",
            "ROMANIA",
            108438,
            "Manufacturer#1",
            "ANDENSOSmk,miq23Xfb5RWt6dvUcvt6Qa",
            "29-520-692-3537",
            "efully express instructions. regular requests against the slyly fin",
        ),
        (
            9936.22,
            "Supplier#000005250",
            "UNITED KINGDOM",
            249,
            "Manufacturer#4",
            "B3rqp0xbSEim4Mpy2RH J",
            "33-320-228-2957",
            "etect about the furiously final accounts. slyly ironic pinto beans sleep inside the furiously",
        ),
        (
            9923.77,
            "Supplier#000002324",
            "GERMANY",
            29821,
            "Manufacturer#4",
            "y3OD9UywSTOk",
            "17-779-299-1839",
            "ackages boost blithely. blithely regular deposits c",
        ),
        (
            9871.22,
            "Supplier#000006373",
            "GERMANY",
            43868,
            "Manufacturer#5",
            "J8fcXWsTqM",
            "17-813-485-8637",
            "etect blithely bold asymptotes. fluffily ironic platelets wake furiously; blit",
        ),
        (
            9870.78,
            "Supplier#000001286",
            "GERMANY",
            81285,
            "Manufacturer#2",
            "YKA,E2fjiVd7eUrzp2Ef8j1QxGo2DFnosaTEH",
            "17-516-924-4574",
            " regular accounts. furiously unusual courts above the fi",
        ),
        (
            9870.78,
            "Supplier#000001286",
            "GERMANY",
            181285,
            "Manufacturer#4",
            "YKA,E2fjiVd7eUrzp2Ef8j1QxGo2DFnosaTEH",
            "17-516-924-4574",
            " regular accounts. furiously unusual courts above the fi",
        ),
        (
            9852.52,
            "Supplier#000008973",
            "RUSSIA",
            18972,
            "Manufacturer#2",
            "t5L67YdBYYH6o,Vz24jpDyQ9",
            "32-188-594-7038",
            "rns wake final foxes. carefully unusual depende",
        ),
        (
            9847.83,
            "Supplier#000008097",
            "RUSSIA",
            130557,
            "Manufacturer#2",
            "xMe97bpE69NzdwLoX",
            "32-375-640-3593",
            " the special excuses. silent sentiments serve carefully final ac",
        ),
        (
            9847.57,
            "Supplier#000006345",
            "FRANCE",
            86344,
            "Manufacturer#1",
            "VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag",
            "16-886-766-7945",
            "ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu",
        ),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q3_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 3.
    """
    columns = ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]
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


def tpch_q4_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 4.
    """
    columns = ["o_orderpriority", "order_count"]
    data = [
        ("1-URGENT", 10594),
        ("2-HIGH", 10476),
        ("3-MEDIUM", 10410),
        ("4-NOT SPECIFIED", 10556),
        ("5-LOW", 10487),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q5_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 5.
    """
    columns = ["n_name", "revenue"]
    data = [
        ("INDONESIA", 55502041.1697),
        ("VIETNAM", 55295086.9967),
        ("CHINA", 53724494.2566),
        ("INDIA", 52035512.000199996),
        ("JAPAN", 45410175.6954),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q6_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 6.
    """
    columns = ["revenue"]
    data = [(123141078.2283,)]
    return pd.DataFrame(data, columns=columns)


def tpch_q7_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 7.
    """
    columns = ["supp_nation", "cust_nation", "l_year", "revenue"]
    data = [
        ("FRANCE", "GERMANY", 1995, 54639732.7336),
        ("FRANCE", "GERMANY", 1996, 54633083.3076),
        ("GERMANY", "FRANCE", 1995, 52531746.6697),
        ("GERMANY", "FRANCE", 1996, 52520549.0224),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q8_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 8.
    """
    columns = ["o_year", "mkt_share"]
    data = [(1995, 0.034435890406654804), (1996, 0.04148552129353032)]
    return pd.DataFrame(data, columns=columns)


def tpch_q9_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 9. Note: This is truncated to
    the first 10 rows.
    """
    columns = ["nation", "o_year", "amount"]
    data = [
        ("ALGERIA", 1998, 27136900.1803),
        ("ALGERIA", 1997, 48611833.496199995),
        ("ALGERIA", 1996, 48285482.6782),
        ("ALGERIA", 1995, 44402273.5999),
        ("ALGERIA", 1994, 48694008.0668),
        ("ALGERIA", 1993, 46044207.7838),
        ("ALGERIA", 1992, 45636849.4881),
        ("ARGENTINA", 1998, 28341663.7848),
        ("ARGENTINA", 1997, 47143964.1176),
        ("ARGENTINA", 1996, 45255278.6021),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q10_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 10.
    """
    columns = [
        "c_custkey",
        "c_name",
        "revenue",
        "c_acctbal",
        "n_name",
        "c_address",
        "c_phone",
        "c_comment",
    ]
    data = [
        (
            57040,
            "Customer#000057040",
            734235.2455,
            632.87,
            "JAPAN",
            "Eioyzjf4pp",
            "22-895-641-3466",
            "sits. slyly regular requests sleep alongside of the regular inst",
        ),
        (
            143347,
            "Customer#000143347",
            721002.6947999999,
            2557.47,
            "EGYPT",
            "1aReFYv,Kw4",
            "14-742-935-3718",
            "ggle carefully enticing requests. final deposits use bold, bold pinto beans. ironic, idle re",
        ),
        (
            60838,
            "Customer#000060838",
            679127.3077,
            2454.77,
            "BRAZIL",
            "64EaJ5vMAHWJlBOxJklpNc2RJiWE",
            "12-913-494-9813",
            " need to boost against the slyly regular account",
        ),
        (
            101998,
            "Customer#000101998",
            637029.5667,
            3790.89,
            "UNITED KINGDOM",
            "01c9CILnNtfOQYmZj",
            "33-593-865-6378",
            "ress foxes wake slyly after the bold excuses. ironic platelets are furiously carefully bold theodolites",
        ),
        (
            125341,
            "Customer#000125341",
            633508.086,
            4983.51,
            "GERMANY",
            "S29ODD6bceU8QSuuEJznkNaK",
            "17-582-695-5962",
            "arefully even depths. blithely even excuses sleep furiously. foxes use except the dependencies. ca",
        ),
        (
            25501,
            "Customer#000025501",
            620269.7849,
            7725.04,
            "ETHIOPIA",
            "  W556MXuoiaYCCZamJI,Rn0B4ACUGdkQ8DZ",
            "15-874-808-6793",
            "he pending instructions wake carefully at the pinto beans. regular, final instructions along the slyly fina",
        ),
        (
            115831,
            "Customer#000115831",
            596423.8672,
            5098.1,
            "FRANCE",
            "rFeBbEEyk dl ne7zV5fDrmiq1oK09wV7pxqCgIc",
            "16-715-386-3788",
            "l somas sleep. furiously final deposits wake blithely regular pinto b",
        ),
        (
            84223,
            "Customer#000084223",
            594998.0239,
            528.65,
            "UNITED KINGDOM",
            "nAVZCs6BaWap rrM27N 2qBnzc5WBauxbA",
            "33-442-824-8191",
            " slyly final deposits haggle regular, pending dependencies. pending escapades wake ",
        ),
        (
            54289,
            "Customer#000054289",
            585603.3918,
            5583.02,
            "IRAN",
            "vXCxoCsU0Bad5JQI ,oobkZ",
            "20-834-292-4707",
            "ely special foxes are quickly finally ironic p",
        ),
        (
            39922,
            "Customer#000039922",
            584878.1134,
            7321.11,
            "GERMANY",
            "Zgy4s50l2GKN4pLDPBU8m342gIw6R",
            "17-147-757-8036",
            "y final requests. furiously final foxes cajole blithely special platelets. f",
        ),
        (
            6226,
            "Customer#000006226",
            576783.7606,
            2230.09,
            "UNITED KINGDOM",
            "8gPu8,NPGkfyQQ0hcIYUGPIBWc,ybP5g,",
            "33-657-701-3391",
            "ending platelets along the express deposits cajole carefully final ",
        ),
        (
            922,
            "Customer#000000922",
            576767.5333,
            3869.25,
            "GERMANY",
            "Az9RFaut7NkPnc5zSD2PwHgVwr4jRzq",
            "17-945-916-9648",
            "luffily fluffy deposits. packages c",
        ),
        (
            147946,
            "Customer#000147946",
            576455.132,
            2030.13,
            "ALGERIA",
            "iANyZHjqhyy7Ajah0pTrYyhJ",
            "10-886-956-3143",
            "ithely ironic deposits haggle blithely ironic requests. quickly regu",
        ),
        (
            115640,
            "Customer#000115640",
            569341.1933,
            6436.1,
            "ARGENTINA",
            "Vtgfia9qI 7EpHgecU1X",
            "11-411-543-4901",
            "ost slyly along the patterns; pinto be",
        ),
        (
            73606,
            "Customer#000073606",
            568656.8578,
            1785.67,
            "JAPAN",
            "xuR0Tro5yChDfOCrjkd2ol",
            "22-437-653-6966",
            "he furiously regular ideas. slowly",
        ),
        (
            110246,
            "Customer#000110246",
            566842.9815,
            7763.35,
            "VIETNAM",
            "7KzflgX MDOq7sOkI",
            "31-943-426-9837",
            "egular deposits serve blithely above the fl",
        ),
        (
            142549,
            "Customer#000142549",
            563537.2368,
            5085.99,
            "INDONESIA",
            "ChqEoK43OysjdHbtKCp6dKqjNyvvi9",
            "19-955-562-2398",
            "sleep pending courts. ironic deposits against the carefully unusual platelets cajole carefully express accounts.",
        ),
        (
            146149,
            "Customer#000146149",
            557254.9865,
            1791.55,
            "ROMANIA",
            "s87fvzFQpU",
            "29-744-164-6487",
            " of the slyly silent accounts. quickly final accounts across the ",
        ),
        (
            52528,
            "Customer#000052528",
            556397.3509,
            551.79,
            "ARGENTINA",
            "NFztyTOR10UOJ",
            "11-208-192-3205",
            " deposits hinder. blithely pending asymptotes breach slyly regular re",
        ),
        (
            23431,
            "Customer#000023431",
            554269.536,
            3381.86,
            "ROMANIA",
            "HgiV0phqhaIa9aydNoIlb",
            "29-915-458-2654",
            "nusual, even instructions: furiously stealthy n",
        ),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q11_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 11. Note: This is truncated to
    the first 10 rows.
    """
    columns = ["ps_partkey", "value"]
    data = [
        (129760, 17538456.86),
        (166726, 16503353.92),
        (191287, 16474801.969999999),
        (161758, 16101755.54),
        (34452, 15983844.72),
        (139035, 15907078.34),
        (9403, 15451755.620000001),
        (154358, 15212937.879999999),
        (38823, 15064802.86),
        (85606, 15053957.150000002),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q12_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 12.
    """
    columns = ["l_shipmode", "high_line_count", "low_line_count"]
    data = [("MAIL", 6202, 9324), ("SHIP", 6200, 9262)]
    return pd.DataFrame(data, columns=columns)


def tpch_q13_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 13. Note: This is truncated to
    the first 10 rows.
    """
    columns = ["c_count", "custdist"]
    data = [
        (0, 50005),
        (9, 6641),
        (10, 6532),
        (11, 6014),
        (8, 5937),
        (12, 5639),
        (13, 5024),
        (19, 4793),
        (7, 4687),
        (17, 4587),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q14_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 14.
    """
    columns = ["promo_revenue"]
    data = [(16.38077862639554,)]
    return pd.DataFrame(data, columns=columns)


def tpch_q15_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 15.
    """
    columns = ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]
    data = [
        (
            8449,
            "Supplier#000008449",
            "Wp34zim9qYFbVctdW",
            "20-469-856-8873",
            1772627.2087,
        )
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q16_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 16. Note: This is truncated to
    the first 10 rows.
    """
    columns = ["p_brand", "p_type", "p_size", "supplier_count"]
    data = [
        ("Brand#41", "MEDIUM BRUSHED TIN", 3, 28),
        ("Brand#54", "STANDARD BRUSHED COPPER", 14, 27),
        ("Brand#11", "STANDARD BRUSHED TIN", 23, 24),
        ("Brand#11", "STANDARD BURNISHED BRASS", 36, 24),
        ("Brand#15", "MEDIUM ANODIZED NICKEL", 3, 24),
        ("Brand#15", "SMALL ANODIZED BRASS", 45, 24),
        ("Brand#15", "SMALL BURNISHED NICKEL", 19, 24),
        ("Brand#21", "MEDIUM ANODIZED COPPER", 3, 24),
        ("Brand#22", "SMALL BRUSHED NICKEL", 3, 24),
        ("Brand#22", "SMALL BURNISHED BRASS", 19, 24),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q17_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 17.

    This query needs manual rewriting to run efficiently in SQLite
    by avoiding the correlated join.
    """
    columns = ["avg_yearly"]
    data = [(348406.0542857143,)]
    return pd.DataFrame(data, columns=columns)


def tpch_q18_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 18. Note: This is truncated to
    the first 10 rows.
    """
    columns = [
        "c_name",
        "c_custkey",
        "o_orderkey",
        "o_orderdate",
        "o_totalprice",
        "total_quantity",
    ]
    data = [
        ("Customer#000128120", 128120, 4722021, "1994-04-07", 544089.09, 323),
        ("Customer#000144617", 144617, 3043270, "1997-02-12", 530604.44, 317),
        ("Customer#000013940", 13940, 2232932, "1997-04-13", 522720.61, 304),
        ("Customer#000066790", 66790, 2199712, "1996-09-30", 515531.82, 327),
        ("Customer#000046435", 46435, 4745607, "1997-07-03", 508047.99, 309),
        ("Customer#000015272", 15272, 3883783, "1993-07-28", 500241.33, 302),
        ("Customer#000146608", 146608, 3342468, "1994-06-12", 499794.58, 303),
        ("Customer#000096103", 96103, 5984582, "1992-03-16", 494398.79, 312),
        ("Customer#000024341", 24341, 1474818, "1992-11-15", 491348.26, 302),
        ("Customer#000137446", 137446, 5489475, "1997-05-23", 487763.25, 311),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q19_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 19.
    """
    columns = ["revenue"]
    data = [(3083843.0578,)]
    return pd.DataFrame(data, columns=columns)


def tpch_q20_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 20. Note: This is truncated to
    the first 10 rows.

    This query needs manual rewriting to run efficiently in SQLite
    by avoiding the correlated join.
    """
    columns = ["s_name", "s_address"]
    data = [
        ("Supplier#000000020", "iybAE,RmTymrZVYaFZva2SH,j"),
        ("Supplier#000000091", "YV45D7TkfdQanOOZ7q9QxkyGUapU1oOWU6q3"),
        ("Supplier#000000205", "rF uV8d0JNEk"),
        ("Supplier#000000285", "Br7e1nnt1yxrw6ImgpJ7YdhFDjuBf"),
        ("Supplier#000000287", "7a9SP7qW5Yku5PvSg"),
        ("Supplier#000000354", "w8fOo5W,aS"),
        ("Supplier#000000378", "FfbhyCxWvcPrO8ltp9"),
        ("Supplier#000000402", "i9Sw4DoyMhzhKXCH9By,AYSgmD"),
        ("Supplier#000000530", "0qwCMwobKY OcmLyfRXlagA8ukENJv,"),
        ("Supplier#000000555", "TfB,a5bfl3Ah 3Z 74GqnNs6zKVGM"),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q21_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 21. Note: This is truncated to
    the first 10 rows.
    """
    columns = ["s_name", "num_wait"]
    data = [
        ("Supplier#000002829", 20),
        ("Supplier#000005808", 18),
        ("Supplier#000000262", 17),
        ("Supplier#000000496", 17),
        ("Supplier#000002160", 17),
        ("Supplier#000002301", 17),
        ("Supplier#000002540", 17),
        ("Supplier#000003063", 17),
        ("Supplier#000005178", 17),
        ("Supplier#000008331", 17),
    ]
    return pd.DataFrame(data, columns=columns)


def tpch_q22_output() -> pd.DataFrame:
    """
    Expected output for TPC-H query 22.

    This query needs manual rewriting to run efficiently in SQLite
    by avoiding the correlated join.
    """
    columns = ["cntrycode", "numcust", "totacctbal"]
    data = [
        ("13", 888, 6737713.99),
        ("17", 861, 6460573.72),
        ("18", 964, 7236687.4),
        ("23", 892, 6701457.95),
        ("29", 948, 7158866.63),
        ("30", 909, 6808436.13),
        ("31", 922, 6806670.18),
    ]
    return pd.DataFrame(data, columns=columns)
