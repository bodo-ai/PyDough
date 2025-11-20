WITH _s0 AS (
  SELECT
    n_name COLLATE utf8mb4_bin AS n_name,
    n_nationkey,
    n_regionkey
  FROM tpch.NATION
  ORDER BY
    1
  LIMIT 5
), _t2 AS (
  SELECT
    CUSTOMER.c_nationkey,
    ORDERS.o_totalprice,
    CASE
      WHEN TRUNCATE(
        CAST(0.99 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey) AS FLOAT),
        0
      ) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS expr_10,
    CASE
      WHEN TRUNCATE(
        CAST(0.75 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey) AS FLOAT),
        0
      ) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS expr_11,
    CASE
      WHEN TRUNCATE(
        CAST(0.25 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey) AS FLOAT),
        0
      ) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS expr_12,
    CASE
      WHEN TRUNCATE(
        CAST(0.09999999999999998 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey) AS FLOAT),
        0
      ) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS expr_13,
    CASE
      WHEN TRUNCATE(
        CAST(0.010000000000000009 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey) AS FLOAT),
        0
      ) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS expr_14,
    CASE
      WHEN TRUNCATE(
        CAST(0.5 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey) AS FLOAT),
        0
      ) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS expr_16,
    CASE
      WHEN TRUNCATE(
        CAST(COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey) AS FLOAT),
        0
      ) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS expr_17,
    CASE
      WHEN TRUNCATE(
        CAST(0.9 * COUNT(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey) AS FLOAT),
        0
      ) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY ORDERS.o_totalprice DESC)
      THEN ORDERS.o_totalprice
      ELSE NULL
    END AS expr_9
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.ORDERS AS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey
    AND EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1998
), _s5 AS (
  SELECT
    c_nationkey,
    MAX(expr_10) AS max_expr10,
    MAX(expr_11) AS max_expr11,
    MAX(expr_12) AS max_expr12,
    MAX(expr_13) AS max_expr13,
    MAX(expr_14) AS max_expr14,
    MAX(expr_16) AS max_expr16,
    MAX(expr_17) AS max_expr17,
    MAX(expr_9) AS max_expr9,
    MAX(o_totalprice) AS max_ototalprice
  FROM _t2
  GROUP BY
    1
)
SELECT
  REGION.r_name AS region_name,
  _s0.n_name COLLATE utf8mb4_bin AS nation_name,
  _s5.max_expr17 AS orders_min,
  _s5.max_expr10 AS orders_1_percent,
  _s5.max_expr9 AS orders_10_percent,
  _s5.max_expr11 AS orders_25_percent,
  _s5.max_expr16 AS orders_median,
  _s5.max_expr12 AS orders_75_percent,
  _s5.max_expr13 AS orders_90_percent,
  _s5.max_expr14 AS orders_99_percent,
  _s5.max_ototalprice AS orders_max
FROM _s0 AS _s0
JOIN tpch.REGION AS REGION
  ON REGION.r_regionkey = _s0.n_regionkey
LEFT JOIN _s5 AS _s5
  ON _s0.n_nationkey = _s5.c_nationkey
ORDER BY
  2
