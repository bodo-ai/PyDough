WITH _s0 AS (
  SELECT
    n_name,
    n_nationkey,
    n_regionkey
  FROM tpch.nation
  ORDER BY
    1
  LIMIT 5
), _t1 AS (
  SELECT
    CASE
      WHEN CAST(1.0 AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_17,
    customer.c_nationkey,
    orders.o_totalprice,
    CASE
      WHEN CAST(0.99 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_10,
    CASE
      WHEN CAST(0.75 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_11,
    CASE
      WHEN CAST(0.25 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_12,
    CASE
      WHEN CAST(0.09999999999999998 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_13,
    CASE
      WHEN CAST(0.010000000000000009 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_14,
    CASE
      WHEN CAST(0.5 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_16,
    CASE
      WHEN CAST(0.9 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_9
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1998
    AND customer.c_custkey = orders.o_custkey
), _s5 AS (
  SELECT
    c_nationkey,
    MAX(expr_10) AS max_expr_10,
    MAX(expr_11) AS max_expr_11,
    MAX(expr_12) AS max_expr_12,
    MAX(expr_13) AS max_expr_13,
    MAX(expr_14) AS max_expr_14,
    MAX(expr_16) AS max_expr_16,
    MAX(expr_17) AS max_expr_17,
    MAX(expr_9) AS max_expr_9,
    MAX(o_totalprice) AS max_o_totalprice
  FROM _t1
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  _s0.n_name AS nation_name,
  _s5.max_expr_17 AS orders_min,
  _s5.max_expr_10 AS orders_1_percent,
  _s5.max_expr_9 AS orders_10_percent,
  _s5.max_expr_11 AS orders_25_percent,
  _s5.max_expr_16 AS orders_median,
  _s5.max_expr_12 AS orders_75_percent,
  _s5.max_expr_13 AS orders_90_percent,
  _s5.max_expr_14 AS orders_99_percent,
  _s5.max_o_totalprice AS orders_max
FROM _s0 AS _s0
JOIN tpch.region AS region
  ON _s0.n_regionkey = region.r_regionkey
LEFT JOIN _s5 AS _s5
  ON _s0.n_nationkey = _s5.c_nationkey
ORDER BY
  2
