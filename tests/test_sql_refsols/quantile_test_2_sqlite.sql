WITH _s0 AS (
  SELECT
    n_name,
    n_nationkey,
    n_regionkey
  FROM tpch.nation
  ORDER BY
    1
  LIMIT 5
), _s5 AS (
  SELECT
    customer.c_nationkey,
    orders.o_totalprice
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1998
    AND customer.c_custkey = orders.o_custkey
), _t1 AS (
  SELECT
    _s0.n_name,
    _s0.n_nationkey,
    _s5.o_totalprice,
    region.r_name,
    CASE
      WHEN CAST(0.99 * COUNT(_s5.o_totalprice) OVER (PARTITION BY _s5.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY _s5.c_nationkey ORDER BY _s5.o_totalprice DESC)
      THEN _s5.o_totalprice
      ELSE NULL
    END AS expr_10,
    CASE
      WHEN CAST(0.75 * COUNT(_s5.o_totalprice) OVER (PARTITION BY _s5.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY _s5.c_nationkey ORDER BY _s5.o_totalprice DESC)
      THEN _s5.o_totalprice
      ELSE NULL
    END AS expr_11,
    CASE
      WHEN CAST(0.25 * COUNT(_s5.o_totalprice) OVER (PARTITION BY _s5.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY _s5.c_nationkey ORDER BY _s5.o_totalprice DESC)
      THEN _s5.o_totalprice
      ELSE NULL
    END AS expr_12,
    CASE
      WHEN CAST(0.09999999999999998 * COUNT(_s5.o_totalprice) OVER (PARTITION BY _s5.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY _s5.c_nationkey ORDER BY _s5.o_totalprice DESC)
      THEN _s5.o_totalprice
      ELSE NULL
    END AS expr_13,
    CASE
      WHEN CAST(0.010000000000000009 * COUNT(_s5.o_totalprice) OVER (PARTITION BY _s5.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY _s5.c_nationkey ORDER BY _s5.o_totalprice DESC)
      THEN _s5.o_totalprice
      ELSE NULL
    END AS expr_14,
    CASE
      WHEN CAST(0.5 * COUNT(_s5.o_totalprice) OVER (PARTITION BY _s5.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY _s5.c_nationkey ORDER BY _s5.o_totalprice DESC)
      THEN _s5.o_totalprice
      ELSE NULL
    END AS expr_16,
    CASE
      WHEN CAST(COUNT(_s5.o_totalprice) OVER (PARTITION BY _s5.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY _s5.c_nationkey ORDER BY _s5.o_totalprice DESC)
      THEN _s5.o_totalprice
      ELSE NULL
    END AS expr_17,
    CASE
      WHEN CAST(0.9 * COUNT(_s5.o_totalprice) OVER (PARTITION BY _s5.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY _s5.c_nationkey ORDER BY _s5.o_totalprice DESC)
      THEN _s5.o_totalprice
      ELSE NULL
    END AS expr_9
  FROM _s0 AS _s0
  JOIN tpch.region AS region
    ON _s0.n_regionkey = region.r_regionkey
  LEFT JOIN _s5 AS _s5
    ON _s0.n_nationkey = _s5.c_nationkey
)
SELECT
  MAX(r_name) AS region_name,
  MAX(n_name) AS nation_name,
  MAX(expr_17) AS orders_min,
  MAX(expr_10) AS orders_1_percent,
  MAX(expr_9) AS orders_10_percent,
  MAX(expr_11) AS orders_25_percent,
  MAX(expr_16) AS orders_median,
  MAX(expr_12) AS orders_75_percent,
  MAX(expr_13) AS orders_90_percent,
  MAX(expr_14) AS orders_99_percent,
  MAX(o_totalprice) AS orders_max
FROM _t1
GROUP BY
  n_nationkey
ORDER BY
  2
