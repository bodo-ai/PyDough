WITH _s0 AS (
  SELECT
    n_name,
    n_nationkey,
    n_regionkey
  FROM tpch.nation
  ORDER BY
    1 NULLS FIRST
  LIMIT 5
), _s5 AS (
  SELECT
    customer.c_nationkey,
    orders.o_totalprice,
    CASE
      WHEN FLOOR(0.99 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_10,
    CASE
      WHEN FLOOR(0.75 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_11,
    CASE
      WHEN FLOOR(0.25 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_12,
    CASE
      WHEN FLOOR(0.1 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_13,
    CASE
      WHEN FLOOR(0.01 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_14,
    CASE
      WHEN FLOOR(0.5 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_15,
    CASE
      WHEN FLOOR(0.9 * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey)) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC)
      THEN orders.o_totalprice
      ELSE NULL
    END AS expr_9
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey AND orders.o_clerk = 'Clerk#000000272'
)
SELECT
  ANY_VALUE(region.r_name) AS region_name,
  ANY_VALUE(_s0.n_name) AS nation_name,
  MIN(_s5.o_totalprice) AS orders_min,
  MAX(_s5.expr_10) AS orders_1_percent,
  MAX(_s5.expr_9) AS orders_10_percent,
  MAX(_s5.expr_11) AS orders_25_percent,
  MAX(_s5.expr_15) AS orders_median,
  MAX(_s5.expr_12) AS orders_75_percent,
  MAX(_s5.expr_13) AS orders_90_percent,
  MAX(_s5.expr_14) AS orders_99_percent,
  MAX(_s5.o_totalprice) AS orders_max
FROM _s0 AS _s0
JOIN tpch.region AS region
  ON _s0.n_regionkey = region.r_regionkey
LEFT JOIN _s5 AS _s5
  ON _s0.n_nationkey = _s5.c_nationkey
GROUP BY
  _s0.n_nationkey
ORDER BY
  2 NULLS FIRST
