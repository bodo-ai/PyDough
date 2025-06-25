WITH _s0 AS (
  SELECT
    n_name,
    n_nationkey,
    n_regionkey
  FROM tpch.nation
  ORDER BY
    n_name
  LIMIT 5
), _s5 AS (
  SELECT
    MAX(
      CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC) > CAST((
          1.0 - 0.1
        ) * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER)
        THEN orders.o_totalprice
        ELSE NULL
      END
    ) AS agg_0,
    MAX(
      CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC) > CAST((
          1.0 - 0.01
        ) * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER)
        THEN orders.o_totalprice
        ELSE NULL
      END
    ) AS agg_1,
    MAX(
      CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC) > CAST((
          1.0 - 0.25
        ) * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER)
        THEN orders.o_totalprice
        ELSE NULL
      END
    ) AS agg_2,
    MAX(
      CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC) > CAST((
          1.0 - 0.75
        ) * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER)
        THEN orders.o_totalprice
        ELSE NULL
      END
    ) AS agg_3,
    MAX(
      CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC) > CAST((
          1.0 - 0.9
        ) * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER)
        THEN orders.o_totalprice
        ELSE NULL
      END
    ) AS agg_4,
    MAX(
      CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC) > CAST((
          1.0 - 0.99
        ) * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER)
        THEN orders.o_totalprice
        ELSE NULL
      END
    ) AS agg_5,
    MAX(
      CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC) > CAST((
          1.0 - 1.0
        ) * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER)
        THEN orders.o_totalprice
        ELSE NULL
      END
    ) AS agg_6,
    MAX(
      CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC) > CAST((
          1.0 - 0.5
        ) * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER)
        THEN orders.o_totalprice
        ELSE NULL
      END
    ) AS agg_7,
    MAX(
      CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY orders.o_totalprice DESC) > CAST((
          1.0 - 0.0
        ) * COUNT(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS INTEGER)
        THEN orders.o_totalprice
        ELSE NULL
      END
    ) AS agg_8,
    customer.c_nationkey
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey AND orders.o_clerk = 'Clerk#000000272'
  GROUP BY
    customer.c_nationkey
)
SELECT
  region.r_name AS region_name,
  _s0.n_name AS nation_name,
  _s5.agg_8 AS orders_min,
  _s5.agg_1 AS orders_1_percent,
  _s5.agg_0 AS orders_10_percent,
  _s5.agg_2 AS orders_25_percent,
  _s5.agg_7 AS orders_median,
  _s5.agg_3 AS orders_75_percent,
  _s5.agg_4 AS orders_90_percent,
  _s5.agg_5 AS orders_99_percent,
  _s5.agg_6 AS orders_max
FROM _s0 AS _s0
JOIN tpch.region AS region
  ON _s0.n_regionkey = region.r_regionkey
LEFT JOIN _s5 AS _s5
  ON _s0.n_nationkey = _s5.c_nationkey
ORDER BY
  _s0.n_name
