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
    orders.o_totalprice
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1998
    AND customer.c_custkey = orders.o_custkey
)
SELECT
  ANY_VALUE(region.r_name) AS region_name,
  ANY_VALUE(_s0.n_name) AS nation_name,
  PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY
    _s5.o_totalprice) AS orders_min,
  PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY
    _s5.o_totalprice) AS orders_1_percent,
  PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY
    _s5.o_totalprice) AS orders_10_percent,
  PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY
    _s5.o_totalprice) AS orders_25_percent,
  PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY
    _s5.o_totalprice) AS orders_median,
  PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY
    _s5.o_totalprice) AS orders_75_percent,
  PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY
    _s5.o_totalprice) AS orders_90_percent,
  PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY
    _s5.o_totalprice) AS orders_99_percent,
  PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY
    _s5.o_totalprice) AS orders_max
FROM _s0 AS _s0
JOIN tpch.region AS region
  ON _s0.n_regionkey = region.r_regionkey
LEFT JOIN _s5 AS _s5
  ON _s0.n_nationkey = _s5.c_nationkey
GROUP BY
  _s0.n_nationkey
ORDER BY
  2 NULLS FIRST
