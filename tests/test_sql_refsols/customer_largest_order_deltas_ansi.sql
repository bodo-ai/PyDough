WITH _s1 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS agg_0,
    l_orderkey AS order_key
  FROM tpch.lineitem
  GROUP BY
    l_orderkey
), _t3 AS (
  SELECT
    _s1.agg_0,
    orders.o_custkey AS customer_key,
    orders.o_orderdate AS order_date
  FROM tpch.orders AS orders
  JOIN _s1 AS _s1
    ON _s1.order_key = orders.o_orderkey
  QUALIFY
    NOT LAG(COALESCE(_s1.agg_0, 0), 1) OVER (PARTITION BY orders.o_custkey ORDER BY orders.o_orderdate NULLS LAST) IS NULL
), _t1 AS (
  SELECT
    COALESCE(agg_0, 0) - LAG(COALESCE(agg_0, 0), 1) OVER (PARTITION BY customer_key ORDER BY order_date NULLS LAST) AS revenue_delta,
    customer_key
  FROM _t3
), _t0 AS (
  SELECT
    MAX(revenue_delta) AS max_diff,
    MIN(revenue_delta) AS min_diff,
    customer_key
  FROM _t1
  GROUP BY
    customer_key
)
SELECT
  customer.c_name AS name,
  CASE WHEN _t0.max_diff < ABS(_t0.min_diff) THEN _t0.min_diff ELSE _t0.max_diff END AS largest_diff
FROM tpch.customer AS customer
JOIN _t0 AS _t0
  ON _t0.customer_key = customer.c_custkey
ORDER BY
  largest_diff DESC
LIMIT 5
