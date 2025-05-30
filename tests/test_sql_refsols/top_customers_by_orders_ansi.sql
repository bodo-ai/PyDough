WITH _s1 AS (
  SELECT
    COUNT() AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  GROUP BY
    o_custkey
), _t0 AS (
  SELECT
    customer.c_custkey AS customer_key_3,
    COALESCE(_s1.agg_0, 0) AS n_orders,
    COALESCE(_s1.agg_0, 0) AS ordering_1
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.customer_key = customer.c_custkey
  ORDER BY
    ordering_1 DESC,
    customer.c_custkey
  LIMIT 5
)
SELECT
  customer_key_3 AS customer_key,
  n_orders
FROM _t0
ORDER BY
  ordering_1 DESC,
  customer_key_3
