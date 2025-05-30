WITH _t0 AS (
  SELECT
    COUNT() AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  GROUP BY
    o_custkey
)
SELECT
  customer.c_custkey AS cust_key,
  COALESCE(_t0.agg_0, 0) AS n_orders
FROM tpch.customer AS customer
JOIN _t0 AS _t0
  ON _t0.customer_key = customer.c_custkey
ORDER BY
  customer.c_acctbal DESC
LIMIT 10
