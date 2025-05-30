WITH _t2 AS (
  SELECT
    o_custkey AS customer_key,
    o_totalprice AS total_price
  FROM tpch.orders
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate DESC NULLS FIRST, o_orderkey NULLS LAST) <= 5
), _t0 AS (
  SELECT
    SUM(total_price) AS agg_0,
    customer_key
  FROM _t2
  GROUP BY
    customer_key
)
SELECT
  customer.c_name AS name,
  COALESCE(_t0.agg_0, 0) AS total_recent_value
FROM tpch.customer AS customer
JOIN _t0 AS _t0
  ON _t0.customer_key = customer.c_custkey
ORDER BY
  total_recent_value DESC
LIMIT 3
