WITH _s3 AS (
  SELECT
    COUNT() AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  GROUP BY
    o_custkey
)
SELECT
  customer.c_custkey AS cust_key,
  COALESCE(_s3.agg_0, 0) AS n_orders,
  COUNT(*) OVER (PARTITION BY nation.n_nationkey) AS n_cust
FROM tpch.nation AS nation
JOIN tpch.customer AS customer
  ON customer.c_nationkey = nation.n_nationkey
JOIN _s3 AS _s3
  ON _s3.customer_key = customer.c_custkey
ORDER BY
  customer.c_acctbal DESC
LIMIT 10
