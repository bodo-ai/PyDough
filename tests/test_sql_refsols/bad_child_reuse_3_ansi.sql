WITH _s3 AS (
  SELECT
    COUNT() AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  GROUP BY
    o_custkey
), _t0 AS (
  SELECT
    customer.c_custkey AS cust_key,
    COUNT(*) OVER (PARTITION BY nation.n_nationkey) AS n_cust,
    COALESCE(_s3.agg_0, 0) AS n_orders,
    customer.c_acctbal AS account_balance,
    _s3.agg_0
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  LEFT JOIN _s3 AS _s3
    ON _s3.customer_key = customer.c_custkey
)
SELECT
  cust_key,
  n_orders,
  n_cust
FROM _t0
WHERE
  agg_0 > 0
ORDER BY
  account_balance DESC
LIMIT 10
