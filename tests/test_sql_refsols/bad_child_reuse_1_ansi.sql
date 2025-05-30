WITH _s1 AS (
  SELECT
    COUNT() AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  GROUP BY
    o_custkey
), _t1 AS (
  SELECT
    customer.c_custkey AS cust_key,
    COALESCE(_s1.agg_0, 0) AS n_orders,
    customer.c_acctbal AS account_balance,
    _s1.agg_0
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.customer_key = customer.c_custkey
  ORDER BY
    account_balance DESC
  LIMIT 10
)
SELECT
  cust_key,
  n_orders
FROM _t1
WHERE
  agg_0 > 0
ORDER BY
  account_balance DESC
