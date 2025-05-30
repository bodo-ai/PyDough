WITH _s3 AS (
  SELECT
    COUNT() AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  GROUP BY
    o_custkey
), _t2 AS (
  SELECT
    customer.c_acctbal AS account_balance,
    _s3.agg_0,
    customer.c_custkey AS key_2
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  LEFT JOIN _s3 AS _s3
    ON _s3.customer_key = customer.c_custkey
  QUALIFY
    COALESCE(_s3.agg_0, 0) < AVG(COALESCE(_s3.agg_0, 0)) OVER (PARTITION BY nation.n_nationkey)
    AND _s3.agg_0 > 0
), _t1 AS (
  SELECT
    account_balance,
    agg_0,
    key_2
  FROM _t2
  ORDER BY
    account_balance DESC
  LIMIT 10
)
SELECT
  key_2 AS cust_key,
  COALESCE(agg_0, 0) AS n_orders
FROM _t1
ORDER BY
  account_balance DESC
