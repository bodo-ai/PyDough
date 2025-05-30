WITH _s3 AS (
  SELECT
    COUNT() AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  GROUP BY
    o_custkey
), _t AS (
  SELECT
    customer.c_acctbal AS account_balance,
    _s3.agg_0,
    customer.c_custkey AS key_2,
    AVG(COALESCE(_s3.agg_0, 0)) OVER (PARTITION BY nation.n_nationkey) AS _w
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN _s3 AS _s3
    ON _s3.customer_key = customer.c_custkey
)
SELECT
  key_2 AS cust_key,
  agg_0 AS n_orders
FROM _t
WHERE
  _w > COALESCE(agg_0, 0)
ORDER BY
  account_balance DESC
LIMIT 10
