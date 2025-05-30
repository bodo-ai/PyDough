WITH _s0 AS (
  SELECT
    customer.c_acctbal AS account_balance,
    customer.c_custkey AS key
  FROM tpch.customer AS customer
), _t2 AS (
  SELECT
    orders.o_custkey AS customer_key
  FROM tpch.orders AS orders
), _s1 AS (
  SELECT
    COUNT() AS agg_0,
    _t2.customer_key AS customer_key
  FROM _t2 AS _t2
  GROUP BY
    _t2.customer_key
), _t1 AS (
  SELECT
    _s0.account_balance AS account_balance,
    _s1.agg_0 AS agg_0,
    _s0.key AS key
  FROM _s0 AS _s0
  LEFT JOIN _s1 AS _s1
    ON _s0.key = _s1.customer_key
), _s2 AS (
  SELECT
    _t1.key AS cust_key,
    COALESCE(_t1.agg_0, 0) AS n_orders,
    _t1.account_balance AS account_balance,
    _t1.key AS key
  FROM _t1 AS _t1
  ORDER BY
    account_balance DESC
  LIMIT 10
), _t0 AS (
  SELECT
    _s2.account_balance AS account_balance,
    _s2.cust_key AS cust_key,
    _s2.n_orders AS n_orders
  FROM _s2 AS _s2
  WHERE
    NOT EXISTS(
      SELECT
        1 AS "1"
      FROM _t2 AS _s3
      WHERE
        _s2.key = _s3.customer_key
    )
)
SELECT
  _t0.cust_key AS cust_key,
  _t0.n_orders AS n_orders
FROM _t0 AS _t0
ORDER BY
  _t0.account_balance DESC
