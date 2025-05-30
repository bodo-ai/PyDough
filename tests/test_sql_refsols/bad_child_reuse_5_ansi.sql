WITH _s0 AS (
  SELECT
    customer.c_acctbal AS account_balance,
    customer.c_custkey AS key
  FROM tpch.customer AS customer
), _s1 AS (
  SELECT
    orders.o_custkey AS customer_key
  FROM tpch.orders AS orders
), _t0 AS (
  SELECT
    NULL AS null_1,
    _s0.account_balance AS account_balance,
    _s0.key AS key
  FROM _s0 AS _s0
  WHERE
    NOT EXISTS(
      SELECT
        1 AS "1"
      FROM _s1 AS _s1
      WHERE
        _s0.key = _s1.customer_key
    )
)
SELECT
  _t0.key AS cust_key,
  COALESCE(_t0.null_1, 0) AS n_orders
FROM _t0 AS _t0
ORDER BY
  _t0.account_balance DESC
LIMIT 10
