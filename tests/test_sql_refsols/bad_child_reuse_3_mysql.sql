WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.ORDERS
  GROUP BY
    1
), _t1 AS (
  SELECT
    CUSTOMER.c_acctbal,
    CUSTOMER.c_custkey,
    _s1.n_rows,
    COUNT(*) OVER (PARTITION BY CUSTOMER.c_nationkey) AS n_cust
  FROM tpch.CUSTOMER AS CUSTOMER
  LEFT JOIN _s1 AS _s1
    ON CUSTOMER.c_custkey = _s1.o_custkey
)
SELECT
  c_custkey AS cust_key,
  n_rows AS n_orders,
  n_cust
FROM _t1
WHERE
  n_rows <> 0
ORDER BY
  c_acctbal DESC
LIMIT 10
