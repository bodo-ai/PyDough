WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.ORDERS
  GROUP BY
    1
), _t AS (
  SELECT
    CUSTOMER.c_acctbal,
    CUSTOMER.c_custkey,
    _s1.n_rows,
    AVG(COALESCE(_s1.n_rows, 0)) OVER (PARTITION BY CUSTOMER.c_nationkey) AS _w
  FROM tpch.CUSTOMER AS CUSTOMER
  LEFT JOIN _s1 AS _s1
    ON CUSTOMER.c_custkey = _s1.o_custkey
)
SELECT
  c_custkey AS cust_key,
  n_rows AS n_orders
FROM _t
WHERE
  _w > COALESCE(n_rows, 0) AND n_rows <> 0
ORDER BY
  c_acctbal DESC
LIMIT 10
