WITH _t2 AS (
  SELECT
    o_custkey
  FROM tpch.ORDERS
), _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM _t2
  GROUP BY
    1
), _s2 AS (
  SELECT
    CUSTOMER.c_acctbal,
    CUSTOMER.c_custkey,
    _s1.n_rows
  FROM tpch.CUSTOMER AS CUSTOMER
  LEFT JOIN _s1 AS _s1
    ON CUSTOMER.c_custkey = _s1.o_custkey
  ORDER BY
    1 DESC,
    2 DESC
  LIMIT 10
), _u_0 AS (
  SELECT
    o_custkey AS _u_1
  FROM _t2
  GROUP BY
    1
)
SELECT
  _s2.c_custkey AS cust_key,
  COALESCE(_s2.n_rows, 0) AS n_orders
FROM _s2 AS _s2
LEFT JOIN _u_0 AS _u_0
  ON _s2.c_custkey = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL
ORDER BY
  _s2.c_acctbal DESC,
  1 DESC
