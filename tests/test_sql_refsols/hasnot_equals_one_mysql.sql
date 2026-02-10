WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.ORDERS
  WHERE
    o_orderpriority = '1-URGENT'
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN _s1 AS _s1
  ON CUSTOMER.c_custkey = _s1.o_custkey
WHERE
  (
    _s1.n_rows = 0 OR _s1.n_rows IS NULL
  ) = 1
