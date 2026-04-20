WITH _u_0 AS (
  SELECT
    o_custkey AS _u_1
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1994
    AND o_orderpriority = '1-URGENT'
  GROUP BY
    1
)
SELECT
  MAX(customer.c_name) AS any
FROM tpch.customer AS customer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = customer.c_custkey
WHERE
  NOT _u_0._u_1 IS NULL
