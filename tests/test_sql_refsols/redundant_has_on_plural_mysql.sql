WITH _u_0 AS (
  SELECT
    o_custkey AS _u_1
  FROM tpch.ORDERS
  WHERE
    o_totalprice > 400000
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN _u_0 AS _u_0
  ON CUSTOMER.c_custkey = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
