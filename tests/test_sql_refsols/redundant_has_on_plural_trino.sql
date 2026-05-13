WITH _u_0 AS (
  SELECT
    o_custkey AS _u_1
  FROM tpch.orders
  WHERE
    o_totalprice > 400000
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM tpch.customer AS customer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = customer.c_custkey
WHERE
  NOT _u_0._u_1 IS NULL
