WITH _u_0 AS (
  SELECT
    n_nationkey AS _u_1
  FROM tpch.NATION
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN _u_0 AS _u_0
  ON CUSTOMER.c_nationkey = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
