WITH _u_0 AS (
  SELECT
    n_nationkey AS _u_1
  FROM tpch.nation
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM tpch.customer AS customer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = customer.c_nationkey
WHERE
  NOT _u_0._u_1 IS NULL
