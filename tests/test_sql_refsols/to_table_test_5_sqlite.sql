WITH _u_0 AS (
  SELECT
    nation_key AS _u_1
  FROM asian_nations_t5
  GROUP BY
    1
)
SELECT
  customer.c_name AS name
FROM tpch.customer AS customer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = customer.c_nationkey
WHERE
  NOT _u_0._u_1 IS NULL
ORDER BY
  1
LIMIT 5
