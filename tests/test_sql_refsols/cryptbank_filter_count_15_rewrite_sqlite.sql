WITH _u_0 AS (
  SELECT
    a_custkey AS _u_1
  FROM crbnk.accounts
  WHERE
    a_type = (
      SUBSTRING('retirement', 2) || SUBSTRING('retirement', 1, 1)
    )
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM crbnk.customers AS customers
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = (
    42 - customers.c_key
  )
WHERE
  NOT _u_0._u_1 IS NULL
