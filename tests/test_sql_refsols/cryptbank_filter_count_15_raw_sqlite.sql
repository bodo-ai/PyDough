WITH _u_0 AS (
  SELECT
    a_custkey AS _u_1
  FROM crbnk.accounts
  WHERE
    (
      SUBSTRING(a_type, -1) || SUBSTRING(a_type, 1, LENGTH(a_type) - 1)
    ) = 'retirement'
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
