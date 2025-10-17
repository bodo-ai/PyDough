WITH _s3 AS (
  SELECT
    t_sourceaccount,
    COUNT(*) AS n_rows
  FROM crbnk.transactions
  WHERE
    (
      1025.67 - t_amount
    ) > 9000.0
  GROUP BY
    1
)
SELECT
  CASE
    WHEN accounts.a_key = 0
    THEN 0
    ELSE CASE WHEN accounts.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(
      accounts.a_key,
      1 + INSTR(accounts.a_key, '-'),
      CAST(LENGTH(accounts.a_key) AS REAL) / 2
    ) AS INTEGER)
  END AS key,
  CONCAT_WS(' ', LOWER(customers.c_fname), LOWER(customers.c_lname)) AS cust_name,
  _s3.n_rows AS n_trans
FROM crbnk.accounts AS accounts
JOIN crbnk.customers AS customers
  ON CAST(STRFTIME('%Y', DATE(customers.c_birthday, '+472 days')) AS INTEGER) <= 1985
  AND CAST(STRFTIME('%Y', DATE(customers.c_birthday, '+472 days')) AS INTEGER) >= 1980
  AND accounts.a_custkey = (
    42 - customers.c_key
  )
JOIN _s3 AS _s3
  ON _s3.t_sourceaccount = CASE
    WHEN accounts.a_key = 0
    THEN 0
    ELSE CASE WHEN accounts.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(
      accounts.a_key,
      1 + INSTR(accounts.a_key, '-'),
      CAST(LENGTH(accounts.a_key) AS REAL) / 2
    ) AS INTEGER)
  END
ORDER BY
  1
