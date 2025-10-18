SELECT
  CASE
    WHEN MAX(accounts.a_key) = 0
    THEN 0
    ELSE CASE WHEN MAX(accounts.a_key) > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(
      MAX(accounts.a_key),
      1 + INSTR(MAX(accounts.a_key), '-'),
      CAST(LENGTH(MAX(accounts.a_key)) AS REAL) / 2
    ) AS INTEGER)
  END AS key,
  CONCAT_WS(' ', LOWER(MAX(customers.c_fname)), LOWER(MAX(customers.c_lname))) AS cust_name,
  COUNT(*) AS n_trans
FROM crbnk.accounts AS accounts
JOIN crbnk.customers AS customers
  ON CAST(STRFTIME('%Y', DATE(customers.c_birthday, '+472 days')) AS INTEGER) <= 1985
  AND CAST(STRFTIME('%Y', DATE(customers.c_birthday, '+472 days')) AS INTEGER) >= 1980
  AND accounts.a_custkey = (
    42 - customers.c_key
  )
JOIN crbnk.transactions AS transactions
  ON (
    1025.67 - transactions.t_amount
  ) > 9000.0
  AND transactions.t_sourceaccount = CASE
    WHEN accounts.a_key = 0
    THEN 0
    ELSE CASE WHEN accounts.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(
      accounts.a_key,
      1 + INSTR(accounts.a_key, '-'),
      CAST(LENGTH(accounts.a_key) AS REAL) / 2
    ) AS INTEGER)
  END
GROUP BY
  transactions.t_sourceaccount
ORDER BY
  1
