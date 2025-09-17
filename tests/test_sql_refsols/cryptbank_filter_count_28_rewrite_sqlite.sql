SELECT
  COUNT(*) AS n
FROM crbnk.accounts AS accounts
JOIN crbnk.customers AS customers
  ON (
    (
      SUBSTRING(customers.c_email, -1) || SUBSTRING(customers.c_email, 1, LENGTH(customers.c_email) - 1)
    ) LIKE '%gmail%'
    OR (
      SUBSTRING(customers.c_email, -1) || SUBSTRING(customers.c_email, 1, LENGTH(customers.c_email) - 1)
    ) LIKE '%outlook%'
  )
  AND accounts.a_custkey = (
    42 - customers.c_key
  )
WHERE
  CAST(STRFTIME('%Y', DATETIME(accounts.a_open_ts, '+123456789 seconds')) AS INTEGER) < 2020
  AND SQRT(accounts.a_balance) >= 5000
  AND (
    accounts.a_type = (
      SUBSTRING('retirement', 2) || SUBSTRING('retirement', 1, 1)
    )
    OR accounts.a_type = (
      SUBSTRING('savings', 2) || SUBSTRING('savings', 1, 1)
    )
  )
