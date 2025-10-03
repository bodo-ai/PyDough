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
  (
    (
      SUBSTRING(accounts.a_type, -1) || SUBSTRING(accounts.a_type, 1, LENGTH(accounts.a_type) - 1)
    ) = 'retirement'
    OR (
      SUBSTRING(accounts.a_type, -1) || SUBSTRING(accounts.a_type, 1, LENGTH(accounts.a_type) - 1)
    ) = 'savings'
  )
  AND CAST(STRFTIME('%Y', DATETIME(accounts.a_open_ts, '+123456789 seconds')) AS INTEGER) < 2020
  AND SQRT(accounts.a_balance) >= 5000
