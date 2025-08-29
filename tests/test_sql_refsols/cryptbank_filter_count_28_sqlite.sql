SELECT
  COUNT(*) AS n
FROM crbnk.accounts AS accounts
JOIN crbnk.customers AS customers
  ON accounts.a_custkey = customers.c_key
  AND (
    customers.c_email LIKE '%gmail%' OR customers.c_email LIKE '%outlook%'
  )
WHERE
  CAST(STRFTIME('%Y', accounts.a_open_ts) AS INTEGER) < 2020
  AND accounts.a_balance >= 5000
  AND (
    accounts.a_type = 'retirement' OR accounts.a_type = 'savings'
  )
