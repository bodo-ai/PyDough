SELECT
  COUNT(*) AS n
FROM crbnk.transactions AS transactions
JOIN crbnk.accounts AS accounts
  ON transactions.t_sourceaccount = CASE
    WHEN accounts.a_key = 0
    THEN 0
    ELSE CASE WHEN accounts.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(
      accounts.a_key,
      1 + INSTR(accounts.a_key, '-'),
      CAST(LENGTH(accounts.a_key) AS REAL) / 2
    ) AS INTEGER)
  END
JOIN crbnk.customers AS customers
  ON LOWER(customers.c_fname) = 'alice'
  AND accounts.a_custkey = (
    42 - customers.c_key
  )
