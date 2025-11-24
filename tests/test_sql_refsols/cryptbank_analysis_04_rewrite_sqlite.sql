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
  ON accounts.a_custkey = (
    42 - customers.c_key
  )
  AND customers.c_birthday IN ('1980-01-18', '1981-07-21', '1981-11-15', '1982-11-07', '1983-12-27')
JOIN crbnk.transactions AS transactions
  ON transactions.t_amount IN (-8934.44, -8881.98, -8736.83, -8717.7, -8648.33, -8639.5, -8620.48, -8593.09, -8553.43, -8527.34, -8484.61, -8480.79, -8472.7, -8457.49, -8366.52, -8361.27, -8352.72, -8308.42, -8254.69, -8077.89, -8067.8)
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
