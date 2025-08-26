SELECT
  COUNT(*) AS n
FROM crbnk.transactions AS transactions
JOIN crbnk.accounts AS accounts
  ON CAST(STRFTIME('%Y', DATETIME(accounts.a_open_ts, '+123456789 seconds')) AS INTEGER) = CAST(STRFTIME('%Y', DATETIME(transactions.t_ts, '+54321 seconds')) AS INTEGER)
  AND transactions.t_sourceaccount = CASE
    WHEN accounts.a_key = 0
    THEN 0
    ELSE CASE WHEN accounts.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(
      accounts.a_key,
      1 + INSTR(accounts.a_key, '-'),
      CAST(LENGTH(accounts.a_key) AS REAL) / 2
    ) AS INTEGER)
  END
