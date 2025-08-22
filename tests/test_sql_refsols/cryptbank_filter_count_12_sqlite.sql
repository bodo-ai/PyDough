SELECT
  COUNT(*) AS n
FROM crbnk.transactions AS transactions
JOIN crbnk.accounts AS accounts
  ON CAST(STRFTIME('%Y', accounts.a_open_ts) AS INTEGER) = CAST(STRFTIME('%Y', transactions.t_ts) AS INTEGER)
  AND accounts.a_key = transactions.t_sourceaccount
