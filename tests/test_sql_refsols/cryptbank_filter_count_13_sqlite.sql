SELECT
  COUNT(*) AS n
FROM crbnk.transactions AS transactions
JOIN crbnk.accounts AS accounts
  ON accounts.a_key = transactions.t_destaccount
  AND transactions.t_ts < DATETIME(accounts.a_open_ts, '2 year')
