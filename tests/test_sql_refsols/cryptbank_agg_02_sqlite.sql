SELECT
  a_type AS account_type,
  COUNT(*) AS n,
  ROUND(AVG(a_balance), 2) AS avg_bal
FROM crbnk.accounts
GROUP BY
  1
