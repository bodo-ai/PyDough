SELECT
  accounttype AS account_type,
  COUNT(*) AS n,
  ROUND(AVG(balance), 2) AS avg_bal
FROM bodo.fsi.accounts
GROUP BY
  1
