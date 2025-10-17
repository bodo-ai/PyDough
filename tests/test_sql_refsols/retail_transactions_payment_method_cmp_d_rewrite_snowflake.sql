SELECT
  COUNT(*) AS n
FROM bodo.retail.transactions
WHERE
  NOT payment_method IN (PTY_PROTECT_ACCOUNT('Mobile Payment'), PTY_PROTECT_ACCOUNT('Gift Card'))
