SELECT
  COUNT(*) AS n
FROM bodo.retail.transactions
WHERE
  payment_method IN (PTY_PROTECT_ACCOUNT('Cash'), PTY_PROTECT_ACCOUNT('Gift Card'))
