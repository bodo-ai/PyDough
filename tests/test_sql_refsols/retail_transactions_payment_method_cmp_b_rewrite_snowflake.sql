SELECT
  COUNT(*) AS n
FROM bodo.retail.transactions
WHERE
  payment_method <> PTY_PROTECT_ACCOUNT('Credit Card')
