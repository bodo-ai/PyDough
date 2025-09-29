SELECT
  COUNT(*) AS n
FROM bodo.retail.transactions
WHERE
  payment_method IN ('Cash', 'Gift Card')
