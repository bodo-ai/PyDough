SELECT
  sbTransaction.sbtxid AS transaction_id,
  COUNT(*) OVER (ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS w1,
  COUNT(*) OVER (PARTITION BY sbTransaction.sbtxcustid ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS w2,
  COUNT(*) OVER (ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w3,
  COUNT(*) OVER (PARTITION BY sbTransaction.sbtxcustid ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w4,
  COUNT(*) OVER (ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS w5,
  COUNT(*) OVER (PARTITION BY sbTransaction.sbtxcustid ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS w6,
  COUNT(*) OVER (ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING) AS w7,
  COUNT(*) OVER (PARTITION BY sbTransaction.sbtxcustid ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING) AS w8
FROM main.sbCustomer AS sbCustomer
JOIN main.sbTransaction AS sbTransaction
  ON sbCustomer.sbcustid = sbTransaction.sbtxcustid
ORDER BY
  sbTransaction.sbtxdatetime
LIMIT 8
