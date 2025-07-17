SELECT
  sbTransaction.sbtxid AS transaction_id,
  SUM(sbTransaction.sbtxshares) OVER (ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS w1,
  SUM(sbTransaction.sbtxshares) OVER (PARTITION BY sbTransaction.sbtxcustid ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS w2,
  SUM(sbTransaction.sbtxshares) OVER (ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w3,
  SUM(sbTransaction.sbtxshares) OVER (PARTITION BY sbTransaction.sbtxcustid ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w4,
  SUM(sbTransaction.sbtxshares) OVER (ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS w5,
  SUM(sbTransaction.sbtxshares) OVER (PARTITION BY sbTransaction.sbtxcustid ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS w6,
  SUM(sbTransaction.sbtxshares) OVER (ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS w7,
  SUM(sbTransaction.sbtxshares) OVER (PARTITION BY sbTransaction.sbtxcustid ORDER BY sbTransaction.sbtxdatetime, sbTransaction.sbtxid ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS w8
FROM main.sbCustomer AS sbCustomer
JOIN main.sbTransaction AS sbTransaction
  ON sbCustomer.sbcustid = sbTransaction.sbtxcustid
ORDER BY
  sbTransaction.sbtxdatetime
LIMIT 8
