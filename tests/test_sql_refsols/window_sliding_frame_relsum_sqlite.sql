SELECT
  sbtransaction.sbtxid AS transaction_id,
  SUM(sbtransaction.sbtxshares) OVER (ORDER BY sbtransaction.sbtxdatetime, sbtransaction.sbtxid ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS w1,
  SUM(sbtransaction.sbtxshares) OVER (PARTITION BY sbcustomer.sbcustid ORDER BY sbtransaction.sbtxdatetime, sbtransaction.sbtxid ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS w2,
  SUM(sbtransaction.sbtxshares) OVER (ORDER BY sbtransaction.sbtxdatetime, sbtransaction.sbtxid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w3,
  SUM(sbtransaction.sbtxshares) OVER (PARTITION BY sbcustomer.sbcustid ORDER BY sbtransaction.sbtxdatetime, sbtransaction.sbtxid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w4,
  SUM(sbtransaction.sbtxshares) OVER (ORDER BY sbtransaction.sbtxdatetime, sbtransaction.sbtxid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS w5,
  SUM(sbtransaction.sbtxshares) OVER (PARTITION BY sbcustomer.sbcustid ORDER BY sbtransaction.sbtxdatetime, sbtransaction.sbtxid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS w6,
  SUM(sbtransaction.sbtxshares) OVER (ORDER BY sbtransaction.sbtxdatetime, sbtransaction.sbtxid ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS w7,
  SUM(sbtransaction.sbtxshares) OVER (PARTITION BY sbcustomer.sbcustid ORDER BY sbtransaction.sbtxdatetime, sbtransaction.sbtxid ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS w8
FROM main.sbcustomer AS sbcustomer
JOIN main.sbtransaction AS sbtransaction
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
ORDER BY
  sbtransaction.sbtxdatetime
LIMIT 8
