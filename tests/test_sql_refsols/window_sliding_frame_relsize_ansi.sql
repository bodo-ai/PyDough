SELECT
  sbtransaction.sbtxid AS transaction_id,
  COUNT(*) OVER (ORDER BY sbtransaction.sbtxdatetime NULLS LAST, sbtransaction.sbtxid NULLS LAST ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS w1,
  COUNT(*) OVER (PARTITION BY sbtransaction.sbtxcustid ORDER BY sbtransaction.sbtxdatetime NULLS LAST, sbtransaction.sbtxid NULLS LAST ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS w2,
  COUNT(*) OVER (ORDER BY sbtransaction.sbtxdatetime NULLS LAST, sbtransaction.sbtxid NULLS LAST ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w3,
  COUNT(*) OVER (PARTITION BY sbtransaction.sbtxcustid ORDER BY sbtransaction.sbtxdatetime NULLS LAST, sbtransaction.sbtxid NULLS LAST ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w4,
  COUNT(*) OVER (ORDER BY sbtransaction.sbtxdatetime NULLS LAST, sbtransaction.sbtxid NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS w5,
  COUNT(*) OVER (PARTITION BY sbtransaction.sbtxcustid ORDER BY sbtransaction.sbtxdatetime NULLS LAST, sbtransaction.sbtxid NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS w6,
  COUNT(*) OVER (ORDER BY sbtransaction.sbtxdatetime NULLS LAST, sbtransaction.sbtxid NULLS LAST ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING) AS w7,
  COUNT(*) OVER (PARTITION BY sbtransaction.sbtxcustid ORDER BY sbtransaction.sbtxdatetime NULLS LAST, sbtransaction.sbtxid NULLS LAST ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING) AS w8
FROM main.sbcustomer AS sbcustomer
JOIN main.sbtransaction AS sbtransaction
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
ORDER BY
  sbtransaction.sbtxdatetime
LIMIT 8
